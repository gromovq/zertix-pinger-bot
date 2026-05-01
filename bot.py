import asyncio
import io
import ipaddress
import logging
import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Iterable

import aiosqlite
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from mcstatus import JavaServer
from telegram.error import NetworkError, TimedOut
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()


@dataclass(frozen=True)
class Target:
    host: str
    port: int = 25565


@dataclass(frozen=True)
class Config:
    token: str
    allowed_user_ids: set[int]
    allowed_chat_ids: set[int]
    targets_file: str
    db_path: str
    poll_interval: int
    tz: timezone
    max_cidr_hosts: int
    ping_timeout: float
    max_concurrency: int

    @classmethod
    def from_env(cls) -> "Config":
        token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            raise ValueError(
                "Environment variable TELEGRAM_BOT_TOKEN is required. "
                "Set it in .env or export it in the environment."
            )
        return cls(
            token=token,
            allowed_user_ids={int(x) for x in os.getenv("ALLOWED_USER_IDS", "").split(",") if x.strip()},
            allowed_chat_ids={int(x) for x in os.getenv("ALLOWED_CHAT_IDS", "").split(",") if x.strip()},
            targets_file=os.getenv("TARGETS_FILE", "targets.txt"),
            db_path=os.getenv("DB_PATH", "stats.sqlite3"),
            poll_interval=int(os.getenv("POLL_INTERVAL_SECONDS", "5")),
            tz=timezone.utc if os.getenv("TIMEZONE", "UTC").upper() == "UTC" else timezone.utc,
            max_cidr_hosts=int(os.getenv("MAX_CIDR_HOSTS", "1024")),
            ping_timeout=float(os.getenv("PING_TIMEOUT_SECONDS", "1.8")),
            max_concurrency=int(os.getenv("MAX_CONCURRENCY", "50")),
        )


class PingCollector:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.targets: list[Target] = []
        self.running = True
        self._rate_limit: dict[int, deque[datetime]] = {}
        self._collector_task: asyncio.Task | None = None
        self._last_online: dict[tuple[str, int], int] = {}

    def load_targets(self) -> list[Target]:
        parsed: set[Target] = set()
        with open(self.cfg.targets_file, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw or raw.startswith("#"):
                    continue
                if "/" in raw:
                    parsed.update(self._expand_cidr_targets(raw))
                    continue
                parsed.update(self._expand_host_targets(raw))
        return sorted(parsed, key=lambda t: (t.host, t.port))

    @staticmethod
    def _split_host_port(raw: str) -> tuple[str, str]:
        if raw.count(":") == 1:
            host, port_str = raw.split(":", 1)
            return host.strip(), port_str.strip()
        return raw, "25565"

    @staticmethod
    def _expand_ports(port_spec: str) -> list[int]:
        if "-" not in port_spec:
            return [int(port_spec)]
        start_s, end_s = port_spec.split("-", 1)
        start = int(start_s.strip())
        end = int(end_s.strip())
        if start > end:
            raise ValueError(f"Invalid port range: {port_spec}")
        if start < 1 or end > 65535:
            raise ValueError(f"Port range out of bounds: {port_spec}")
        return list(range(start, end + 1))

    def _expand_host_targets(self, raw: str) -> set[Target]:
        host, port_spec = self._split_host_port(raw)
        ports = self._expand_ports(port_spec)
        return {Target(host, p) for p in ports}

    def _expand_cidr_targets(self, raw: str) -> set[Target]:
        network_part = raw
        port_spec = "25565"
        if raw.count(":") == 1:
            network_part, port_spec = raw.split(":", 1)
            network_part = network_part.strip()
            port_spec = port_spec.strip()
        network = ipaddress.ip_network(network_part, strict=False)
        if network.num_addresses > self.cfg.max_cidr_hosts:
            raise ValueError(
                f"CIDR {network_part} expands to {network.num_addresses} hosts (max {self.cfg.max_cidr_hosts})."
            )
        ports = self._expand_ports(port_spec)
        targets: set[Target] = set()
        for ip in network.hosts():
            for port in ports:
                targets.add(Target(str(ip), port))
        return targets

    async def init_db(self):
        async with aiosqlite.connect(self.cfg.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS samples (
                    ts INTEGER NOT NULL,
                    total_online INTEGER NOT NULL,
                    success_count INTEGER NOT NULL,
                    failure_count INTEGER NOT NULL
                )
                """
            )
            await db.execute("CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples(ts)")
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS server_state (
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    last_online INTEGER NOT NULL,
                    updated_ts INTEGER NOT NULL,
                    PRIMARY KEY(host, port)
                )
                """
            )
            await db.commit()
            cur = await db.execute("SELECT host, port, last_online FROM server_state")
            rows = await cur.fetchall()
            self._last_online = {(r[0], int(r[1])): int(r[2]) for r in rows}

    async def ping_target(self, target: Target, sem: asyncio.Semaphore) -> tuple[bool, int]:
        async with sem:
            try:
                server = JavaServer.lookup(f"{target.host}:{target.port}", timeout=self.cfg.ping_timeout)
                status = await server.async_status()
                return True, int(status.players.online)
            except Exception:
                return False, 0

    async def collect_once(self):
        sem = asyncio.Semaphore(self.cfg.max_concurrency)
        results = await asyncio.gather(*(self.ping_target(t, sem) for t in self.targets), return_exceptions=False)
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        total = 0
        success = sum(1 for ok, _ in results if ok)
        failed = len(results) - success
        upserts: list[tuple[str, int, int, int]] = []
        for target, (ok, online) in zip(self.targets, results, strict=False):
            key = (target.host, target.port)
            if ok:
                self._last_online[key] = online
                upserts.append((target.host, target.port, online, now_ts))
            total += self._last_online.get(key, 0)
        async with aiosqlite.connect(self.cfg.db_path) as db:
            await db.execute(
                "INSERT INTO samples(ts, total_online, success_count, failure_count) VALUES(?,?,?,?)",
                (now_ts, total, success, failed),
            )
            if upserts:
                await db.executemany(
                    """
                    INSERT INTO server_state(host, port, last_online, updated_ts)
                    VALUES(?,?,?,?)
                    ON CONFLICT(host, port) DO UPDATE SET
                        last_online=excluded.last_online,
                        updated_ts=excluded.updated_ts
                    """,
                    upserts,
                )
            await db.commit()
        logging.info("Saved sample: online=%s success=%s failed=%s", total, success, failed)

    async def collector_loop(self):
        while self.running:
            started = datetime.now(tz=timezone.utc)
            try:
                self.targets = self.load_targets()
                if self.targets:
                    await self.collect_once()
            except Exception:
                logging.exception("Collect cycle failed")
            elapsed = (datetime.now(tz=timezone.utc) - started).total_seconds()
            await asyncio.sleep(max(0.1, self.cfg.poll_interval - elapsed))

    async def start(self):
        self.running = True
        if self._collector_task is None or self._collector_task.done():
            self._collector_task = asyncio.create_task(self.collector_loop(), name="collector_loop")

    async def stop(self):
        self.running = False
        if self._collector_task and not self._collector_task.done():
            self._collector_task.cancel()
            try:
                await self._collector_task
            except asyncio.CancelledError:
                pass

    def _auth_ok(self, update: Update) -> bool:
        user_id = update.effective_user.id if update.effective_user else None
        chat_id = update.effective_chat.id if update.effective_chat else None
        if self.cfg.allowed_user_ids and user_id not in self.cfg.allowed_user_ids:
            return False
        if self.cfg.allowed_chat_ids and chat_id not in self.cfg.allowed_chat_ids:
            return False
        return True

    def _rate_limit_ok(self, update: Update) -> bool:
        user_id = update.effective_user.id if update.effective_user else 0
        now = datetime.now(tz=timezone.utc)
        bucket = self._rate_limit.setdefault(user_id, deque())
        while bucket and (now - bucket[0]).total_seconds() > 20:
            bucket.popleft()
        if len(bucket) >= 5:
            return False
        bucket.append(now)
        return True

    async def _fetch_samples(self, since_ts: int) -> list[tuple[int, int]]:
        async with aiosqlite.connect(self.cfg.db_path) as db:
            cur = await db.execute(
                "SELECT ts, total_online FROM samples WHERE ts >= ? ORDER BY ts ASC", (since_ts,)
            )
            return await cur.fetchall()

    async def _all_time_record(self) -> int:
        async with aiosqlite.connect(self.cfg.db_path) as db:
            cur = await db.execute("SELECT COALESCE(MAX(total_online), 0) FROM samples")
            row = await cur.fetchone()
            return int(row[0] if row else 0)

    async def cmd_now(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth_ok(update):
            return
        if not self._rate_limit_ok(update):
            await update.message.reply_text("Слишком часто. Подождите немного.")
            return
        msg = await update.message.reply_text("⚙️ Обработка запроса")
        now = int(datetime.now(tz=timezone.utc).timestamp())
        rows = await self._fetch_samples(now - 86400)
        self.all_time_record = await self._all_time_record()
        text = self._build_stats_text(rows, now)
        await msg.edit_text(text)

    def _build_stats_text(self, rows: Iterable[tuple[int, int]], now_ts: int) -> str:
        values = [v for _, v in rows]
        current = values[-1] if values else 0
        target_ts = now_ts - 86400
        day_ago = 0
        if rows:
            day_ago = min(rows, key=lambda r: abs(r[0] - target_ts))[1]
        min_v = min(values) if values else 0
        avg_v = int(mean(values)) if values else 0
        max_day = max(values) if values else 0
        return (
            f"– Текущий онлайн: {current}\n"
            f"– Онлайн сутки назад в это же время: {day_ago}\n"
            f"– Минимальный онлайн за сутки: {min_v}\n"
            f"– Средний онлайн за сутки: {avg_v}\n"
            f"– Рекорд онлайна за сутки: {max_day}\n"
            f"– Рекорд онлайна за всё время: {self.all_time_record if hasattr(self, 'all_time_record') else max_day}"
        )

    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._auth_ok(update):
            return
        if not self._rate_limit_ok(update):
            await update.message.reply_text("Слишком часто. Подождите немного.")
            return
        msg = await update.message.reply_text("⚙️ Обработка запроса")
        now = datetime.now(tz=timezone.utc)
        rows = await self._fetch_samples(int((now - timedelta(hours=24)).timestamp()))
        self.all_time_record = await self._all_time_record()
        caption = self._build_stats_text(rows, int(now.timestamp()))
        img = self._build_chart(rows)
        await update.message.reply_photo(photo=img, caption=caption)
        await msg.delete()

    def _build_chart(self, rows: list[tuple[int, int]]) -> io.BytesIO:
        if not rows:
            rows = [(int(datetime.now(tz=timezone.utc).timestamp()), 0)]
        ts = [datetime.fromtimestamp(t, tz=timezone.utc) for t, _ in rows]
        vals = [v for _, v in rows]
        fig, ax = plt.subplots(figsize=(10, 4.8), dpi=120)
        fig.patch.set_facecolor("#0a0f16")
        ax.set_facecolor("#0a0f16")
        ax.plot(ts, vals, color="#0ea5ff", linewidth=2.0)
        ax.fill_between(ts, vals, 0, color="#0b4f77", alpha=0.55)
        ax.grid(color="#1f2937", linewidth=0.8)
        ax.tick_params(colors="#9ca3af")
        for spine in ax.spines.values():
            spine.set_color("#111827")
        ax.yaxis.set_major_formatter(lambda x, pos: f"{int(x):d}")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=timezone.utc))
        ax.set_xlabel("Время (UTC)", color="#9ca3af")
        ax.set_ylabel("Онлайн", color="#9ca3af")
        fig.tight_layout()
        buf = io.BytesIO()
        buf.name = "stats.png"
        fig.savefig(buf, format="png")
        plt.close(fig)
        buf.seek(0)
        return buf


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = Config.from_env()
    collector = PingCollector(cfg)
    await collector.init_db()
    while True:
        app = Application.builder().token(cfg.token).build()
        app.add_handler(CommandHandler("now", collector.cmd_now))
        app.add_handler(CommandHandler("stats", collector.cmd_stats))
        try:
            await app.initialize()
            await app.start()
            await collector.start()
            await app.updater.start_polling(allowed_updates=Update.ALL_TYPES, poll_interval=1.0, timeout=30)
            logging.info("Bot started")
            await asyncio.Event().wait()
        except (TimedOut, NetworkError) as err:
            logging.warning("Telegram API unavailable (%s). Retry in 10 seconds...", err.__class__.__name__)
            await asyncio.sleep(10)
        finally:
            await collector.stop()
            if app.updater and app.updater.running:
                await app.updater.stop()
            if app.running:
                await app.stop()
            await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
