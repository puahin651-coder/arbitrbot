# -*- coding: utf-8 -*-
"""
ARBITR v3 — Telegram-терминал межбиржевого арбитража (спот, USDT).

Что нового в v3:
  • Защита от «чужого» бота на вашем токене: при старте сносится webhook, проверяется
    конфликт getUpdates и пишется понятная ошибка в консоль
  • Шлюз подписки на канал на ВСЕ действия (а не только /start), с кэшем
  • Чёрный список монет: /block, /unblock, /blocklist
  • /top — быстрый топ связок, /help — справка, /ping — диагностика для админа
  • Глобальный обработчик ошибок, фолбэк на неизвестные сообщения, только личные чаты
  • Платёжный цикл не крутится без CRYPTO_PAY_TOKEN

Что было в v2 (по сравнению с v1):
  • 13 бирж: Binance, Bybit, OKX, Gate, KuCoin, MEXC, HTX, Crypto.com Exchange,
    Bitfinex, BingX, Coinbase Exchange, Upbit, Bitget
  • У каждой биржи свой поллер: медленная/упавшая биржа не тормозит остальные,
    есть backoff при ошибках и панель статуса бирж
  • Реальные комиссии по каждой бирже вместо плоских 0.2%
  • Фильтры фантомов: выбросы цены, ширина стакана, объём, ликвидность,
    плавающие/левередж-токены, подтверждение сигнала несколькими сканами подряд
  • ФУНКЦИЯ 1: проверка по стакану — реальный профит с учётом проскальзывания
  • ФУНКЦИЯ 2: персональные push-уведомления в личку по вашим фильтрам
  • ФУНКЦИЯ 3: реферальная программа (бонус-дни PRO)
  • Админка: статистика, предпросмотр рассылки (всем/только PRO), поиск юзера,
    промокоды в БД, статус бирж
  • Безопасность: токены только из переменных окружения / .env

Запуск:
  pip install -U aiogram aiosqlite aiohttp ccxt
  создайте файл .env рядом со скриптом (см. README внизу файла) и запустите
  python ARBITR.py
"""
import asyncio
import csv
import html
import io
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, NamedTuple, Optional, Tuple
from urllib.parse import quote

import aiohttp
import aiosqlite
import ccxt.async_support as ccxt

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
try:
    from aiogram.client.default import DefaultBotProperties
except ImportError:  # aiogram < 3.7
    DefaultBotProperties = None
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.exceptions import TelegramUnauthorizedError
try:
    from aiogram.exceptions import TelegramConflictError
except ImportError:  # очень старые версии
    class TelegramConflictError(Exception):
        pass
from aiogram.filters import BaseFilter, Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    BufferedInputFile,
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)


# =============================================================================
#                                КОНФИГУРАЦИЯ
# =============================================================================
def _load_dotenv(path: str = ".env") -> None:
    """Минимальный загрузчик .env (без внешних зависимостей)."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("\"'"))


_load_dotenv()

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║  ▼▼▼  ВСТАВЬТЕ ТОКЕНЫ СЮДА (между кавычек)  ▼▼▼                           ║
# ╠═══════════════════════════════════════════════════════════════════════════╣
# ║  1) Токен БОТА из @BotFather. Выглядит так: 1234567890:AAH...             ║
MANUAL_BOT_TOKEN = "8815901320:AAELeaHezsD8Tw6Gx9qi5oenamBjfi3UGac"
# ║  2) Токен CRYPTO PAY из @CryptoBot → Crypto Pay → Create App → API Token. ║
# ║     Выглядит так: 12345:AAxxxx...  (нужен только для приёма оплаты)      ║
MANUAL_CRYPTO_PAY_TOKEN = "640413:AAozTIOPhVCXP62brvl6Bt8kL0vp9ticohx"
# ╠═══════════════════════════════════════════════════════════════════════════╣
# ║  Альтернатива: положить их в файл .env (BOT_TOKEN=..., CRYPTO_PAY_TOKEN=...)║
# ║  Если задан .env — он приоритетнее. Токены никому не показывайте!         ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

BOT_TOKEN = (os.getenv("BOT_TOKEN") or MANUAL_BOT_TOKEN).strip()
CRYPTO_PAY_TOKEN = (os.getenv("CRYPTO_PAY_TOKEN") or MANUAL_CRYPTO_PAY_TOKEN).strip()
CHANNEL_SIGNALS_ID = int(os.getenv("CHANNEL_SIGNALS_ID", "-1004368321305"))
REQUIRED_CHANNEL_ID = os.getenv("REQUIRED_CHANNEL_ID", "@arbitrnewwws")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "8066395175").replace(" ", "").split(",") if x]
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "piki_wor")
DB_NAME = os.getenv("DB_NAME", "bot_database.db")

if not re.fullmatch(r"\d{6,}:[A-Za-z0-9_-]{30,}", BOT_TOKEN):
    raise SystemExit(
        "\n❌ Не задан или неверен токен бота.\n"
        "Откройте файл и вставьте токен в MANUAL_BOT_TOKEN (в начале файла, блок «ВСТАВЬТЕ ТОКЕНЫ СЮДА»)\n"
        "либо добавьте BOT_TOKEN=... в файл .env. Токен берётся у @BotFather.")

PRICES = {
    "week": {"usd": 7.0, "days": 7, "name": "PRO (7 дней)"},
    "month": {"usd": 30.0, "days": 30, "name": "VIP (30 дней)"},
}

# --- Параметры сканера ---
ANALYZE_INTERVAL = 5.0       # как часто пересчитываем связки (сек)
FETCH_TIMEOUT = 20.0         # таймаут загрузки тикеров одной биржи
STALE_SEC = 45.0             # данные биржи старше — игнорируются
MIN_NET_SPREAD = 0.30        # мин. чистый спред для попадания в список (%)
MAX_NET_SPREAD = 8.0         # выше — почти всегда фантом (закрыт ввод/вывод, разные токены)
MIN_VOLUME_FLOOR = 50_000.0  # мин. 24ч объём на каждой бирже (USDT)
MIN_TOP_USD = 50.0           # мин. объём лучшего ордера в стакане (USDT)
MAX_BOOK_SPREAD = 0.03       # bid/ask на одной бирже шире 3% — монета неликвидна
OUTLIER_PCT = 0.10           # отклонение цены от медианы по биржам > 10% — выброс
MIN_DEAL_USD = 10.0          # минимальная сумма сделки

# --- Параметры уведомлений ---
ALERT_MIN_STREAK = 2         # сигнал должен продержаться N сканов подряд
ALERT_COOLDOWN = 300         # пауза между повторами одного сигнала (сек)
ALERT_REARM_DELTA = 0.5      # повтор раньше паузы, если спред вырос на N п.п.
CHANNEL_MIN_NET = 0.50       # порог для публикации в канал (%)
CHANNEL_MAX_PER_CYCLE = 3
DM_MAX_PER_MIN = 4           # лимит личных уведомлений на пользователя в минуту
DM_MAX_SIGNALS_PER_CYCLE = 6

# --- Рефералка ---
REF_INVITER_DAYS = 2
REF_INVITEE_DAYS = 1
REF_MAX_REWARDED = 50

SPREAD_OPTIONS = [0.3, 0.5, 1.0, 2.0, 3.0]
VOLUME_OPTIONS = [50_000, 100_000, 250_000, 1_000_000]
PAGE_SIZE = 4


@dataclass(frozen=True)
class ExchangeCfg:
    ccxt_ids: Tuple[str, ...]  # варианты id в ccxt (на случай старых версий)
    title: str
    taker_fee: float           # базовый спот-тейкер, % (проверьте свой уровень!)
    poll_sec: float            # период опроса
    url: str                   # шаблон ссылки на пару: {B} — BASE, {b} — base


EXCHANGES: Dict[str, ExchangeCfg] = {
    "binance":   ExchangeCfg(("binance",), "Binance", 0.10, 5, "https://www.binance.com/en/trade/{B}_USDT?type=spot"),
    "bybit":     ExchangeCfg(("bybit",), "Bybit", 0.10, 5, "https://www.bybit.com/en/trade/spot/{B}/USDT"),
    "okx":       ExchangeCfg(("okx",), "OKX", 0.10, 5, "https://www.okx.com/trade-spot/{b}-usdt"),
    "gate":      ExchangeCfg(("gate", "gateio"), "Gate", 0.20, 6, "https://www.gate.io/trade/{B}_USDT"),
    "kucoin":    ExchangeCfg(("kucoin",), "KuCoin", 0.10, 6, "https://www.kucoin.com/trade/{B}-USDT"),
    "mexc":      ExchangeCfg(("mexc",), "MEXC", 0.10, 6, "https://www.mexc.com/exchange/{B}_USDT"),
    "htx":       ExchangeCfg(("htx", "huobi"), "HTX", 0.20, 6, "https://www.htx.com/trade/{b}_usdt"),
    "cryptocom": ExchangeCfg(("cryptocom",), "Crypto.com", 0.25, 8, "https://crypto.com/exchange/trade/{B}_USDT"),
    "bitfinex":  ExchangeCfg(("bitfinex", "bitfinex2"), "Bitfinex", 0.20, 10, "https://trading.bitfinex.com/t/{B}:UST"),
    "bingx":     ExchangeCfg(("bingx",), "BingX", 0.10, 8, "https://bingx.com/en/spot/{B}USDT"),
    "coinbase":  ExchangeCfg(("coinbaseexchange", "coinbasepro"), "Coinbase", 0.60, 15, "https://exchange.coinbase.com/trade/{B}-USDT"),
    "upbit":     ExchangeCfg(("upbit",), "Upbit", 0.25, 10, "https://upbit.com/exchange?code=CRIX.UPBIT.USDT-{B}"),
    "bitget":    ExchangeCfg(("bitget",), "Bitget", 0.10, 6, "https://www.bitget.com/spot/{B}USDT"),
}
EXCHANGE_NAMES = list(EXCHANGES.keys())
FEES = {k: v.taker_fee / 100.0 for k, v in EXCHANGES.items()}
OLD_DEFAULT_EXCHANGES = '["binance","bybit","okx","gate","kucoin"]'

# Мем-коины и низколиквидные «щиткоины»: короткие ключи — точное совпадение, длинные — вхождение
MEME_KEYWORDS = {
    "doge", "shib", "pepe", "wif", "bonk", "floki", "bome", "mew",
    "popcat", "turbo", "neiro", "brett", "mog", "myro", "meme", "lunc",
}
STABLE_BASES = {
    "USDC", "USDT", "DAI", "TUSD", "FDUSD", "BUSD", "USDP", "PYUSD", "USDD",
    "EUR", "EURT", "EURC", "GUSD", "USDE", "USD1", "UST", "USTC",
}
LEVERAGED_RE = re.compile(r".*\d+[LS]$|.*(BULL|BEAR)$")

from logging.handlers import RotatingFileHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler("arbitr.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8"),
    ],
)
logging.getLogger("aiogram").setLevel(logging.WARNING)


# =============================================================================
#                           ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================
def now_ts() -> int:
    return int(time.time())


def fmt_dt(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m.%Y %H:%M UTC")


def parse_expiry(val) -> int:
    if not val:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        v = val.strip()
        if not v:
            return 0
        try:
            return int(float(v))
        except ValueError:
            pass
        try:
            return int(datetime.fromisoformat(v).timestamp())
        except ValueError:
            pass
    return 0


def money(v: float) -> str:
    return f"{'+' if v >= 0 else '-'}${abs(v):,.2f}"


def fmt_price(p: float) -> str:
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.6f}"
    return f"{p:.8f}"


def fmt_vol(v: Optional[float]) -> str:
    if v is None:
        return "н/д"
    if v >= 1_000_000_000:
        return f"${v / 1_000_000_000:.1f}B"
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v / 1_000:.0f}K"
    return f"${v:.0f}"


def msg_text(message: Message) -> str:
    return (message.text or "").strip()


def parse_floats(text: str, min_n: int, max_n: int) -> Optional[List[float]]:
    try:
        parts = text.replace(",", ".").split()
        if not (min_n <= len(parts) <= max_n):
            return None
        return [float(p) for p in parts]
    except ValueError:
        return None


def is_meme(base: str) -> bool:
    b = base.lower()
    for kw in MEME_KEYWORDS:
        if len(kw) <= 3:
            if b == kw:
                return True
        elif kw in b:
            return True
    return False


def trade_url(exchange: str, symbol: str) -> str:
    cfg = EXCHANGES.get(exchange.lower())
    if not cfg:
        return "https://t.me"
    base = symbol.split("/")[0]
    return cfg.url.format(B=base.upper(), b=base.lower())


def ex_title(ex: str) -> str:
    cfg = EXCHANGES.get(ex)
    return cfg.title if cfg else ex.upper()


# =============================================================================
#                              БАЗА ДАННЫХ
# =============================================================================
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self):
        if not self._db:
            self._db = await aiosqlite.connect(self.db_path)
            await self._db.execute("PRAGMA journal_mode=WAL;")
            await self._db.execute("PRAGMA synchronous=NORMAL;")
            await self._db.execute("PRAGMA busy_timeout=5000;")
            await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if not self._db:
            raise RuntimeError("БД не инициализирована.")
        return self._db


db_mgr = DatabaseManager(DB_NAME)

ALL_EXCHANGES_JSON = json.dumps(EXCHANGE_NAMES)


async def init_db():
    db = db_mgr.conn
    await db.execute(f"""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            sub_expiry INTEGER DEFAULT 0,
            notified_24h INTEGER DEFAULT 0,
            min_spread REAL DEFAULT 0.3,
            balance REAL DEFAULT 50.0,
            max_deal_amount REAL DEFAULT 0.0,
            enabled_exchanges TEXT DEFAULT '{ALL_EXCHANGES_JSON}'
        )
    """)
    # Миграции для старых баз
    migrations = [
        ("users", "balance", "REAL DEFAULT 50.0"),
        ("users", "max_deal_amount", "REAL DEFAULT 0.0"),
        ("users", "min_volume", "REAL DEFAULT 100000"),
        ("users", "dm_alerts", "INTEGER DEFAULT 1"),
        ("users", "allow_memes", "INTEGER DEFAULT 0"),
        ("users", "referred_by", "INTEGER"),
        ("users", "ref_rewarded", "INTEGER DEFAULT 0"),
        ("users", "created_at", "INTEGER DEFAULT 0"),
        ("users", "blocked_coins", "TEXT DEFAULT '[]'"),
        ("users", "pause_until", "INTEGER DEFAULT 0"),
        ("invoices", "created_at", "INTEGER DEFAULT 0"),
    ]
    await db.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id TEXT UNIQUE,
            provider TEXT,
            user_id INTEGER,
            amount REAL,
            plan TEXT,
            status TEXT DEFAULT 'active'
        )
    """)
    for table, col, col_type in migrations:
        try:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
        except Exception:
            pass

    await db.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            pair_info TEXT,
            amount_usd REAL,
            profit_usd REAL,
            roi_percent REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS promo_activations (
            user_id INTEGER,
            code TEXT,
            activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, code)
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS promocodes (
            code TEXT PRIMARY KEY,
            days INTEGER NOT NULL,
            max_uses INTEGER DEFAULT 0,
            used INTEGER DEFAULT 0
        )
    """)
    await db.execute("INSERT OR IGNORE INTO promocodes (code, days, max_uses, used) VALUES ('free1', 3, 0, 0)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_trades_user ON trades(user_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_users_ref ON users(referred_by)")
    # Пользователям со старым набором из 5 бирж — включаем все новые
    await db.execute(
        "UPDATE users SET enabled_exchanges = ? WHERE enabled_exchanges = ?",
        (ALL_EXCHANGES_JSON, OLD_DEFAULT_EXCHANGES),
    )
    await db.commit()


async def user_exists(user_id: int) -> bool:
    async with db_mgr.conn.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)) as cur:
        return await cur.fetchone() is not None


async def get_sub_expiry(user_id: int) -> int:
    async with db_mgr.conn.execute("SELECT sub_expiry FROM users WHERE user_id = ?", (user_id,)) as cur:
        row = await cur.fetchone()
        return parse_expiry(row[0]) if row else 0


def _parse_exchanges(raw) -> List[str]:
    try:
        lst = json.loads(raw) if raw else EXCHANGE_NAMES
    except Exception:
        lst = EXCHANGE_NAMES
    lst = [e for e in lst if e in EXCHANGES]
    return lst or list(EXCHANGE_NAMES)


USER_COLUMNS = ("user_id, username, sub_expiry, min_spread, enabled_exchanges, balance, "
                "max_deal_amount, min_volume, dm_alerts, allow_memes, blocked_coins, pause_until")


def _parse_blocked(raw) -> List[str]:
    try:
        lst = json.loads(raw) if raw else []
        return [str(x).upper() for x in lst if str(x).isalnum()][:100]
    except Exception:
        return []


def _row_to_user(row) -> dict:
    return {
        "user_id": row[0],
        "username": row[1] or "User",
        "sub_expiry": parse_expiry(row[2]),
        "min_spread": row[3] if row[3] is not None else 0.3,
        "exchanges": _parse_exchanges(row[4]),
        "balance": row[5] if row[5] is not None else 50.0,
        "max_deal_amount": row[6] if row[6] is not None else 0.0,
        "min_volume": row[7] if row[7] is not None else 100_000.0,
        "dm_alerts": bool(row[8]) if row[8] is not None else True,
        "allow_memes": bool(row[9]) if row[9] is not None else False,
        "blocked": _parse_blocked(row[10]) if len(row) > 10 else [],
        "pause_until": int(row[11] or 0) if len(row) > 11 else 0,
    }


async def get_user_data(user_id: int) -> dict:
    async with db_mgr.conn.execute(f"SELECT {USER_COLUMNS} FROM users WHERE user_id = ?", (user_id,)) as cur:
        row = await cur.fetchone()
    if row:
        return _row_to_user(row)
    return {
        "user_id": user_id, "username": "User", "sub_expiry": 0, "min_spread": 0.3,
        "exchanges": list(EXCHANGE_NAMES), "balance": 50.0, "max_deal_amount": 0.0,
        "min_volume": 100_000.0, "dm_alerts": True, "allow_memes": False, "blocked": [],
        "pause_until": 0,
    }


async def update_user(user_id: int, username: str, referred_by: Optional[int] = None) -> bool:
    """Создаёт/обновляет пользователя. Возвращает True, если пользователь новый."""
    is_new = not await user_exists(user_id)
    if is_new:
        await db_mgr.conn.execute(
            "INSERT INTO users (user_id, username, enabled_exchanges, created_at, referred_by) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, ALL_EXCHANGES_JSON, now_ts(), referred_by),
        )
    else:
        await db_mgr.conn.execute("UPDATE users SET username = ? WHERE user_id = ?", (username, user_id))
    await db_mgr.conn.commit()
    return is_new


ALLOWED_USER_FIELDS = {"balance", "max_deal_amount", "min_spread", "min_volume", "dm_alerts",
                       "allow_memes", "blocked_coins", "pause_until"}


async def update_user_field(user_id: int, field_name: str, value):
    if field_name not in ALLOWED_USER_FIELDS:
        raise ValueError(f"Недопустимое поле: {field_name}")
    await db_mgr.conn.execute(f"UPDATE users SET {field_name} = ? WHERE user_id = ?", (value, user_id))
    await db_mgr.conn.commit()


async def set_user_exchanges(user_id: int, exchanges: List[str]):
    await db_mgr.conn.execute(
        "UPDATE users SET enabled_exchanges = ? WHERE user_id = ?", (json.dumps(exchanges), user_id)
    )
    await db_mgr.conn.commit()


async def add_subscription(user_id: int, days: int) -> int:
    current = await get_sub_expiry(user_id)
    new_expiry = max(now_ts(), current) + days * 86400
    await db_mgr.conn.execute("""
        INSERT INTO users (user_id, username, sub_expiry, notified_24h, enabled_exchanges, created_at)
        VALUES (?, 'User', ?, 0, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET sub_expiry = excluded.sub_expiry, notified_24h = 0
    """, (user_id, new_expiry, ALL_EXCHANGES_JSON, now_ts()))
    await db_mgr.conn.commit()
    return new_expiry


async def revoke_subscription(user_id: int):
    await db_mgr.conn.execute("UPDATE users SET sub_expiry = 0 WHERE user_id = ?", (user_id,))
    await db_mgr.conn.commit()


async def is_user_subscribed(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    return (await get_sub_expiry(user_id)) > now_ts()


# --- Дневник сделок ---
async def save_trade(user_id: int, pair_info: str, amount_usd: float, profit_usd: float):
    roi = (profit_usd / amount_usd * 100) if amount_usd > 0 else 0.0
    await db_mgr.conn.execute(
        "INSERT INTO trades (user_id, pair_info, amount_usd, profit_usd, roi_percent) VALUES (?, ?, ?, ?, ?)",
        (user_id, pair_info, amount_usd, profit_usd, roi),
    )
    await db_mgr.conn.commit()


async def delete_last_trade(user_id: int) -> bool:
    cur = await db_mgr.conn.execute(
        "DELETE FROM trades WHERE id = (SELECT MAX(id) FROM trades WHERE user_id = ?)", (user_id,)
    )
    await db_mgr.conn.commit()
    return cur.rowcount > 0


async def get_user_trade_stats(user_id: int) -> dict:
    async with db_mgr.conn.execute("""
        SELECT COUNT(*), COALESCE(SUM(amount_usd), 0), COALESCE(SUM(profit_usd), 0), COALESCE(AVG(roi_percent), 0),
               COALESCE(SUM(CASE WHEN created_at >= datetime('now', '-7 day') THEN profit_usd ELSE 0 END), 0),
               COALESCE(SUM(CASE WHEN profit_usd > 0 THEN 1 ELSE 0 END), 0)
        FROM trades WHERE user_id = ?
    """, (user_id,)) as cur:
        row = await cur.fetchone()
    return {
        "count": row[0], "total_volume": round(row[1], 2), "total_profit": round(row[2], 2),
        "avg_roi": round(row[3], 2), "profit_7d": round(row[4], 2), "wins": row[5],
    }


async def get_recent_trades(user_id: int, limit: int = 5) -> list:
    async with db_mgr.conn.execute(
        "SELECT pair_info, amount_usd, profit_usd, roi_percent, created_at FROM trades "
        "WHERE user_id = ? ORDER BY id DESC LIMIT ?", (user_id, limit),
    ) as cur:
        return await cur.fetchall()


# --- Промокоды ---
async def activate_promo(user_id: int, code: str) -> Tuple[bool, str]:
    code = code.strip().lower()
    db = db_mgr.conn
    async with db.execute("SELECT days, max_uses, used FROM promocodes WHERE code = ?", (code,)) as cur:
        row = await cur.fetchone()
    if not row:
        return False, "❌ Промокод не найден."
    days, max_uses, used = row
    if max_uses > 0 and used >= max_uses:
        return False, "❌ Лимит активаций этого промокода исчерпан."
    cur = await db.execute("INSERT OR IGNORE INTO promo_activations (user_id, code) VALUES (?, ?)", (user_id, code))
    if cur.rowcount == 0:
        await db.commit()
        return False, "❌ Вы уже активировали этот промокод."
    await db.execute("UPDATE promocodes SET used = used + 1 WHERE code = ?", (code,))
    await db.commit()
    new_exp = await add_subscription(user_id, days)
    return True, (f"🎉 <b>Промокод активирован!</b>\n\nВам выдана PRO-подписка на {days} дн.\n"
                  f"Действует до: <code>{fmt_dt(new_exp)}</code>")


# =============================================================================
#                              CRYPTOPAY
# =============================================================================
class CryptoPayAPI:
    def __init__(self, token: str):
        self.headers = {"Crypto-Pay-API-Token": token}
        self.base_url = "https://pay.crypt.bot/api/"
        self._session: Optional[aiohttp.ClientSession] = None

    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self.headers, timeout=aiohttp.ClientTimeout(total=15))
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def create_invoice(self, amount: float, payload: str, description: str) -> Optional[Dict]:
        data = {
            "asset": "USDT", "amount": str(amount), "description": description,
            "payload": payload, "expires_in": 3600,
        }
        try:
            session = await self.get_session()
            async with session.post(f"{self.base_url}createInvoice", json=data) as resp:
                res = await resp.json()
                if res.get("ok"):
                    r = res["result"]
                    return {"invoice_id": str(r["invoice_id"]), "pay_url": r.get("bot_invoice_url") or r["pay_url"]}
                logging.error(f"CryptoPay createInvoice: {res}")
        except Exception as e:
            logging.error(f"CryptoPay error: {e}")
        return None

    async def get_invoices(self, invoice_ids: List[str]) -> Dict[str, Dict]:
        """Возвращает {invoice_id: invoice} для списка счетов (одним запросом)."""
        if not invoice_ids:
            return {}
        try:
            session = await self.get_session()
            async with session.get(f"{self.base_url}getInvoices",
                                   params={"invoice_ids": ",".join(invoice_ids)}) as resp:
                res = await resp.json()
            if res.get("ok"):
                result = res["result"]
                items = result.get("items", []) if isinstance(result, dict) else result
                return {str(i["invoice_id"]): i for i in items}
        except Exception as e:
            logging.error(f"CryptoPay get error: {e}")
        return {}


crypto_pay = CryptoPayAPI(CRYPTO_PAY_TOKEN)


# =============================================================================
#                      РЫНОЧНЫЕ ДАННЫЕ И СКАНЕР
# =============================================================================
class Quote(NamedTuple):
    bid: float
    ask: float
    qvol: float                    # 24ч объём в USDT
    bid_usd: Optional[float]       # объём лучшего бида (USDT), если биржа отдаёт
    ask_usd: Optional[float]


@dataclass
class ExStatus:
    last_ok: float = 0.0
    latency: float = 0.0
    pairs: int = 0
    fails: int = 0
    last_error: str = ""


class MarketData:
    def __init__(self):
        self.books: Dict[str, Dict[str, Quote]] = {}
        self.updated: Dict[str, float] = {}
        self.status: Dict[str, ExStatus] = {n: ExStatus() for n in EXCHANGE_NAMES}

    def online(self) -> List[str]:
        t = time.time()
        return [n for n in EXCHANGE_NAMES if t - self.updated.get(n, 0) <= STALE_SEC]


MARKET = MarketData()
EXCH_OBJ: Dict[str, "ccxt.Exchange"] = {}


@dataclass
class Signal:
    symbol: str
    base: str
    buy_ex: str
    sell_ex: str
    buy_price: float
    sell_price: float
    gross: float
    net: float
    volume: float
    top_liq: Optional[float]
    is_meme: bool
    ts: str
    key: str
    streak: int = 1


LATEST_SIGNALS: List[Signal] = []
STREAK: Dict[str, int] = {}


def normalize_tickers(tickers: dict) -> Dict[str, Quote]:
    """Оставляет только спот /USDT, отсекает мусор и неликвид."""
    out: Dict[str, Quote] = {}
    if not isinstance(tickers, dict):
        return out
    for symbol, t in tickers.items():
        if not isinstance(symbol, str) or not symbol.endswith("/USDT") or ":" in symbol:
            continue
        base = symbol[:-5].upper()
        if not base or not base.isalnum() or base in STABLE_BASES or LEVERAGED_RE.match(base):
            continue
        if not isinstance(t, dict):
            continue
        try:
            bid = float(t.get("bid"))
            ask = float(t.get("ask"))
        except (TypeError, ValueError):
            continue
        if bid <= 0 or ask <= 0 or ask < bid or (ask - bid) / bid > MAX_BOOK_SPREAD:
            continue

        qvol = t.get("quoteVolume")
        if not qvol:
            bv = t.get("baseVolume")
            qvol = float(bv) * float(t.get("last") or ask) if bv else 0.0
        qvol = float(qvol or 0.0)
        if qvol < MIN_VOLUME_FLOOR:
            continue

        bid_usd = ask_usd = None
        try:
            if t.get("bidVolume") is not None:
                bid_usd = float(t["bidVolume"]) * bid
            if t.get("askVolume") is not None:
                ask_usd = float(t["askVolume"]) * ask
        except (TypeError, ValueError):
            bid_usd = ask_usd = None
        if (bid_usd is not None and bid_usd < MIN_TOP_USD) or (ask_usd is not None and ask_usd < MIN_TOP_USD):
            continue

        out[symbol] = Quote(bid, ask, qvol, bid_usd, ask_usd)
    return out


async def fetch_all_tickers(ex) -> dict:
    """fetch_tickers, а если биржа не умеет — аккуратный фолбэк по топу USDT-пар."""
    if ex.has.get("fetchTickers"):
        return await ex.fetch_tickers()
    if not ex.markets:
        await ex.load_markets()
    symbols = [s for s, m in ex.markets.items()
               if s.endswith("/USDT") and ":" not in s and m.get("active", True) is not False][:80]
    sem = asyncio.Semaphore(8)

    async def one(sym):
        async with sem:
            try:
                return sym, await ex.fetch_ticker(sym)
            except Exception:
                return sym, None

    res = await asyncio.gather(*(one(s) for s in symbols))
    return {s: t for s, t in res if t}


async def exchange_poller(name: str):
    """Независимый цикл опроса одной биржи с экспоненциальным backoff."""
    cfg = EXCHANGES[name]
    ex = EXCH_OBJ[name]
    st = MARKET.status[name]
    while True:
        started = time.monotonic()
        try:
            tickers = await asyncio.wait_for(fetch_all_tickers(ex), timeout=FETCH_TIMEOUT)
            parsed = normalize_tickers(tickers)
            MARKET.books[name] = parsed
            MARKET.updated[name] = time.time()
            st.last_ok = time.time()
            st.latency = time.monotonic() - started
            st.pairs = len(parsed)
            if st.fails:
                logging.info(f"[{name}] восстановился ({len(parsed)} пар)")
            st.fails = 0
            st.last_error = ""
        except asyncio.CancelledError:
            raise
        except Exception as e:
            st.fails += 1
            st.last_error = f"{type(e).__name__}: {str(e)[:80]}"
            if st.fails in (1, 5) or st.fails % 20 == 0:
                logging.warning(f"[{name}] ошибка #{st.fails}: {st.last_error}")
        elapsed = time.monotonic() - started
        delay = cfg.poll_sec if st.fails == 0 else min(120.0, cfg.poll_sec * (2 ** min(st.fails, 5)))
        await asyncio.sleep(max(1.0, delay - elapsed))


def build_signals() -> List[Signal]:
    now = time.time()
    coin_map: Dict[str, Dict[str, Quote]] = {}
    for ex, book in MARKET.books.items():
        if now - MARKET.updated.get(ex, 0) > STALE_SEC:
            continue
        for sym, q in book.items():
            coin_map.setdefault(sym, {})[ex] = q

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    out: List[Signal] = []
    for sym, quotes in coin_map.items():
        if len(quotes) < 2:
            continue
        # Выбросы: один и тот же тикер может означать разные токены на разных биржах
        if len(quotes) >= 3:
            mids = sorted((q.bid + q.ask) / 2 for q in quotes.values())
            med = mids[len(mids) // 2]
            quotes = {e: q for e, q in quotes.items() if abs((q.bid + q.ask) / 2 / med - 1) <= OUTLIER_PCT}
            if len(quotes) < 2:
                continue

        buys = sorted(quotes.items(), key=lambda kv: kv[1].ask * (1 + FEES[kv[0]]))[:3]
        sells = sorted(quotes.items(), key=lambda kv: kv[1].bid * (1 - FEES[kv[0]]), reverse=True)[:3]
        best = None
        for bex, bq in buys:
            for sex, sq in sells:
                if bex == sex:
                    continue
                net = (sq.bid * (1 - FEES[sex])) / (bq.ask * (1 + FEES[bex])) - 1
                if best is None or net > best[0]:
                    best = (net, bex, bq, sex, sq)
        if not best:
            continue
        net, bex, bq, sex, sq = best
        net_pct = net * 100
        if not (MIN_NET_SPREAD <= net_pct <= MAX_NET_SPREAD):
            continue
        liq_vals = [v for v in (bq.ask_usd, sq.bid_usd) if v is not None]
        base = sym[:-5]
        out.append(Signal(
            symbol=sym, base=base, buy_ex=bex, sell_ex=sex,
            buy_price=bq.ask, sell_price=sq.bid,
            gross=round((sq.bid / bq.ask - 1) * 100, 2), net=round(net_pct, 2),
            volume=min(bq.qvol, sq.qvol), top_liq=min(liq_vals) if liq_vals else None,
            is_meme=is_meme(base), ts=ts, key=f"{base}|{bex}|{sex}",
        ))
    out.sort(key=lambda s: s.net, reverse=True)
    return out[:300]


def signal_matches_user(sig: Signal, u: dict) -> bool:
    if sig.net < u["min_spread"]:
        return False
    if sig.buy_ex not in u["exchanges"] or sig.sell_ex not in u["exchanges"]:
        return False
    if sig.volume < u["min_volume"]:
        return False
    if sig.is_meme and not u["allow_memes"]:
        return False
    if sig.base.upper() in u.get("blocked", ()):
        return False
    if MIN_DEAL_USD > u["balance"]:
        return False
    return True


def effective_amount(u: dict) -> float:
    bal = u["balance"]
    return min(bal, u["max_deal_amount"]) if u["max_deal_amount"] > 0 else bal


def render_signal_block(sig: Signal, amount: float, idx: Optional[int] = None) -> str:
    profit = amount * sig.net / 100.0
    liq_warn = " ⚠️" if sig.top_liq is not None and sig.top_liq < amount else ""
    head = f"{idx}. " if idx else ""
    comm = (FEES[sig.buy_ex] + FEES[sig.sell_ex]) * 100
    return (
        f"{head}<b>{sig.symbol}</b> · <b>+{sig.net}%</b> чистыми\n"
        f"🟢 <a href=\"{html.escape(trade_url(sig.buy_ex, sig.symbol), quote=True)}\">{ex_title(sig.buy_ex)}</a>"
        f" → <code>{fmt_price(sig.buy_price)}</code>\n"
        f"🔴 <a href=\"{html.escape(trade_url(sig.sell_ex, sig.symbol), quote=True)}\">{ex_title(sig.sell_ex)}</a>"
        f" → <code>{fmt_price(sig.sell_price)}</code>\n"
        f"📈 Грязный {sig.gross}% · комиссии −{comm:.2f}%\n"
        f"💵 На ${amount:,.0f}: <b>{money(profit)}</b>\n"
        f"📊 Объём 24ч: {fmt_vol(sig.volume)} · по лучшей цене: {fmt_vol(sig.top_liq)}{liq_warn}\n"
    )


# --- ФУНКЦИЯ 1: проверка по стакану ---
def simulate_arbitrage(asks, bids, budget: float, fee_buy: float, fee_sell: float) -> Optional[dict]:
    """Проходит по реальному стакану: покупка на `budget` USDT, продажа всей позиции."""
    eff_budget = budget / (1 + fee_buy)
    remaining, base_qty, spent = eff_budget, 0.0, 0.0
    for row in asks:
        p, q = float(row[0]), float(row[1])
        if p <= 0 or q <= 0:
            continue
        cost = p * q
        if cost >= remaining:
            base_qty += remaining / p
            spent += remaining
            remaining = 0.0
            break
        base_qty += q
        spent += cost
        remaining -= cost
    if base_qty <= 0:
        return None

    left, proceeds, sold = base_qty, 0.0, 0.0
    for row in bids:
        p, q = float(row[0]), float(row[1])
        if p <= 0 or q <= 0:
            continue
        take = min(left, q)
        proceeds += take * p
        sold += take
        left -= take
        if left <= 1e-12:
            break
    if sold <= 0:
        return None

    total_cost = spent * (1 + fee_buy)
    sold_cost = total_cost * (sold / base_qty)
    net_proceeds = proceeds * (1 - fee_sell)
    profit = net_proceeds - sold_cost
    return {
        "budget": budget,
        "filled": spent / eff_budget,
        "unsold": left / base_qty,
        "avg_buy": spent / base_qty,
        "avg_sell": proceeds / sold,
        "profit": profit,
        "roi": profit / sold_cost * 100 if sold_cost > 0 else 0.0,
        "cost": sold_cost,
    }


async def fetch_book(ex_name: str, symbol: str) -> dict:
    ex = EXCH_OBJ[ex_name]
    try:
        return await asyncio.wait_for(ex.fetch_order_book(symbol, 50), timeout=8)
    except asyncio.TimeoutError:
        raise
    except Exception:
        return await asyncio.wait_for(ex.fetch_order_book(symbol), timeout=8)


async def render_depth(base: str, buy_ex: str, sell_ex: str, user: dict) -> str:
    symbol = f"{base}/USDT"
    books = await asyncio.gather(fetch_book(buy_ex, symbol), fetch_book(sell_ex, symbol), return_exceptions=True)
    for ex_name, b in zip((buy_ex, sell_ex), books):
        if isinstance(b, Exception):
            return (f"❌ Не удалось получить стакан <b>{ex_title(ex_name)}</b> ({html.escape(symbol)}): "
                    f"<code>{html.escape(type(b).__name__)}</code>.\nПопробуйте позже.")
    buy_book, sell_book = books
    asks, bids = buy_book.get("asks") or [], sell_book.get("bids") or []
    if not asks or not bids:
        return "❌ Стакан пуст — связка недоступна."

    fb, fs = FEES[buy_ex], FEES[sell_ex]
    top_ask, top_bid = float(asks[0][0]), float(bids[0][0])
    user_amt = max(MIN_DEAL_USD, round(effective_amount(user), 2))
    sizes = sorted({user_amt, 100.0, 500.0, 1000.0})

    lines = []
    main_res = None
    for s in sizes:
        r = simulate_arbitrage(asks, bids, s, fb, fs)
        if not r:
            continue
        if s == user_amt:
            main_res = r
        shallow = r["filled"] < 0.98 or r["unsold"] > 0.02
        icon = "⚠️" if shallow else ("✅" if r["profit"] > 0 else "❌")
        mark = " ← ваша сумма" if s == user_amt else ""
        lines.append(f"{icon} ${s:,.0f}: <b>{money(r['profit'])}</b> ({r['roi']:+.2f}%){' · мало глубины' if shallow else ''}{mark}")

    gross_top = (top_bid / top_ask - 1) * 100
    head = (
        f"📊 <b>Стакан {symbol}</b>\n"
        f"🟢 {ex_title(buy_ex)} ask: <code>{fmt_price(top_ask)}</code>\n"
        f"🔴 {ex_title(sell_ex)} bid: <code>{fmt_price(top_bid)}</code>\n"
        f"📈 Спред по лучшим ценам: {gross_top:.2f}% · комиссии −{(fb + fs) * 100:.2f}%\n\n"
    )
    body = "<b>Реальный профит с проскальзыванием:</b>\n" + "\n".join(lines) + "\n"
    if main_res:
        slip_buy = (main_res["avg_buy"] / top_ask - 1) * 100
        slip_sell = (1 - main_res["avg_sell"] / top_bid) * 100
        body += (f"\n🔍 Ср. цена покупки: <code>{fmt_price(main_res['avg_buy'])}</code> (+{slip_buy:.2f}%)\n"
                 f"🔍 Ср. цена продажи: <code>{fmt_price(main_res['avg_sell'])}</code> (−{slip_sell:.2f}%)\n")
        if main_res["profit"] <= 0:
            verdict = "❌ <b>Вердикт:</b> на вашу сумму связка убыточна."
        elif main_res["filled"] < 0.98 or main_res["unsold"] > 0.02:
            verdict = "⚠️ <b>Вердикт:</b> глубины стакана не хватает на вашу сумму — уменьшите объём."
        elif main_res["roi"] < 0.1:
            verdict = "⚠️ <b>Вердикт:</b> профит минимальный, комиссия сети его съест."
        else:
            verdict = "✅ <b>Вердикт:</b> связка исполнима на вашу сумму."
        body += f"\n{verdict}\n"
    body += "\n<i>Комиссии вывода/ввода и задержки сети не учтены.</i>"
    return head + body


# --- Рассылка сигналов ---
ALERT_STATE: Dict[tuple, Tuple[float, float]] = {}
USER_ALERT_TIMES: Dict[int, List[float]] = {}
_ALERT_USERS_CACHE: Tuple[float, List[dict]] = (0.0, [])


def should_alert(k: tuple, net: float, now: float) -> bool:
    last = ALERT_STATE.get(k)
    if last and now - last[0] < ALERT_COOLDOWN and net < last[1] + ALERT_REARM_DELTA:
        return False
    ALERT_STATE[k] = (now, net)
    return True


async def get_alert_users() -> List[dict]:
    global _ALERT_USERS_CACHE
    ts, cached = _ALERT_USERS_CACHE
    if time.time() - ts < 30:
        return cached
    async with db_mgr.conn.execute(f"SELECT {USER_COLUMNS} FROM users WHERE dm_alerts = 1") as cur:
        rows = await cur.fetchall()
    now = now_ts()
    users = []
    for r in rows:
        u = _row_to_user(r)
        if u["pause_until"] > now:
            continue
        if u["user_id"] in ADMIN_IDS or u["sub_expiry"] > now:
            users.append(u)
    _ALERT_USERS_CACHE = (time.time(), users)
    return users


def invalidate_alert_cache():
    global _ALERT_USERS_CACHE
    _ALERT_USERS_CACHE = (0.0, [])


async def safe_send(bot: Bot, chat_id: int, text: str, kb: Optional[InlineKeyboardMarkup] = None) -> str:
    """Возвращает 'ok' | 'blocked' | 'error'. Переживает flood-wait."""
    for attempt in range(2):
        try:
            await bot.send_message(chat_id, text, reply_markup=kb)
            return "ok"
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
        except TelegramForbiddenError:
            return "blocked"
        except Exception as e:
            logging.debug(f"send error {chat_id}: {e}")
            return "error"
    return "error"


def alert_kb(sig: Signal, with_depth: bool, with_mute: bool) -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton(text=f"🟢 {ex_title(sig.buy_ex)}", url=trade_url(sig.buy_ex, sig.symbol)),
        InlineKeyboardButton(text=f"🔴 {ex_title(sig.sell_ex)}", url=trade_url(sig.sell_ex, sig.symbol)),
    ]]
    if with_depth:
        rows.append([InlineKeyboardButton(text="📊 Проверить стакан",
                                          callback_data=f"d|{sig.base}|{sig.buy_ex}|{sig.sell_ex}")])
    if with_mute:
        rows.append([InlineKeyboardButton(text="🔕 Отключить уведомления", callback_data="dm_off")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def dispatch_alerts(bot: Bot, signals: List[Signal]):
    now = time.time()
    fresh = [s for s in signals if s.streak >= ALERT_MIN_STREAK]
    if len(ALERT_STATE) > 20000:
        for k in [k for k, v in ALERT_STATE.items() if now - v[0] > ALERT_COOLDOWN * 4]:
            ALERT_STATE.pop(k, None)
    if not fresh:
        return

    # 1) Публичный канал
    sent = 0
    for s in fresh:
        if s.net < CHANNEL_MIN_NET or s.is_meme or not should_alert(("ch", s.key), s.net, now):
            continue
        text = "🔔 <b>Арбитражный сигнал</b>\n\n" + render_signal_block(s, 100.0) + (
            f"⏱ {s.ts} UTC · подтверждён {s.streak} сканами\n\n"
            "<i>Проверьте статус ввода/вывода и сеть перед переводом.</i>"
        )
        status = await safe_send(bot, CHANNEL_SIGNALS_ID, text, alert_kb(s, False, False))
        if status != "ok":
            logging.error(f"Не удалось отправить сигнал в канал: {status}")
        sent += 1
        if sent >= CHANNEL_MAX_PER_CYCLE:
            break

    # 2) Персональные уведомления (ФУНКЦИЯ 2)
    users = await get_alert_users()
    if not users:
        return
    jobs: List[Tuple[int, str, InlineKeyboardMarkup]] = []
    for s in fresh[:DM_MAX_SIGNALS_PER_CYCLE]:
        for u in users:
            uid = u["user_id"]
            if not signal_matches_user(s, u):
                continue
            times = [t for t in USER_ALERT_TIMES.get(uid, []) if now - t < 60]
            USER_ALERT_TIMES[uid] = times
            if len(times) >= DM_MAX_PER_MIN:
                continue
            if not should_alert(("u", uid, s.key), s.net, now):
                continue
            times.append(now)
            amount = effective_amount(u)
            text = "🔔 <b>Персональный сигнал</b>\n\n" + render_signal_block(s, amount) + f"⏱ {s.ts} UTC"
            jobs.append((uid, text, alert_kb(s, True, True)))

    sem = asyncio.Semaphore(15)

    async def worker(uid, text, kb):
        async with sem:
            status = await safe_send(bot, uid, text, kb)
            if status == "blocked":
                await update_user_field(uid, "dm_alerts", 0)
                invalidate_alert_cache()

    if jobs:
        await asyncio.gather(*(worker(*j) for j in jobs))


async def analyzer_loop(bot: Bot):
    global LATEST_SIGNALS, STREAK
    while True:
        t0 = time.monotonic()
        try:
            signals = build_signals()
            prev = STREAK
            STREAK = {s.key: prev.get(s.key, 0) + 1 for s in signals}
            for s in signals:
                s.streak = STREAK[s.key]
            LATEST_SIGNALS = signals
            await dispatch_alerts(bot, signals)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception("Analyzer error")
        await asyncio.sleep(max(1.0, ANALYZE_INTERVAL - (time.monotonic() - t0)))


async def db_backup_loop():
    """Раз в сутки делает копию базы (хранит 7 последних)."""
    import glob
    while True:
        await asyncio.sleep(3600)
        try:
            day = datetime.now(timezone.utc).strftime("%Y%m%d")
            target = f"{DB_NAME}.{day}.bak"
            if not os.path.exists(target):
                await db_mgr.conn.execute(f"VACUUM INTO '{target}'")
                for old in sorted(glob.glob(f"{DB_NAME}.*.bak"))[:-7]:
                    os.remove(old)
                logging.info(f"Бэкап базы: {target}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.warning(f"Бэкап базы не удался: {e}")


def render_status() -> str:
    t = time.time()
    lines = ["🏦 <b>Статус бирж</b>\n"]
    ok_count = 0
    for name in EXCHANGE_NAMES:
        st = MARKET.status[name]
        age = t - MARKET.updated.get(name, 0)
        if st.last_ok and age <= STALE_SEC:
            ok_count += 1
            lines.append(f"✅ <b>{ex_title(name)}</b> — {st.pairs} пар · {st.latency:.1f}с · {age:.0f}с назад")
        elif st.last_ok:
            lines.append(f"⚠️ <b>{ex_title(name)}</b> — данные устарели ({age:.0f}с)")
        elif st.fails:
            lines.append(f"❌ <b>{ex_title(name)}</b> — <code>{html.escape(st.last_error)}</code>")
        else:
            lines.append(f"⏳ <b>{ex_title(name)}</b> — подключение…")
    lines.append(f"\nОнлайн: <b>{ok_count}/{len(EXCHANGE_NAMES)}</b> · Связок сейчас: <b>{len(LATEST_SIGNALS)}</b>")
    return "\n".join(lines)


# =============================================================================
#                           ОПЛАТА / БИЛЛИНГ
# =============================================================================
async def apply_paid_invoice(bot: Bot, row_id: int, user_id: int, plan: str) -> bool:
    """Идемпотентно зачисляет оплату. True — если подписка выдана именно сейчас."""
    cur = await db_mgr.conn.execute(
        "UPDATE invoices SET status = 'paid' WHERE id = ? AND status = 'active'", (row_id,)
    )
    await db_mgr.conn.commit()
    if cur.rowcount == 0:
        return False
    info = PRICES.get(plan)
    if not info:
        logging.error(f"Оплачен счёт с неизвестным тарифом: {plan}")
        return True
    new_exp = await add_subscription(user_id, info["days"])
    kb, _ = await render_main(user_id)
    await safe_send(
        bot, user_id,
        f"✅ <b>Оплата получена</b>\n\nТариф: {info['name']}\nДействует до: <code>{fmt_dt(new_exp)}</code>\n\n"
        f"Все функции сканера и персональные уведомления доступны.", kb,
    )
    for admin in ADMIN_IDS:
        await safe_send(bot, admin, f"💰 Оплата: ID <code>{user_id}</code> · {info['name']} · ${info['usd']}")
    invalidate_alert_cache()
    return True


async def background_billing_checker(bot: Bot):
    if not CRYPTO_PAY_TOKEN:
        logging.warning("CRYPTO_PAY_TOKEN не задан — оплата подписки отключена.")
    while True:
        try:
            db = db_mgr.conn
            invoices = []
            if CRYPTO_PAY_TOKEN:
                async with db.execute(
                    "SELECT id, invoice_id, user_id, plan, created_at FROM invoices "
                    "WHERE status = 'active' AND provider = 'cryptobot'"
                ) as cur:
                    invoices = await cur.fetchall()

            for i in range(0, len(invoices), 50):
                chunk = invoices[i:i + 50]
                remote = await crypto_pay.get_invoices([r[1] for r in chunk])
                for row_id, inv_id, u_id, plan, created in chunk:
                    inv = remote.get(inv_id)
                    st = inv.get("status") if inv else None
                    try:
                        if st == "paid":
                            await apply_paid_invoice(bot, row_id, u_id, plan)
                        elif st == "expired" or (created and now_ts() - created > 7200 and st != "paid"):
                            await db.execute("UPDATE invoices SET status = 'expired' WHERE id = ? AND status = 'active'",
                                             (row_id,))
                            await db.commit()
                    except Exception as e:
                        logging.error(f"Invoice {inv_id} error: {e}")

            # Напоминание об окончании подписки
            now = now_ts()
            async with db.execute(
                "SELECT user_id, sub_expiry FROM users WHERE notified_24h = 0 AND sub_expiry > ?", (now,)
            ) as cur:
                users = await cur.fetchall()
            for u_id, exp_ts in users:
                exp = parse_expiry(exp_ts)
                if exp and now < exp <= now + 86400:
                    kb = InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="💎 Продлить", callback_data="menu_buy")]])
                    await safe_send(bot, u_id, "⏳ Срок действия PRO-подписки истекает менее чем через 24 часа.", kb)
                    await db.execute("UPDATE users SET notified_24h = 1 WHERE user_id = ?", (u_id,))
                    await db.commit()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"Billing bg error: {e}")
        await asyncio.sleep(10)


# =============================================================================
#                           AIOGRAM: ИНИЦИАЛИЗАЦИЯ
# =============================================================================
if DefaultBotProperties is None:
    bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
else:
    try:
        _defaults = DefaultBotProperties(parse_mode=ParseMode.HTML, link_preview_is_disabled=True)
    except TypeError:  # старые версии aiogram
        _defaults = DefaultBotProperties(parse_mode=ParseMode.HTML)
    bot = Bot(token=BOT_TOKEN, default=_defaults)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
admin_router = Router()
BOT_USERNAME = ""
BG_TASKS: set = set()


class IsAdmin(BaseFilter):
    async def __call__(self, event) -> bool:
        return bool(event.from_user and event.from_user.id in ADMIN_IDS)


admin_router.message.filter(IsAdmin())
admin_router.callback_query.filter(IsAdmin())
for _r in (router, admin_router):
    _r.message.filter(F.chat.type == "private")


class ThrottleMiddleware(BaseMiddleware):
    """Защита от флуда нажатиями кнопок."""
    def __init__(self, delay: float = 0.5):
        self.delay = delay
        self.last: Dict[int, float] = {}

    async def __call__(self, handler, event, data):
        uid = event.from_user.id if getattr(event, "from_user", None) else 0
        now = time.monotonic()
        if now - self.last.get(uid, 0) < self.delay:
            if isinstance(event, CallbackQuery):
                try:
                    await event.answer()
                except Exception:
                    pass
            return None
        self.last[uid] = now
        if len(self.last) > 20000:
            self.last.clear()
        return await handler(event, data)


router.callback_query.middleware(ThrottleMiddleware(0.5))


class ChannelGate(BaseMiddleware):
    """Не пускает к функциям бота без подписки на обязательный канал (кэш 2 мин)."""
    TTL = 120.0

    def __init__(self):
        self.ok_until: Dict[int, float] = {}

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if not user or user.id in ADMIN_IDS:
            return await handler(event, data)
        if isinstance(event, Message) and (event.text or "").startswith("/start"):
            return await handler(event, data)
        if isinstance(event, CallbackQuery) and event.data == "check_sub":
            return await handler(event, data)
        now = time.monotonic()
        if self.ok_until.get(user.id, 0) > now:
            return await handler(event, data)
        if await check_channel_sub(user.id):
            await update_user(user.id, user.username or "User")
            self.ok_until[user.id] = now + self.TTL
            if len(self.ok_until) > 50000:
                self.ok_until.clear()
            return await handler(event, data)
        await send_join_prompt(event)
        return None


async def send_join_prompt(event):
    clean = REQUIRED_CHANNEL_ID.replace("@", "")
    kb = kb_of([
        [InlineKeyboardButton(text="📢 Подписаться на канал", url=f"https://t.me/{clean}")],
        [btn("✅ Проверить подписку", "check_sub")],
    ])
    text = "<b>Приветствуем!</b>\n\nДля доступа к сканеру подпишитесь на наш канал."
    if isinstance(event, CallbackQuery):
        try:
            await event.answer("Сначала подпишитесь на канал.", show_alert=True)
        except Exception:
            pass
        await safe_send(bot, event.from_user.id, text, kb)
    else:
        await event.answer(text, reply_markup=kb)


_gate = ChannelGate()
router.message.middleware(_gate)
router.callback_query.middleware(_gate)


class Form(StatesGroup):
    waiting_for_broadcast = State()
    confirm_broadcast = State()
    waiting_for_grant_id = State()
    waiting_for_grant_days = State()
    waiting_for_revoke_id = State()
    waiting_for_trade_pair = State()
    waiting_for_trade_amount = State()
    waiting_for_trade_profit = State()
    waiting_for_calc_input = State()
    waiting_for_balance = State()
    waiting_for_max_deal = State()


def kb_of(rows: List[List[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=rows)


def btn(text: str, cb: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(text=text, callback_data=cb)


def back_row(cb: str = "menu_main", text: str = "⬅️ Назад") -> List[InlineKeyboardButton]:
    return [btn(text, cb)]


async def respond(event, text: str, kb: Optional[InlineKeyboardMarkup] = None):
    """Универсальный ответ: редактирует сообщение для callback, отправляет новое для message."""
    if isinstance(event, CallbackQuery):
        try:
            await event.answer()
        except Exception:
            pass
        msg = event.message
        if isinstance(msg, Message):
            try:
                await msg.edit_text(text, reply_markup=kb)
                return
            except TelegramBadRequest as e:
                if "not modified" in str(e).lower():
                    return
            except Exception as e:
                logging.debug(f"edit error: {e}")
            try:
                await msg.answer(text, reply_markup=kb)
                return
            except Exception:
                pass
        await bot.send_message(event.from_user.id, text, reply_markup=kb)
    else:
        await event.answer(text, reply_markup=kb)


async def check_channel_sub(user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(REQUIRED_CHANNEL_ID, user_id)
        return m.status in ("creator", "administrator", "member")
    except Exception as e:
        logging.warning(f"Проверка подписки на канал не удалась (бот должен быть админом канала): {e}")
        return True


# --- Реферальная программа (ФУНКЦИЯ 3) ---
async def process_referral(user_id: int):
    db = db_mgr.conn
    async with db.execute("SELECT referred_by, ref_rewarded FROM users WHERE user_id = ?", (user_id,)) as cur:
        row = await cur.fetchone()
    if not row or not row[0] or row[1]:
        return
    inviter = row[0]
    cur = await db.execute("UPDATE users SET ref_rewarded = 1 WHERE user_id = ? AND ref_rewarded = 0", (user_id,))
    await db.commit()
    if cur.rowcount == 0:
        return
    exp = await add_subscription(user_id, REF_INVITEE_DAYS)
    await safe_send(bot, user_id,
                    f"🎁 Бонус за приглашение: +{REF_INVITEE_DAYS} дн. PRO. Действует до <code>{fmt_dt(exp)}</code>")
    async with db.execute("SELECT COUNT(*) FROM users WHERE referred_by = ? AND ref_rewarded = 1", (inviter,)) as c2:
        rewarded = (await c2.fetchone())[0]
    if rewarded <= REF_MAX_REWARDED:
        await add_subscription(inviter, REF_INVITER_DAYS)
        await safe_send(bot, inviter, f"🎁 По вашей ссылке пришёл новый пользователь: +{REF_INVITER_DAYS} дн. PRO!")
    invalidate_alert_cache()


async def render_referral(user_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
    async with db_mgr.conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(ref_rewarded), 0) FROM users WHERE referred_by = ?", (user_id,)
    ) as cur:
        total, rewarded = await cur.fetchone()
    text = (
        "🎁 <b>Реферальная программа</b>\n\n"
        f"За каждого нового друга:\n• вы получаете <b>+{REF_INVITER_DAYS} дн.</b> PRO\n"
        f"• друг получает <b>+{REF_INVITEE_DAYS} дн.</b> PRO\n\n"
        f"Ваша ссылка:\n<code>{link}</code>\n\n"
        f"👥 Перешло по ссылке: <b>{total}</b>\n✅ Засчитано: <b>{rewarded}</b> (лимит наград: {REF_MAX_REWARDED})\n\n"
        "<i>Бонус начисляется после подтверждения подписки на канал.</i>"
    )
    share = f"https://t.me/share/url?url={quote(link)}&text={quote('Арбитражный сканер: сигналы по 13 биржам')}"
    return text, kb_of([[InlineKeyboardButton(text="📤 Поделиться", url=share)], back_row()])


# --- Главное меню ---
async def render_main(user_id: int) -> Tuple[InlineKeyboardMarkup, str]:
    has_sub = await is_user_subscribed(user_id)
    online = len(MARKET.online())
    if user_id in ADMIN_IDS:
        sub_line = "👑 Администратор"
    elif has_sub:
        left = -(-(await get_sub_expiry(user_id) - now_ts()) // 86400)
        sub_line = f"💎 PRO · осталось {max(1, left)} дн."
    else:
        sub_line = "🔒 PRO не активна"

    rows: List[List[InlineKeyboardButton]] = []
    if has_sub:
        rows.append([btn("📡 Сканер сигналов", "signals:0"), btn("⚙️ Фильтры", "menu_settings")])
    else:
        rows.append([btn("💎 Оформить PRO", "menu_buy")])
    rows.append([btn("📊 Дневник сделок", "menu_trades"), btn("👤 Профиль", "menu_profile")])
    rows.append([btn("🎁 Рефералы", "menu_ref"), btn("🏦 Статус бирж", "menu_status")])
    rows.append([btn("📖 Инструкция", "menu_guide"),
                 InlineKeyboardButton(text="👨‍💻 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}")])
    if user_id in ADMIN_IDS:
        rows.append([btn("👑 Админ-панель", "menu_admin")])

    names = ", ".join(c.title for c in EXCHANGES.values())
    text = (
        "<b>Arbitrage Terminal</b>\n\n"
        f"• Статус: <code>ONLINE</code> ({online}/{len(EXCHANGE_NAMES)} бирж)\n"
        f"• Подписка: {sub_line}\n"
        f"• Мониторинг: {names}\n"
        f"• Связок сейчас: <b>{len(LATEST_SIGNALS)}</b> · обновление каждые {ANALYZE_INTERVAL:.0f} сек\n\n"
        "Выберите раздел:"
    )
    return kb_of(rows), text


async def show_main(event):
    kb, text = await render_main(event.from_user.id)
    await respond(event, text, kb)


# =============================================================================
#                           ПОЛЬЗОВАТЕЛЬСКИЕ ХЕНДЛЕРЫ
# =============================================================================
@router.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    ref_id = None
    args = (command.args or "").strip()
    if args.startswith("ref_") and args[4:].isdigit():
        cand = int(args[4:])
        if cand != uid and await user_exists(cand):
            ref_id = cand
    await update_user(uid, message.from_user.username or "User", ref_id)

    if not await check_channel_sub(uid):
        await send_join_prompt(message)
        return
    _gate.ok_until[uid] = time.monotonic() + ChannelGate.TTL
    await process_referral(uid)
    await show_main(message)


@router.message(Command("status"))
async def cmd_status(message: Message):
    await message.answer(render_status(), reply_markup=kb_of([back_row("menu_main", "Главное меню")]))


@router.message(Command("ref"))
async def cmd_ref(message: Message):
    text, kb = await render_referral(message.from_user.id)
    await message.answer(text, reply_markup=kb)


@router.message(Command("calc"))
async def cmd_calc(message: Message, state: FSMContext):
    await state.set_state(Form.waiting_for_calc_input)
    await message.answer(CALC_PROMPT, reply_markup=kb_of([back_row("menu_trades", "К дневнику")]))


@router.message(Command("pause"))
async def cmd_pause(message: Message, command: CommandObject):
    arg = (command.args or "").strip().replace(",", ".")
    try:
        hours = float(arg)
        if not (0 < hours <= 168):
            raise ValueError
    except ValueError:
        return await message.answer("Формат: <code>/pause 2</code> — отключить push на 2 часа (макс. 168).")
    uid = message.from_user.id
    await update_user(uid, message.from_user.username or "User")
    until = now_ts() + int(hours * 3600)
    await update_user_field(uid, "pause_until", until)
    invalidate_alert_cache()
    await message.answer(f"🔕 Push-уведомления на паузе до <code>{fmt_dt(until)}</code>.\nВернуть сразу: /resume")


@router.message(Command("resume"))
async def cmd_resume(message: Message):
    await update_user_field(message.from_user.id, "pause_until", 0)
    invalidate_alert_cache()
    await message.answer("🔔 Push-уведомления снова включены.")


@router.message(Command("export"))
async def cmd_export_trades(message: Message):
    async with db_mgr.conn.execute(
        "SELECT created_at, pair_info, amount_usd, profit_usd, roi_percent FROM trades "
        "WHERE user_id = ? ORDER BY id", (message.from_user.id,)
    ) as cur:
        rows = await cur.fetchall()
    if not rows:
        return await message.answer("Дневник сделок пуст — экспортировать нечего.")
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Дата", "Связка", "Сумма $", "Профит $", "ROI %"])
    for r in rows:
        w.writerow([r[0], r[1], f"{r[2]:.2f}", f"{r[3]:.2f}", f"{r[4]:.2f}"])
    await message.answer_document(
        BufferedInputFile(out.getvalue().encode("utf-8-sig"), filename="my_trades.csv"),
        caption=f"Ваш дневник сделок ({len(rows)} записей).")


HELP_TEXT = (
    "<b>📚 Команды</b>\n\n"
    "/start — главное меню\n"
    "/top — топ связок под ваши фильтры (PRO)\n"
    "/status — статус бирж\n"
    "/calc — калькулятор арбитража\n"
    "/block SOL — скрыть монету из сигналов\n"
    "/unblock SOL — вернуть монету\n"
    "/blocklist — список скрытых монет\n"
    "/pause 2 — отключить push на 2 часа · /resume — включить\n"
    "/export — выгрузить дневник сделок в CSV\n"
    "/ref — реферальная программа\n"
    "/promo КОД — активировать промокод"
)


@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(HELP_TEXT, reply_markup=kb_of([back_row("menu_main", "Главное меню")]))


@router.message(Command("top"))
async def cmd_top(message: Message):
    uid = message.from_user.id
    if not await is_user_subscribed(uid):
        return await message.answer("Раздел доступен только с PRO-подпиской.",
                                    reply_markup=kb_of([[btn("💎 Оформить PRO", "menu_buy")]]))
    u = await get_user_data(uid)
    if u["balance"] <= 0:
        return await message.answer("Ваш баланс равен $0. Задайте его в разделе «Профиль».")
    amount = effective_amount(u)
    top = [s for s in LATEST_SIGNALS if signal_matches_user(s, u)][:5]
    if not top:
        return await message.answer("Сейчас нет связок под ваши фильтры. Загляните позже или ослабьте фильтры.",
                                    reply_markup=kb_of([[btn("⚙️ Фильтры", "menu_settings")]]))
    text = f"<b>🏆 Топ связок (${amount:,.2f})</b>\n\n"
    for i, s in enumerate(top, 1):
        text += render_signal_block(s, amount, i) + f"⏱ {s.ts} UTC · скан ×{s.streak}\n\n"
    rows = [[btn(f"📊 {s.base}", f"d|{s.base}|{s.buy_ex}|{s.sell_ex}") for s in top]]
    rows.append([btn("📡 Весь сканер", "signals:0")])
    await message.answer(text, reply_markup=kb_of(rows))


def _coin_arg(command: CommandObject) -> str:
    parts = (command.args or "").replace(",", " ").split()
    coin = parts[0].upper().replace("/USDT", "") if parts else ""
    return coin if coin.isalnum() and len(coin) <= 20 else ""


@router.message(Command("block"))
async def cmd_block(message: Message, command: CommandObject):
    coin = _coin_arg(command)
    if not coin:
        return await message.answer("Формат: <code>/block SOL</code>")
    uid = message.from_user.id
    await update_user(uid, message.from_user.username or "User")
    lst = (await get_user_data(uid))["blocked"]
    if coin in lst:
        return await message.answer(f"<b>{coin}</b> уже в чёрном списке.")
    if len(lst) >= 100:
        return await message.answer("Лимит чёрного списка — 100 монет.")
    lst.append(coin)
    await update_user_field(uid, "blocked_coins", json.dumps(lst))
    invalidate_alert_cache()
    await message.answer(f"🚫 <b>{coin}</b> скрыта из сигналов и уведомлений. Вернуть: <code>/unblock {coin}</code>")


@router.message(Command("unblock"))
async def cmd_unblock(message: Message, command: CommandObject):
    coin = _coin_arg(command)
    if not coin:
        return await message.answer("Формат: <code>/unblock SOL</code>")
    uid = message.from_user.id
    lst = (await get_user_data(uid))["blocked"]
    if coin not in lst:
        return await message.answer(f"<b>{coin}</b> нет в чёрном списке.")
    lst.remove(coin)
    await update_user_field(uid, "blocked_coins", json.dumps(lst))
    invalidate_alert_cache()
    await message.answer(f"✅ <b>{coin}</b> снова участвует в сигналах.")


@router.message(Command("blocklist"))
async def cmd_blocklist(message: Message):
    lst = (await get_user_data(message.from_user.id))["blocked"]
    await message.answer("🚫 Чёрный список: " + (", ".join(lst) if lst else "пуст") +
                         "\n\nДобавить: <code>/block SOL</code>")


@router.message(Command("promo"))
async def cmd_promo(message: Message, command: CommandObject):
    code = (command.args or "").strip()
    if not code:
        await message.answer("Формат: <code>/promo КОД</code>")
        return
    await update_user(message.from_user.id, message.from_user.username or "User")
    ok, text = await activate_promo(message.from_user.id, code)
    if ok:
        invalidate_alert_cache()
        kb, _ = await render_main(message.from_user.id)
        await message.answer(text, reply_markup=kb)
    else:
        await message.answer(text)


@router.message(Command("free1"))
async def cmd_promo_free1(message: Message):
    await update_user(message.from_user.id, message.from_user.username or "User")
    ok, text = await activate_promo(message.from_user.id, "free1")
    if ok:
        invalidate_alert_cache()
        kb, _ = await render_main(message.from_user.id)
        await message.answer(text, reply_markup=kb)
    else:
        await message.answer(text)


@router.callback_query(F.data == "menu_main")
async def cb_menu_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_main(call)


@router.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery):
    if await check_channel_sub(call.from_user.id):
        _gate.ok_until[call.from_user.id] = time.monotonic() + ChannelGate.TTL
        await update_user(call.from_user.id, call.from_user.username or "User")
        await process_referral(call.from_user.id)
        await show_main(call)
    else:
        await call.answer("Подписка на канал не обнаружена.", show_alert=True)


@router.callback_query(F.data == "menu_status")
async def cb_status(call: CallbackQuery):
    await respond(call, render_status(), kb_of([[btn("🔄 Обновить", "menu_status")], back_row()]))


@router.callback_query(F.data == "menu_ref")
async def cb_ref(call: CallbackQuery):
    text, kb = await render_referral(call.from_user.id)
    await respond(call, text, kb)


# --- Профиль ---
@router.callback_query(F.data == "menu_profile")
async def cb_profile(call: CallbackQuery, state: FSMContext):
    await state.clear()
    uid = call.from_user.id
    u = await get_user_data(uid)
    stats = await get_user_trade_stats(uid)
    if uid in ADMIN_IDS:
        sub_badge, exp_info = "PRO (Administrator)", "Бессрочно"
    elif u["sub_expiry"] > now_ts():
        sub_badge, exp_info = "PRO", fmt_dt(u["sub_expiry"])
    else:
        sub_badge, exp_info = "Не активна", "—"

    uname = html.escape(call.from_user.username or "не указан")
    max_deal = f"${u['max_deal_amount']:.2f}" if u["max_deal_amount"] > 0 else "без ограничений"
    text = (
        "<b>👤 Личный профиль</b>\n\n"
        f"• ID: <code>{uid}</code>\n"
        f"• Юзернейм: @{uname}\n"
        f"• Подписка: <b>{sub_badge}</b>\n"
        f"• Активна до: <code>{exp_info}</code>\n\n"
        "⚙️ <b>Капитал и фильтры:</b>\n"
        f"💰 Баланс: <b>${u['balance']:.2f} USDT</b>\n"
        f"🎯 Лимит на сделку: <b>{max_deal}</b>\n"
        f"📊 Мин. спред: <code>{u['min_spread']}%</code> · мин. объём: <code>{fmt_vol(u['min_volume'])}</code>\n"
        f"🔔 Личные уведомления: <b>{'вкл' if u['dm_alerts'] else 'выкл'}</b>\n\n"
        "<b>Статистика торговли:</b>\n"
        f"• Сделок: {stats['count']} (в плюс: {stats['wins']})\n"
        f"• Оборот: ${stats['total_volume']:,.2f}\n"
        f"• Профит всего: {money(stats['total_profit'])} · за 7 дн.: {money(stats['profit_7d'])}\n"
        f"• Средний ROI: {stats['avg_roi']:+.2f}%"
    )
    kb = kb_of([
        [btn("💰 Изменить баланс", "profile_edit_balance"), btn("🎯 Лимит сделки", "profile_edit_max_deal")],
        back_row(),
    ])
    await respond(call, text, kb)


@router.callback_query(F.data == "profile_edit_balance")
async def cb_edit_balance(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_balance)
    await respond(call,
                  "<b>Настройка баланса</b>\n\nВведите ваш баланс в USDT. Сканер считает профит под эту сумму.\n\n"
                  "<i>Пример: <code>50</code> или <code>150.5</code></i>",
                  kb_of([back_row("menu_profile", "Отмена")]))


@router.message(Form.waiting_for_balance)
async def process_balance_input(message: Message, state: FSMContext):
    vals = parse_floats(msg_text(message), 1, 1)
    if not vals or vals[0] < 0 or vals[0] > 1e9:
        await message.answer("Введите корректную сумму числом (например: 50):")
        return
    await update_user_field(message.from_user.id, "balance", vals[0])
    await state.clear()
    kb, _ = await render_main(message.from_user.id)
    await message.answer(f"✅ Баланс обновлён: <b>${vals[0]:.2f} USDT</b>", reply_markup=kb)


@router.callback_query(F.data == "profile_edit_max_deal")
async def cb_edit_max_deal(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_max_deal)
    await respond(call,
                  "<b>Лимит на 1 сделку</b>\n\nМаксимум USDT на одну связку. <code>0</code> — использовать весь баланс.\n\n"
                  "<i>Пример: <code>25</code></i>",
                  kb_of([back_row("menu_profile", "Отмена")]))


@router.message(Form.waiting_for_max_deal)
async def process_max_deal_input(message: Message, state: FSMContext):
    vals = parse_floats(msg_text(message), 1, 1)
    if not vals or vals[0] < 0 or vals[0] > 1e9:
        await message.answer("Введите корректную сумму числом (например: 25):")
        return
    await update_user_field(message.from_user.id, "max_deal_amount", vals[0])
    await state.clear()
    kb, _ = await render_main(message.from_user.id)
    await message.answer(
        f"✅ Лимит на сделку: <b>${vals[0]:.2f} USDT</b>" if vals[0] > 0 else "✅ Лимит снят (весь баланс).",
        reply_markup=kb)


# --- Дневник и калькулятор ---
@router.callback_query(F.data == "menu_trades")
async def cb_trades_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    stats = await get_user_trade_stats(call.from_user.id)
    recent = await get_recent_trades(call.from_user.id, limit=3)
    history = ""
    if recent:
        history = "\n<b>Недавние записи:</b>\n"
        for p_info, amt, prof, roi, _ in recent:
            history += f"• {html.escape(str(p_info))} | ${amt:,.2f} | {money(prof)} ({roi:+.2f}%)\n"
    text = (
        "<b>📊 Дневник сделок и калькулятор</b>\n\n"
        f"• Оборот: <code>${stats['total_volume']:,.2f}</code>\n"
        f"• Профит: <code>{money(stats['total_profit'])}</code> (7 дн.: {money(stats['profit_7d'])})\n"
        f"• Всего сделок: <code>{stats['count']}</code>\n{history}"
    )
    kb = kb_of([
        [btn("➕ Новая запись", "trade_add"), btn("🧮 Калькулятор", "trade_calc")],
        [btn("📜 Вся история", "trade_history"), btn("🗑 Удалить последнюю", "trade_del_last")],
        back_row("menu_main", "Главное меню"),
    ])
    await respond(call, text, kb)


@router.callback_query(F.data == "trade_del_last")
async def cb_trade_del_last(call: CallbackQuery, state: FSMContext):
    ok = await delete_last_trade(call.from_user.id)
    await call.answer("Последняя запись удалена." if ok else "Записей нет.", show_alert=not ok)
    await cb_trades_menu(call, state)


@router.callback_query(F.data == "trade_add")
async def cb_trade_add(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_trade_pair)
    await respond(call,
                  "<b>Добавление сделки (шаг 1 из 3)</b>\n\nУкажите связку или пару:\n"
                  "<i>Пример: <code>SOL Binance -> Bybit</code></i>",
                  kb_of([back_row("menu_trades", "Отмена")]))


@router.message(Form.waiting_for_trade_pair)
async def process_trade_pair(message: Message, state: FSMContext):
    pair = msg_text(message)[:100]
    if not pair:
        await message.answer("Отправьте текстом, например: <code>SOL Binance -> Bybit</code>")
        return
    await state.update_data(pair_info=pair)
    await state.set_state(Form.waiting_for_trade_amount)
    await message.answer("<b>Шаг 2 из 3</b>\n\nСумма входа в $:\n<i>Пример: <code>1000</code></i>")


@router.message(Form.waiting_for_trade_amount)
async def process_trade_amount(message: Message, state: FSMContext):
    vals = parse_floats(msg_text(message), 1, 1)
    if not vals or vals[0] <= 0 or vals[0] > 1e9:
        await message.answer("Укажите корректную сумму числом (например: 1000).")
        return
    await state.update_data(amount_usd=vals[0])
    await state.set_state(Form.waiting_for_trade_profit)
    await message.answer("<b>Шаг 3 из 3</b>\n\nЧистый профит в $ (при убытке — со знаком минус):\n"
                         "<i>Пример: <code>12.5</code> или <code>-3</code></i>")


@router.message(Form.waiting_for_trade_profit)
async def process_trade_profit(message: Message, state: FSMContext):
    vals = parse_floats(msg_text(message), 1, 1)
    if not vals or abs(vals[0]) > 1e9:
        await message.answer("Укажите профит числом (например: 12.5).")
        return
    prof = vals[0]
    data = await state.get_data()
    await save_trade(message.from_user.id, data["pair_info"], data["amount_usd"], prof)
    roi = prof / data["amount_usd"] * 100
    await state.clear()
    kb, _ = await render_main(message.from_user.id)
    await message.answer(
        "<b>✅ Сделка сохранена</b>\n\n"
        f"• Пара: {html.escape(data['pair_info'])}\n• Сумма: ${data['amount_usd']:,.2f}\n"
        f"• Результат: {money(prof)} ({roi:+.2f}%)", reply_markup=kb)


CALC_PROMPT = (
    "<b>🧮 Арбитражный калькулятор</b>\n\n"
    "Комиссии: 0.1% покупка + 0.1% продажа. Отправьте через пробел:\n"
    "<code>[Депозит] [Цена покупки] [Цена продажи] [Комиссия сети в $ — необязательно]</code>\n\n"
    "<i>Пример: <code>1000 142.5 144.1</code> или <code>1000 142.5 144.1 1.5</code></i>"
)


@router.callback_query(F.data == "trade_calc")
async def cb_calc_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_calc_input)
    await respond(call, CALC_PROMPT, kb_of([back_row("menu_trades", "Назад")]))


@router.message(Form.waiting_for_calc_input)
async def process_calc_input(message: Message, state: FSMContext):
    vals = parse_floats(msg_text(message), 3, 4)
    if not vals or any(v <= 0 for v in vals[:3]) or (len(vals) == 4 and vals[3] < 0):
        await message.answer("Неверный формат. Пример: <code>1000 142.5 144.1</code> "
                             "(3 положительных числа + необязательная комиссия сети).")
        return
    capital, buy_p, sell_p = vals[:3]
    net_fee = vals[3] if len(vals) == 4 else 0.0
    bought = capital * 0.999 / buy_p
    received = bought * sell_p * 0.999
    net_profit = received - capital - net_fee
    roi = net_profit / capital * 100
    gross = (sell_p / buy_p - 1) * 100
    be_sell = (capital + net_fee) / (bought * 0.999)
    be_spread = (be_sell / buy_p - 1) * 100
    await state.clear()
    text = (
        "<b>Результат расчёта</b>\n\n"
        f"• Депозит: ${capital:,.2f} USDT\n• Грязный спред: {gross:+.2f}%\n"
        f"• Комиссии бирж: −0.2%" + (f" · сеть: −${net_fee:.2f}" if net_fee else "") + "\n"
        f"• Безубыточный спред: {be_spread:.2f}%\n\n"
        f"• Чистый профит: <b>{money(net_profit)}</b>\n• Чистый ROI: <b>{roi:+.2f}%</b>"
    )
    await message.answer(text, reply_markup=kb_of([
        [btn("🧮 Рассчитать ещё", "trade_calc")], [btn("📊 К дневнику", "menu_trades")]]))


@router.callback_query(F.data == "trade_history")
async def cb_trade_history(call: CallbackQuery):
    trades = await get_recent_trades(call.from_user.id, limit=15)
    if not trades:
        text = "История сделок пуста."
    else:
        text = "<b>📜 Последние сделки:</b>\n\n"
        for p_info, amt, prof, roi, _ in trades:
            text += f"• <b>{html.escape(str(p_info))}</b>\n  ${amt:,.2f} → {money(prof)} ({roi:+.2f}%)\n\n"
    await respond(call, text, kb_of([back_row("menu_trades")]))


# --- Фильтры ---
def settings_view(u: dict) -> Tuple[str, InlineKeyboardMarkup]:
    text = (
        "<b>⚙️ Настройка фильтров</b>\n\n"
        f"• Мин. чистый спред: <b>{u['min_spread']:g}%</b>\n"
        f"• Мин. объём 24ч: <b>{fmt_vol(u['min_volume'])}</b>\n"
        f"• Биржи: <b>{len(u['exchanges'])}/{len(EXCHANGE_NAMES)}</b>\n"
        f"• Мем-коины: <b>{'включены' if u['allow_memes'] else 'скрыты'}</b>\n"
        f"• Личные уведомления: <b>{'вкл' if u['dm_alerts'] else 'выкл'}</b>\n\n"
        "Фильтры действуют и на список сканера, и на push-уведомления."
    )
    rows = [
        [btn(("✅ " if abs(u["min_spread"] - sp) < 1e-9 else "") + f"{sp:g}%", f"set_spread_{sp}")
         for sp in SPREAD_OPTIONS],
        [btn(("✅ " if abs(u["min_volume"] - v) < 1 else "") + fmt_vol(v), f"set_vol_{v}") for v in VOLUME_OPTIONS],
        [btn(f"🔔 Push: {'вкл' if u['dm_alerts'] else 'выкл'}", "toggle_dm"),
         btn(f"🐸 Мемы: {'да' if u['allow_memes'] else 'нет'}", "toggle_meme")],
    ]
    row: List[InlineKeyboardButton] = []
    for ex in EXCHANGE_NAMES:
        mark = "✅" if ex in u["exchanges"] else "▫️"
        row.append(btn(f"{mark} {ex_title(ex)}", f"toggle_ex_{ex}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([btn("Все биржи", "ex_all")])
    rows.append(back_row())
    return text, kb_of(rows)


async def render_settings(call: CallbackQuery):
    u = await get_user_data(call.from_user.id)
    text, kb = settings_view(u)
    await respond(call, text, kb)


@router.callback_query(F.data == "menu_settings")
async def cb_settings(call: CallbackQuery):
    if not await is_user_subscribed(call.from_user.id):
        await call.answer("Настройки доступны только с PRO-подпиской.", show_alert=True)
        return
    await render_settings(call)


@router.callback_query(F.data.startswith("set_spread_"))
async def cb_set_spread(call: CallbackQuery):
    try:
        val = float(call.data.replace("set_spread_", ""))
    except ValueError:
        return await call.answer()
    if val in SPREAD_OPTIONS:
        await update_user_field(call.from_user.id, "min_spread", val)
        invalidate_alert_cache()
    await render_settings(call)


@router.callback_query(F.data.startswith("set_vol_"))
async def cb_set_vol(call: CallbackQuery):
    try:
        val = float(call.data.replace("set_vol_", ""))
    except ValueError:
        return await call.answer()
    if int(val) in VOLUME_OPTIONS:
        await update_user_field(call.from_user.id, "min_volume", val)
        invalidate_alert_cache()
    await render_settings(call)


@router.callback_query(F.data.in_({"toggle_dm", "toggle_meme"}))
async def cb_toggle_flags(call: CallbackQuery):
    u = await get_user_data(call.from_user.id)
    if call.data == "toggle_dm":
        await update_user_field(call.from_user.id, "dm_alerts", 0 if u["dm_alerts"] else 1)
    else:
        await update_user_field(call.from_user.id, "allow_memes", 0 if u["allow_memes"] else 1)
    invalidate_alert_cache()
    await render_settings(call)


@router.callback_query(F.data == "dm_off")
async def cb_dm_off(call: CallbackQuery):
    await update_user_field(call.from_user.id, "dm_alerts", 0)
    invalidate_alert_cache()
    await call.answer("Личные уведомления отключены. Включить: Фильтры → Push.", show_alert=True)


@router.callback_query(F.data.startswith("toggle_ex_"))
async def cb_toggle_ex(call: CallbackQuery):
    ex = call.data.replace("toggle_ex_", "")
    if ex not in EXCHANGES:
        return await call.answer()
    u = await get_user_data(call.from_user.id)
    exs = u["exchanges"]
    if ex in exs:
        if len(exs) <= 2:
            return await call.answer("Нужно оставить минимум 2 биржи.", show_alert=True)
        exs.remove(ex)
    else:
        exs.append(ex)
    await set_user_exchanges(call.from_user.id, exs)
    invalidate_alert_cache()
    await render_settings(call)


@router.callback_query(F.data == "ex_all")
async def cb_ex_all(call: CallbackQuery):
    await set_user_exchanges(call.from_user.id, list(EXCHANGE_NAMES))
    invalidate_alert_cache()
    await render_settings(call)


# --- Сканер ---
@router.callback_query(F.data == "view_fast_signals")
@router.callback_query(F.data.startswith("signals:"))
async def cb_view_signals(call: CallbackQuery):
    uid = call.from_user.id
    if not await is_user_subscribed(uid):
        await call.answer("Раздел доступен только с PRO-подпиской.", show_alert=True)
        return
    u = await get_user_data(uid)
    if u["balance"] <= 0:
        await call.answer("Ваш баланс равен $0. Задайте его в разделе «Профиль».", show_alert=True)
        return
    page = 0
    if call.data.startswith("signals:"):
        try:
            page = max(0, int(call.data.split(":")[1]))
        except ValueError:
            page = 0

    amount = effective_amount(u)
    filtered = [s for s in LATEST_SIGNALS if signal_matches_user(s, u)]
    pages = max(1, -(-len(filtered) // PAGE_SIZE))
    page = min(page, pages - 1)
    chunk = filtered[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    if not filtered:
        text = (
            "<b>🔎 Поиск связок…</b>\n\n"
            f"Сейчас нет связок под ваши фильтры (баланс <b>${u['balance']:.2f}</b>).\n"
            f"• Мин. спред: <code>{u['min_spread']:g}%</code> · объём ≥ {fmt_vol(u['min_volume'])}\n"
            f"• Рынок сканируется каждые {ANALYZE_INTERVAL:.0f} сек, онлайн {len(MARKET.online())} бирж.\n\n"
            "Включены личные уведомления — бот сам напишет, когда связка появится."
            if u["dm_alerts"] else
            "<b>🔎 Поиск связок…</b>\n\nСейчас нет связок под ваши фильтры."
        )
        kb = kb_of([[btn("🔄 Обновить", "signals:0")], [btn("⚙️ Фильтры", "menu_settings")], back_row()])
    else:
        text = f"<b>📡 Связки под ваш депозит (${amount:,.2f})</b>\n<i>Чистый спред после комиссий каждой биржи</i>\n\n"
        for i, sig in enumerate(chunk, start=page * PAGE_SIZE + 1):
            text += render_signal_block(sig, amount, i) + f"⏱ {sig.ts} UTC · скан ×{sig.streak}\n\n"
        text += f"<i>Найдено: {len(filtered)} · стр. {page + 1}/{pages}</i>"
        rows = [[btn(f"📊 {s.base}", f"d|{s.base}|{s.buy_ex}|{s.sell_ex}") for s in chunk]]
        nav = []
        if page > 0:
            nav.append(btn("◀️", f"signals:{page - 1}"))
        nav.append(btn("🔄", f"signals:{page}"))
        if page < pages - 1:
            nav.append(btn("▶️", f"signals:{page + 1}"))
        rows.append(nav)
        rows.append([btn("⚙️ Фильтры", "menu_settings"), btn("👤 Профиль", "menu_profile")])
        rows.append(back_row())
        kb = kb_of(rows)
    await respond(call, text, kb)


@router.callback_query(F.data.startswith("d|"))
async def cb_depth(call: CallbackQuery):
    uid = call.from_user.id
    if not await is_user_subscribed(uid):
        await call.answer("Проверка стакана доступна только с PRO-подпиской.", show_alert=True)
        return
    try:
        _, base, buy_ex, sell_ex = call.data.split("|")
    except ValueError:
        return await call.answer()
    if buy_ex not in EXCH_OBJ or sell_ex not in EXCH_OBJ or not base.isalnum():
        return await call.answer("Связка недоступна.", show_alert=True)
    await call.answer("⏳ Загружаю стакан…")
    u = await get_user_data(uid)
    text = await render_depth(base, buy_ex, sell_ex, u)
    kb = kb_of([[btn("🔄 Обновить", call.data)], [btn("📡 К сигналам", "signals:0")], back_row()])
    await respond(call, text, kb)


@router.callback_query(F.data == "menu_guide")
async def cb_guide(call: CallbackQuery):
    text = (
        "<b>📖 Как работать со сканером</b>\n\n"
        "1. Выберите связку в сканере или получите её в личные уведомления.\n"
        "2. Нажмите «📊 Стакан» — бот покажет реальный профит на вашу сумму с учётом проскальзывания.\n"
        "3. Проверьте на биржах, открыты ли ввод/вывод монеты и какая сеть дешевле.\n"
        "4. Купите актив на дешёвой бирже, переведите, продайте на дорогой.\n\n"
        "<b>Важно:</b>\n"
        "• Комиссии в расчёте — базовые тейкерские тарифы бирж; у вас они могут отличаться.\n"
        "• Комиссия вывода монеты не учитывается — добавляйте её в калькуляторе.\n"
        "• Спред выше 8% почти всегда фантом (закрыт вывод или разные токены с одним тикером).\n"
        "• Цены меняются за секунды: чем больше сумма, тем сильнее проскальзывание.\n"
        "• Upbit торгуется в основном в KRW — в сканере участвуют только его USDT-рынки.\n\n"
        "<i>Это не инвестиционная рекомендация.</i>"
    )
    await respond(call, text, kb_of([back_row()]))


# --- Покупка подписки ---
@router.callback_query(F.data == "menu_buy")
async def cb_buy(call: CallbackQuery):
    text = (
        "<b>💎 Оформление PRO</b>\n\n"
        "• Персональные push-уведомления о связках под ваши фильтры\n"
        "• Проверка по стакану с реальным профитом\n"
        f"• Сканер по {len(EXCHANGE_NAMES)} биржам и гибкие фильтры\n"
        "• Доступ к VIP-каналу, дневнику и калькулятору\n\n"
        "Выберите тариф:"
    )
    kb = kb_of([
        [btn(f"{PRICES['week']['name']} — ${PRICES['week']['usd']:.2f}", "select_plan_week")],
        [btn(f"{PRICES['month']['name']} — ${PRICES['month']['usd']:.2f}", "select_plan_month")],
        back_row(),
    ])
    await respond(call, text, kb)


@router.callback_query(F.data.startswith("select_plan_"))
async def cb_select_plan(call: CallbackQuery):
    plan_key = call.data.replace("select_plan_", "")
    plan = PRICES.get(plan_key)
    if not plan:
        return await call.answer("Тариф не найден.", show_alert=True)

    if not CRYPTO_PAY_TOKEN:
        return await call.answer(
            f"Приём оплаты временно недоступен. Напишите в поддержку: @{SUPPORT_USERNAME}", show_alert=True)
    invoice = await crypto_pay.create_invoice(plan["usd"], f"{call.from_user.id}:{plan_key}", f"{plan['name']} — Arbitrage Terminal")
    if not invoice:
        return await call.answer("Ошибка при создании счёта. Попробуйте позже.", show_alert=True)

    await db_mgr.conn.execute(
        "INSERT INTO invoices (invoice_id, provider, user_id, amount, plan, created_at) "
        "VALUES (?, 'cryptobot', ?, ?, ?, ?) ON CONFLICT(invoice_id) DO NOTHING",
        (invoice["invoice_id"], call.from_user.id, plan["usd"], plan_key, now_ts()),
    )
    await db_mgr.conn.commit()
    text = (
        "<b>Оплата счёта</b>\n\n"
        f"Тариф: <b>{plan['name']}</b>\nК оплате: <b>{plan['usd']} USDT</b>\n"
        "Счёт действует 1 час. После оплаты подписка активируется автоматически."
    )
    kb = kb_of([
        [InlineKeyboardButton(text=f"💳 Оплатить (${plan['usd']})", url=invoice["pay_url"])],
        [btn("🔄 Я оплатил — проверить", f"check_pay_{invoice['invoice_id']}")],
        back_row("menu_buy", "К тарифам"),
    ])
    await respond(call, text, kb)


@router.callback_query(F.data.startswith("check_pay_"))
async def cb_check_pay(call: CallbackQuery):
    inv_id = call.data.replace("check_pay_", "")
    async with db_mgr.conn.execute(
        "SELECT id, user_id, plan, status FROM invoices WHERE invoice_id = ?", (inv_id,)
    ) as cur:
        row = await cur.fetchone()
    if not row or row[1] != call.from_user.id:
        return await call.answer("Счёт не найден.", show_alert=True)
    row_id, uid, plan, status = row
    if status == "paid":
        return await call.answer("Этот счёт уже оплачен и подписка активирована.", show_alert=True)
    remote = (await crypto_pay.get_invoices([inv_id])).get(inv_id)
    st = remote.get("status") if remote else None
    if st == "paid":
        await apply_paid_invoice(bot, row_id, uid, plan)
        await call.answer("Оплата получена!")
    elif st == "expired":
        await call.answer("Счёт истёк. Создайте новый.", show_alert=True)
    else:
        await call.answer("Оплата ещё не поступила. Подождите минуту и повторите.", show_alert=True)


@router.message(F.text)
async def fallback_text(message: Message, state: FSMContext):
    """Любой непонятный текст — показываем меню вместо молчания."""
    if await state.get_state():
        return
    kb, text = await render_main(message.from_user.id)
    await message.answer("Не понял команду. Вот главное меню (список команд: /help).\n\n" + text, reply_markup=kb)


@router.errors()
async def on_error(event: ErrorEvent):
    exc = event.exception
    if isinstance(exc, TelegramBadRequest) and ("not modified" in str(exc).lower() or "query is too old" in str(exc).lower()):
        return True
    logging.error(f"Ошибка в хендлере: {type(exc).__name__}: {exc}", exc_info=exc)
    upd = event.update
    try:
        if upd.callback_query:
            await upd.callback_query.answer("⚠️ Что-то пошло не так. Попробуйте ещё раз.", show_alert=True)
        elif upd.message:
            await upd.message.answer("⚠️ Что-то пошло не так. Попробуйте ещё раз или нажмите /start")
    except Exception:
        pass
    return True


# =============================================================================
#                              АДМИН-ПАНЕЛЬ
# =============================================================================
@admin_router.callback_query(F.data == "menu_admin")
async def cb_admin_panel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    db = db_mgr.conn
    now = now_ts()
    try:
        async def one(sql, params=()):
            async with db.execute(sql, params) as c:
                r = await c.fetchone()
                return r[0] if r and r[0] is not None else 0
        total = await one("SELECT COUNT(*) FROM users")
        active = await one("SELECT COUNT(*) FROM users WHERE sub_expiry > ?", (now,))
        new24 = await one("SELECT COUNT(*) FROM users WHERE created_at > ?", (now - 86400,))
        dm_users = await one("SELECT COUNT(*) FROM users WHERE dm_alerts = 1 AND sub_expiry > ?", (now,))
        refs = await one("SELECT COUNT(*) FROM users WHERE referred_by IS NOT NULL")
        revenue = await one("SELECT COALESCE(SUM(amount), 0.0) FROM invoices WHERE status = 'paid'")
        rev30 = await one("SELECT COALESCE(SUM(amount), 0.0) FROM invoices WHERE status = 'paid' AND created_at > ?",
                          (now - 30 * 86400,))
    except Exception as e:
        logging.error(f"Admin SQL error: {e}")
        total = active = new24 = dm_users = refs = 0
        revenue = rev30 = 0.0

    text = (
        "<b>👑 Панель администратора</b>\n\n"
        f"• Пользователей: <b>{total}</b> (+{new24} за 24ч)\n"
        f"• Активных PRO: <b>{active}</b> · с push: {dm_users}\n"
        f"• По рефералкам пришло: {refs}\n"
        f"• Доход: <b>${revenue:.2f}</b> (30 дн.: ${rev30:.2f})\n"
        f"• Бирж онлайн: {len(MARKET.online())}/{len(EXCHANGE_NAMES)} · связок: {len(LATEST_SIGNALS)}\n\n"
        "Команды: <code>/grant ID дни</code> · <code>/revoke ID</code> · <code>/user ID</code> · "
        "<code>/newpromo КОД дни [лимит]</code>"
    )
    kb = kb_of([
        [btn("➕ Выдать PRO", "admin_grant"), btn("➖ Снять PRO", "admin_revoke")],
        [btn("📢 Рассылка", "admin_broadcast"), btn("🏦 Статус бирж", "menu_status")],
        [btn("📥 Экспорт базы (CSV)", "admin_export")],
        back_row("menu_main", "Главное меню"),
    ])
    await respond(call, text, kb)


@admin_router.callback_query(F.data == "admin_grant")
async def cb_admin_grant_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_grant_id)
    await respond(call, "Укажите Telegram ID пользователя:", kb_of([back_row("menu_admin", "Отмена")]))


@admin_router.message(Form.waiting_for_grant_id)
async def process_grant_id(message: Message, state: FSMContext):
    t = msg_text(message)
    if not t.lstrip("-").isdigit():
        return await message.answer("Введите корректный числовой Telegram ID.")
    await state.update_data(target_id=int(t))
    await state.set_state(Form.waiting_for_grant_days)
    await message.answer("Укажите срок подписки в днях:")


@admin_router.message(Form.waiting_for_grant_days)
async def process_grant_days(message: Message, state: FSMContext):
    t = msg_text(message)
    if not t.isdigit() or not (0 < int(t) <= 3650):
        return await message.answer("Укажите количество дней числом (1–3650).")
    days = int(t)
    target_id = (await state.get_data())["target_id"]
    exp = await add_subscription(target_id, days)
    await state.clear()
    invalidate_alert_cache()
    await message.answer(f"PRO для ID <code>{target_id}</code> выдана на {days} дн. До: {fmt_dt(exp)}",
                         reply_markup=kb_of([back_row("menu_admin", "В админку")]))
    await safe_send(bot, target_id, f"🎁 Вам активирована PRO-подписка на {days} дн.\nДо: <code>{fmt_dt(exp)}</code>")


@admin_router.callback_query(F.data == "admin_revoke")
async def cb_admin_revoke_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_revoke_id)
    await respond(call, "Укажите Telegram ID для снятия подписки:", kb_of([back_row("menu_admin", "Отмена")]))


@admin_router.message(Form.waiting_for_revoke_id)
async def process_revoke_id(message: Message, state: FSMContext):
    t = msg_text(message)
    if not t.lstrip("-").isdigit():
        return await message.answer("Введите корректный числовой Telegram ID.")
    await revoke_subscription(int(t))
    await state.clear()
    invalidate_alert_cache()
    await message.answer(f"Подписка ID <code>{t}</code> аннулирована.",
                         reply_markup=kb_of([back_row("menu_admin", "В админку")]))


# --- Рассылка с предпросмотром ---
@admin_router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_broadcast)
    await respond(call, "Введите текст рассылки (поддерживаются HTML-теги):", kb_of([back_row("menu_admin", "Отмена")]))


@admin_router.message(Form.waiting_for_broadcast)
async def process_broadcast_text(message: Message, state: FSMContext):
    txt = msg_text(message)
    if not txt:
        return await message.answer("Отправьте текст рассылки.")
    body = f"<b>Объявление:</b>\n\n{txt}"
    try:
        await message.answer("👁 <b>Предпросмотр:</b>\n\n" + body)
    except TelegramBadRequest as e:
        return await message.answer(f"Ошибка в HTML-разметке: <code>{html.escape(str(e))}</code>\nИсправьте и отправьте снова.")
    now = now_ts()
    async with db_mgr.conn.execute("SELECT COUNT(*), COALESCE(SUM(CASE WHEN sub_expiry > ? THEN 1 ELSE 0 END), 0) FROM users", (now,)) as c:
        total, pro = await c.fetchone()
    await state.update_data(bc_text=body)
    await state.set_state(Form.confirm_broadcast)
    await message.answer("Кому отправить?", reply_markup=kb_of([
        [btn(f"👥 Всем ({total})", "bc_all"), btn(f"💎 Только PRO ({pro})", "bc_pro")],
        [btn("Отмена", "menu_admin")],
    ]))


async def run_broadcast(admin_id: int, body: str, only_pro: bool):
    now = now_ts()
    async with db_mgr.conn.execute("SELECT user_id, sub_expiry FROM users") as cur:
        rows = await cur.fetchall()
    targets = [u for u, exp in rows if not only_pro or parse_expiry(exp) > now or u in ADMIN_IDS]
    ok = blocked = err = 0
    for uid in targets:
        res = await safe_send(bot, uid, body)
        if res == "ok":
            ok += 1
        elif res == "blocked":
            blocked += 1
        else:
            err += 1
        await asyncio.sleep(0.05)
    await safe_send(bot, admin_id,
                    f"📢 Рассылка завершена.\nДоставлено: {ok}\nЗаблокировали бота: {blocked}\nОшибок: {err}",
                    kb_of([back_row("menu_admin", "В админку")]))


@admin_router.callback_query(Form.confirm_broadcast, F.data.in_({"bc_all", "bc_pro"}))
async def cb_broadcast_confirm(call: CallbackQuery, state: FSMContext):
    body = (await state.get_data()).get("bc_text")
    await state.clear()
    if not body:
        return await call.answer("Текст не найден, начните заново.", show_alert=True)
    task = asyncio.create_task(run_broadcast(call.from_user.id, body, only_pro=(call.data == "bc_pro")))
    BG_TASKS.add(task)
    task.add_done_callback(BG_TASKS.discard)
    await respond(call, "🚀 Рассылка запущена в фоне. По завершении пришлю отчёт.",
                  kb_of([back_row("menu_admin", "В админку")]))


@admin_router.callback_query(F.data == "admin_export")
async def cb_admin_export(call: CallbackQuery):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["User ID", "Username", "Expiry (UTC)", "Status", "Balance", "Min spread", "Push", "Referred by", "Joined (UTC)"])
    now = now_ts()
    async with db_mgr.conn.execute(
        "SELECT user_id, username, sub_expiry, balance, min_spread, dm_alerts, referred_by, created_at FROM users"
    ) as cur:
        rows = await cur.fetchall()
    for u_id, uname, exp, bal, ms, dm, ref, created in rows:
        p = parse_expiry(exp)
        writer.writerow([
            u_id, uname or "N/A", fmt_dt(p) if p > 0 else "N/A", "Active" if p > now else "Expired",
            bal or 0.0, ms, dm, ref or "", fmt_dt(created) if created else "",
        ])
    await call.message.answer_document(
        BufferedInputFile(output.getvalue().encode("utf-8-sig"), filename="users_export.csv"),
        caption="Экспорт пользователей.")
    await call.answer()


@admin_router.message(Command("grant"))
async def cmd_grant(message: Message, command: CommandObject):
    try:
        target, days = (command.args or "").split()
        exp = await add_subscription(int(target