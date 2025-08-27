import os
import logging
import random
import sqlite3
from datetime import datetime
from contextlib import contextmanager
from typing import Optional, Dict, Any

from flask import Flask, request
import telebot
from telebot import types

from dotenv import load_dotenv
load_dotenv()

# ======================================================
# Environment / Config
# ======================================================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # required
USE_WEBHOOK = int(os.getenv("USE_WEBHOOK", "0"))  # 1 for webhook, 0 for polling
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # e.g., https://yourdomain.onrender.com
PORT = int(os.getenv("PORT", 5000))  # Render sets this automatically
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DB_PATH = os.getenv("DB_PATH", "cricket_bot.db")
DEFAULT_OVERS = int(os.getenv("DEFAULT_OVERS", "2"))
DEFAULT_WICKETS = int(os.getenv("DEFAULT_WICKETS", "1"))  # classic hand-cricket: 1 wicket
MAX_OVERS = 20
MAX_WICKETS = 10

# ======================================================
# Logging
# ======================================================
logging.basicConfig(level=LOG_LEVEL, format='[%(levelname)s] %(message)s')
telebot.logger.setLevel(LOG_LEVEL)
logger = logging.getLogger("cricket-bot")

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

# ======================================================
# Initialize Bot
# ======================================================
bot = telebot.TeleBot(TOKEN, parse_mode="HTML", threaded=True)

# ======================================================
# GIF Helpers (local only)
# ======================================================
ASSETS = {
    "six": "assets/six.gif",
    "wicket": "assets/wicket.gif",
    "win": "assets/win.gif",
    "lose": "assets/lose.gif",
    "tie": "assets/tie.gif",
}

# Optional remote GIF URLs (override local assets if provided)
SIX_GIF_URL = os.getenv("SIX_GIF_URL")
WICKET_GIF_URL = os.getenv("WICKET_GIF_URL")
WIN_GIF_URL = os.getenv("WIN_GIF_URL")
LOSE_GIF_URL = os.getenv("LOSE_GIF_URL")
TIE_GIF_URL = os.getenv("TIE_GIF_URL")
GIF_URLS = {
    "six": SIX_GIF_URL,
    "wicket": WICKET_GIF_URL,
    "win": WIN_GIF_URL,
    "lose": LOSE_GIF_URL,
    "tie": TIE_GIF_URL,
}

def _send_animation(chat_id, key: str):
    # Prefer remote URL if provided; otherwise fallback to local asset file
    url = GIF_URLS.get(key)
    if url:
        try:
            bot.send_animation(chat_id, url)
            return
        except Exception as e:
            logger.warning(f"Failed to send remote GIF '{key}' from {url}: {e}")

    path = ASSETS.get(key)
    if not path:
        return
    try:
        with open(path, "rb") as f:
            bot.send_animation(chat_id, f)
    except Exception as e:
        logger.warning(f"Failed to send local GIF {path}: {e}")

# ======================================================
# Persistence (SQLite)
# ======================================================

@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def db_init():
    with db_conn() as db:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                created_at TEXT
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS stats (
                user_id INTEGER PRIMARY KEY,
                games_played INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                ties INTEGER DEFAULT 0,
                high_score INTEGER DEFAULT 0,
                best_chase INTEGER DEFAULT 0,
                fastest_50_balls INTEGER DEFAULT NULL
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS games (
                chat_id INTEGER PRIMARY KEY,
                state TEXT,
                innings INTEGER,
                batting TEXT,
                player_score INTEGER,
                bot_score INTEGER,
                player_wkts INTEGER,
                bot_wkts INTEGER,
                balls_in_over INTEGER,
                overs_bowled INTEGER,
                target INTEGER,
                overs_limit INTEGER,
                wickets_limit INTEGER,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                event TEXT,
                meta TEXT,
                created_at TEXT
            )
            """
        )

def upsert_user(u: types.User):
    with db_conn() as db:
        db.execute(
            """
            INSERT INTO users (user_id, username, first_name, last_name, created_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name
            """,
            (u.id, u.username, u.first_name, u.last_name, datetime.utcnow().isoformat()),
        )
        # Ensure a stats row exists for this user (SQLite-compatible upsert)
        db.execute("INSERT OR IGNORE INTO stats (user_id) VALUES (?)", (u.id,))


def log_event(chat_id: int, event: str, meta: str = ""):
    with db_conn() as db:
        db.execute(
            "INSERT INTO history (chat_id, event, meta, created_at) VALUES (?, ?, ?, ?)",
            (chat_id, event, meta, datetime.utcnow().isoformat()),
        )


# ======================================================
# Game Utilities
# ======================================================

def default_game(overs: int = DEFAULT_OVERS, wickets: int = DEFAULT_WICKETS) -> Dict[str, Any]:
    overs = max(1, min(overs, MAX_OVERS))
    wickets = max(1, min(wickets, MAX_WICKETS))
    return dict(
        state="toss",              # toss, play, finished
        innings=1,                 # 1 or 2
        batting=None,              # "player" or "bot"
        player_score=0,
        bot_score=0,
        player_wkts=0,
        bot_wkts=0,
        balls_in_over=0,           # 0..5
        overs_bowled=0,            # completed overs in current innings
        target=None,
        overs_limit=overs,
        wickets_limit=wickets,
        created_at=datetime.utcnow().isoformat(),
        updated_at=datetime.utcnow().isoformat(),
    )


def save_game(chat_id: int, g: Dict[str, Any]):
    with db_conn() as db:
        db.execute(
            """
            INSERT INTO games (
                chat_id, state, innings, batting, player_score, bot_score,
                player_wkts, bot_wkts, balls_in_over, overs_bowled, target,
                overs_limit, wickets_limit, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                state=excluded.state,
                innings=excluded.innings,
                batting=excluded.batting,
                player_score=excluded.player_score,
                bot_score=excluded.bot_score,
                player_wkts=excluded.player_wkts,
                bot_wkts=excluded.bot_wkts,
                balls_in_over=excluded.balls_in_over,
                overs_bowled=excluded.overs_bowled,
                target=excluded.target,
                overs_limit=excluded.overs_limit,
                wickets_limit=excluded.wickets_limit,
                updated_at=excluded.updated_at
            """,
            (
                chat_id,
                g["state"], g["innings"], g["batting"],
                g["player_score"], g["bot_score"],
                g["player_wkts"], g["bot_wkts"],
                g["balls_in_over"], g["overs_bowled"], g["target"],
                g["overs_limit"], g["wickets_limit"],
                g["created_at"], datetime.utcnow().isoformat(),
            ),
        )


def load_game(chat_id: int) -> Optional[Dict[str, Any]]:
    with db_conn() as db:
        cur = db.execute("SELECT * FROM games WHERE chat_id=?", (chat_id,))
        row = cur.fetchone()
        if not row:
            return None
        return dict(row)


def delete_game(chat_id: int):
    with db_conn() as db:
        db.execute("DELETE FROM games WHERE chat_id=?", (chat_id,))


# ======================================================
# Keyboards
# ======================================================

def kb_toss_choice() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🪙 Heads", callback_data="toss_heads"),
           types.InlineKeyboardButton("🪙 Tails", callback_data="toss_tails"))
    return kb


def kb_bat_numbers() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=3, one_time_keyboard=False)
    for n in [1, 2, 3, 4, 5, 6]:
        kb.insert(types.KeyboardButton(str(n)))
    return kb


def kb_bat_bowl_choice() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🏏 Bat", callback_data="choose_bat"),
           types.InlineKeyboardButton("🎯 Bowl", callback_data="choose_bowl"))
    return kb


# ======================================================
# Game Mechanics
# ======================================================

def start_new_game(chat_id: int, overs: int = DEFAULT_OVERS, wickets: int = DEFAULT_WICKETS):
    g = default_game(overs, wickets)
    save_game(chat_id, g)
    bot.send_message(chat_id, (
        "🏏 <b>New Match!</b>\n"
        f"Format: <b>{g['overs_limit']} over(s), {g['wickets_limit']} wicket(s)</b>\n\n"
        "Tap to toss a coin and decide who starts:"
    ), reply_markup=kb_toss_choice())
    log_event(chat_id, "game_start", f"overs={g['overs_limit']} wickets={g['wickets_limit']}")
    logger.debug(f"New game saved for chat {chat_id}: {g}")


def set_batting(chat_id: int, who: str):
    g = load_game(chat_id)
    if not g:
        return
    g["state"] = "play"
    g["batting"] = who  # "player" or "bot"
    save_game(chat_id, g)

    if who == "player":
        bot.send_message(chat_id, "You bat first! Send a number 1–6 for each ball.", reply_markup=kb_bat_numbers())
    else:
        bot.send_message(chat_id, "Bot bats first! Send a number 1–6 to bowl each ball.", reply_markup=kb_bat_numbers())


def check_over_progress(g: Dict[str, Any]) -> bool:
    # Returns True if over completed and increments overs
    if g["balls_in_over"] >= 6:
        g["balls_in_over"] = 0
        g["overs_bowled"] += 1
        return True
    return False


def is_innings_over(g: Dict[str, Any]) -> bool:
    # innings ends if wickets reached or overs exhausted
    if g["batting"] == "player":
        if g["player_wkts"] >= g["wickets_limit"]:
            return True
    else:
        if g["bot_wkts"] >= g["wickets_limit"]:
            return True
    if g["overs_bowled"] >= g["overs_limit"]:
        return True
    return False


def progress_ball(chat_id: int, user_value: int):
    g = load_game(chat_id)
    if not g or g["state"] != "play":
        return

    # Guard input
    if user_value < 1 or user_value > 6:
        bot.send_message(chat_id, "Please send a number between 1 and 6.")
        return

    bot_value = random.randint(1, 6)
    g["balls_in_over"] += 1

    if g["batting"] == "player":
        if user_value == bot_value:
            g["player_wkts"] += 1
            msg = f"❌ <b>OUT!</b> You: {user_value} | Bot: {bot_value}\n"
            _send_animation(chat_id, "wicket")
        else:
            g["player_score"] += user_value
            msg = f"🏏 You scored <b>{user_value}</b> | Bot bowled {bot_value}."
            if user_value == 6:
                _send_animation(chat_id, "six")
        over_done = check_over_progress(g)
        if over_done:
            msg += f"\n⏱️ Over completed. Overs: {g['overs_bowled']}/{g['overs_limit']}"

        # Chase condition in 2nd innings
        if g["innings"] == 2 and g["target"] is not None and g["player_score"] > g["target"]:
            save_game(chat_id, g)
            bot.send_message(chat_id, msg)
            end_match(chat_id)
            return

    else:  # bot batting, user bowls
        if user_value == bot_value:
            g["bot_wkts"] += 1
            msg = f"❌ <b>BOT OUT!</b> You: {user_value} | Bot: {bot_value}\n"
            _send_animation(chat_id, "wicket")
        else:
            g["bot_score"] += bot_value
            msg = f"🤖 Bot scored <b>{bot_value}</b> | You bowled {user_value}."
            if bot_value == 6:
                _send_animation(chat_id, "six")
        over_done = check_over_progress(g)
        if over_done:
            msg += f"\n⏱️ Over completed. Overs: {g['overs_bowled']}/{g['overs_limit']}"

        if g["innings"] == 2 and g["target"] is not None and g["bot_score"] > g["target"]:
            save_game(chat_id, g)
            bot.send_message(chat_id, msg)
            end_match(chat_id)
            return

    # Check innings end
    if is_innings_over(g):
        save_game(chat_id, g)
        bot.send_message(chat_id, msg)
        end_innings(chat_id)
        return

    save_game(chat_id, g)
    bot.send_message(chat_id, f"{msg}\n\n<code>Score</code>\nYou: {g['player_score']}/{g['player_wkts']}  |  Bot: {g['bot_score']}/{g['bot_wkts']}\nOver: {g['overs_bowled']}.{g['balls_in_over']} / {g['overs_limit']}")


def end_innings(chat_id: int):
    g = load_game(chat_id)
    if not g:
        return

    if g["innings"] == 1:
        # switch innings
        if g["batting"] == "player":
            g["target"] = g["player_score"]
            g["batting"] = "bot"
            msg = (
                f"🔁 <b>Innings Break</b>\n"
                f"You: <b>{g['player_score']}/{g['player_wkts']}</b> in {g['overs_bowled']} ov\n"
                f"🎯 Target for bot: <b>{g['player_score'] + 1}</b>\n\n"
                "Second innings: Bot bats. Send 1–6 to bowl."
            )
        else:
            g["target"] = g["bot_score"]
            g["batting"] = "player"
            msg = (
                f"🔁 <b>Innings Break</b>\n"
                f"Bot: <b>{g['bot_score']}/{g['bot_wkts']}</b> in {g['overs_bowled']} ov\n"
                f"🎯 Target for you: <b>{g['bot_score'] + 1}</b>\n\n"
                "Second innings: You bat. Send 1–6 to bat."
            )
        # reset over counters for new innings
        g["innings"] = 2
        g["balls_in_over"] = 0
        g["overs_bowled"] = 0
        save_game(chat_id, g)
        bot.send_message(chat_id, msg, reply_markup=kb_bat_numbers())
    else:
        # innings 2 ended
        end_match(chat_id)


def _finalize_match_message(g: Dict[str, Any]) -> str:
    p, b = g["player_score"], g["bot_score"]
    if p > b:
        result = "🎉 <b>You WIN!</b>"
        outcome = "win"
    elif b > p:
        result = "🤖 <b>Bot WINS!</b>"
        outcome = "loss"
    else:
        result = "😮 <b>It's a TIE!</b>"
        outcome = "tie"

    summary = (
        f"🏁 <b>Match Over</b>\n"
        f"You: <b>{p}/{g['player_wkts']}</b>\n"
        f"Bot: <b>{b}/{g['bot_wkts']}</b>\n"
        f"Result: {result}\n\n"
        "Use /play to start a new game or /leaderboard to see rankings."
    )
    return summary, outcome


def end_match(chat_id: int):
    g = load_game(chat_id)
    if not g:
        return

    p, b = g["player_score"], g["bot_score"]

    # Animations
    if p > b:
        _send_animation(chat_id, "win")
    elif b > p:
        _send_animation(chat_id, "lose")
    else:
        _send_animation(chat_id, "tie")

    summary, outcome = _finalize_match_message(g)
    bot.send_message(chat_id, summary)
    log_event(chat_id, "match_over", f"p={p} b={b} outcome={outcome}")
    delete_game(chat_id)


# ======================================================
# Stats & Leaderboards
# ======================================================

def add_result_for_user(user_id: int, runs: int, won: Optional[bool], tied: bool = False):
    with db_conn() as db:
        cur = db.execute("SELECT * FROM stats WHERE user_id=?", (user_id,))
        if not cur.fetchone():
            db.execute("INSERT INTO stats (user_id) VALUES (?)", (user_id,))
        db.execute("UPDATE stats SET games_played = games_played + 1 WHERE user_id=?", (user_id,))
        if tied:
            db.execute("UPDATE stats SET ties = ties + 1 WHERE user_id=?", (user_id,))
        else:
            if won is True:
                db.execute("UPDATE stats SET wins = wins + 1 WHERE user_id=?", (user_id,))
            elif won is False:
                db.execute("UPDATE stats SET losses = losses + 1 WHERE user_id=?", (user_id,))
        db.execute("UPDATE stats SET high_score = MAX(high_score, ?) WHERE user_id=?", (runs, user_id))


def get_stats_text(user_id: int) -> str:
    with db_conn() as db:
        cur = db.execute("SELECT * FROM stats WHERE user_id=?", (user_id,))
        s = cur.fetchone()
        if not s:
            return "No stats yet. Play a match with /play!"
        return (
            f"📈 <b>Your Stats</b>\n"
            f"Games: {s['games_played']}\n"
            f"Wins: {s['wins']} | Losses: {s['losses']} | Ties: {s['ties']}\n"
            f"High Score: {s['high_score']}\n"
        )


def get_leaderboard(limit: int = 10) -> str:
    with db_conn() as db:
        cur = db.execute(
            """
            SELECT u.first_name, u.username, s.wins, s.games_played, s.high_score
            FROM stats s JOIN users u ON u.user_id = s.user_id
            ORDER BY s.wins DESC, s.high_score DESC, s.games_played DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        if not rows:
            return "No players yet. Be the first! Use /play"
        lines = ["🏆 <b>Leaderboard</b>"]
        for i, r in enumerate(rows, 1):
            display = r["first_name"] or (("@" + r["username"]) if r["username"] else "Unknown")
            lines.append(f"{i}. {display} — {r['wins']} win(s), HS {r['high_score']}")
        return "\n".join(lines)


# ======================================================
# Middleware: credit last interacting user when match ends
# ======================================================

def credit_last_user(chat_id: int, p_runs: int, b_runs: int):
    with db_conn() as db:
        cur = db.execute(
            "SELECT meta FROM history WHERE chat_id=? AND event='ball_input' ORDER BY id DESC LIMIT 1",
            (chat_id,),
        )
        row = cur.fetchone()
        if not row:
            return
        meta = row["meta"] or ""
        try:
            # meta like: "from=123456 n=4"
            parts = dict(kv.split("=") for kv in meta.split())
            uid = int(parts.get("from", "0"))
        except Exception:
            return
        won = (p_runs > b_runs)
        tied = (p_runs == b_runs)
        add_result_for_user(uid, p_runs, None if tied else won, tied)


# Keep a reference to the original end_match
_original_end_match = end_match

def end_match_hook(chat_id: int):
    g = load_game(chat_id)
    if not g:
        return
    p, b = g["player_score"], g["bot_score"]
    credit_last_user(chat_id, p, b)
    # call original end_match to show summary + cleanup
    _original_end_match(chat_id)

# Replace end_match with the hook
end_match = end_match_hook


# ======================================================
# Command Handlers
# ======================================================

def ensure_user(message: types.Message):
    if message.from_user:
        upsert_user(message.from_user)

@bot.message_handler(commands=["start", "help"])  # start doubles as help
def cmd_help(message: types.Message):
    ensure_user(message)
    text = (
        "👋 <b>Welcome to Cricket Bot!</b>\n\n"
        "Play hand-cricket against the bot. Rules:\n"
        "• Send 1–6 each ball. If you match the opponent's number, it's OUT.\n"
        "• First innings: whoever bats tries to score as much as possible within overs/wickets.\n"
        "• Second innings: chase the target.\n\n"
        "Commands:\n"
        "• /play — start a new match\n"
        f"• /format &lt;overs 1..{MAX_OVERS}&gt; &lt;wickets 1..{MAX_WICKETS}&gt; — set default format\n"
        "• /score — show live score\n"
        "• /stats — your career stats\n"
        "• /leaderboard — global top players\n"
        "• /forfeit — concede current match\n"
        "• /about — about this bot\n\n"
        "Tip: Use the on-screen keypad (1–6) for quick play."
    )
    bot.reply_to(message, text, reply_markup=kb_bat_numbers())

@bot.message_handler(commands=["about"])
def cmd_about(message: types.Message):
    ensure_user(message)
    bot.reply_to(message, "🏏 <b>Cricket Bot</b> — fast, fun, and open-source friendly.\nMade with ❤️ using Python, Flask and PyTelegramBotAPI.")

@bot.message_handler(commands=["play"])
def cmd_play(message: types.Message):
    ensure_user(message)
    parts = (message.text or "").split()
    overs, wkts = DEFAULT_OVERS, DEFAULT_WICKETS
    if len(parts) >= 2:
        try:
            overs = int(parts[1])
        except Exception:
            pass
    if len(parts) >= 3:
        try:
            wkts = int(parts[2])
        except Exception:
            pass
    start_new_game(message.chat.id, overs, wkts)

@bot.message_handler(commands=["format"])  # set default format for this chat's next /play
def cmd_format(message: types.Message):
    ensure_user(message)
    parts = (message.text or "").split()
    if len(parts) < 3:
        bot.reply_to(message, f"Usage: /format &lt;overs 1..{MAX_OVERS}&gt; &lt;wickets 1..{MAX_WICKETS}&gt;")
        return
    try:
        overs = max(1, min(int(parts[1]), MAX_OVERS))
        wkts = max(1, min(int(parts[2]), MAX_WICKETS))
        global DEFAULT_OVERS, DEFAULT_WICKETS
        DEFAULT_OVERS, DEFAULT_WICKETS = overs, wkts
        bot.reply_to(message, f"✅ Default format set to {overs} over(s), {wkts} wicket(s). Use /play to start.")
    except ValueError:
        bot.reply_to(message, "Please provide integers for overs and wickets, e.g. /format 2 1")

@bot.message_handler(commands=["score"])
def cmd_score(message: types.Message):
    ensure_user(message)
    g = load_game(message.chat.id)
    if not g or g["state"] == "finished":
        bot.reply_to(message, "No active match. Use /play to start.")
        return
    bot.reply_to(message,
        f"📊 <b>Live Score</b>\n"
        f"You: {g['player_score']}/{g['player_wkts']} | Bot: {g['bot_score']}/{g['bot_wkts']}\n"
        f"Innings: {g['innings']} | Batting: {g['batting']}\n"
        f"Over: {g['overs_bowled']}.{g['balls_in_over']} / {g['overs_limit']}\n"
        + (f"Target: {g['target'] + 1}\n" if g['target'] is not None else "")
    )

@bot.message_handler(commands=["stats"])
def cmd_stats(message: types.Message):
    ensure_user(message)
    bot.reply_to(message, get_stats_text(message.from_user.id))

@bot.message_handler(commands=["leaderboard"])
def cmd_leaderboard(message: types.Message):
    ensure_user(message)
    bot.reply_to(message, get_leaderboard())

@bot.message_handler(commands=["forfeit"])
def cmd_forfeit(message: types.Message):
    ensure_user(message)
    g = load_game(message.chat.id)
    if not g:
        bot.reply_to(message, "No active match to forfeit.")
        return
    delete_game(message.chat.id)
    bot.reply_to(message, "🏳️ You forfeited the match. Use /play to start again.")


# ======================================================
# Callback Query Handlers (Inline Buttons)
# ======================================================

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("toss_"))
def cq_toss(call: types.CallbackQuery):
    g = load_game(call.message.chat.id)
    if not g:
        bot.answer_callback_query(call.id, "Start a match first with /play")
        return
    if g["state"] != "toss":
        bot.answer_callback_query(call.id, "Toss already decided")
        return

    user_choice = call.data.split("_", 1)[1]  # heads/tails
    coin = random.choice(["heads", "tails"])
    won = (user_choice == coin)

    text = f"🪙 Coin: <b>{coin}</b> | You chose: <b>{user_choice}</b>\n"
    if won:
        text += "You won the toss! Choose to bat or bowl first."
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb_bat_bowl_choice(), parse_mode="HTML")
    else:
        text += "You lost the toss. Bot chooses to bat first."
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="HTML")
        set_batting(call.message.chat.id, "bot")

    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data in ("choose_bat", "choose_bowl"))
def cq_choose(call: types.CallbackQuery):
    g = load_game(call.message.chat.id)
    if not g or (g["state"] not in ("play", "toss")):
        bot.answer_callback_query(call.id)
        return
    who = "player" if call.data == "choose_bat" else "bot"
    set_batting(call.message.chat.id, who)
    bot.answer_callback_query(call.id)


# ======================================================
# Text Message Handler for Bat/Bowl Numbers
# ======================================================

@bot.message_handler(content_types=["text"])
def on_text(message: types.Message):
    ensure_user(message)
    text = (message.text or "").strip()

    # quick number input 1..6 drives the game
    if text.isdigit():
        n = int(text)
        if 1 <= n <= 6:
            # Remember who to credit stats to when the match finishes
            # We'll store the user id in history
            log_event(message.chat.id, "ball_input", f"from={message.from_user.id} n={n}")
            progress_ball(message.chat.id, n)
            return

    # ignore other texts to avoid noise


# ======================================================
# Flask Webhook
# ======================================================
app = Flask(__name__)

# Ensure webhook is set when running under WSGI servers (e.g., gunicorn on Render)
if USE_WEBHOOK:
    if not WEBHOOK_URL:
        logger.error("USE_WEBHOOK=1 but WEBHOOK_URL is not set")
    else:
        try:
            try:
                bot.remove_webhook()
            except Exception:
                pass
            full_url = WEBHOOK_URL.rstrip("/") + "/webhook"
            ok = bot.set_webhook(url=full_url)
            logger.info(f"Webhook set (WSGI init): {full_url} -> {ok}")
        except Exception as e:
            logger.exception(f"Failed to set webhook during WSGI init: {e}")

# Existing webhook route
@app.route("/webhook", methods=["POST"])
def webhook():
    # Minimal logging to confirm reception (avoid logging secrets)
    data = request.stream.read().decode("utf-8")
    logger.info(f"Received Telegram update: {data[:200]}...")  # log first 200 chars
    update = telebot.types.Update.de_json(data)
    try:
        bot.process_new_updates([update])
    except Exception as e:
        logger.exception(f"Error while processing update: {e}")
    return "OK", 200

# Root (stops Render 404 loops)
@app.route("/", methods=["GET"])
def index():
    return "Hand Cricket Bot is running!", 200

# Health check for Render
@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


def run_flask():
    app.run(host="0.0.0.0", port=PORT)


def run_polling():
    # Ensure no webhook is active when using polling
    try:
        bot.remove_webhook()
        logger.info("Removed existing webhook (if any) for polling mode.")
    except Exception as e:
        logger.warning(f"Could not remove webhook before polling: {e}")
    # Skip old pending updates and use reasonable timeouts for stability
    bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)


# Note: Avoid catch-all handlers that intercept all messages, so the
# game logic handlers above can work as intended.

# ======================================================
# Boot
# ======================================================
if __name__ == "__main__":
    db_init()
    if USE_WEBHOOK:
        if not WEBHOOK_URL:
            raise ValueError("USE_WEBHOOK=1 but WEBHOOK_URL is not set")
        try:
            bot.remove_webhook()
        except Exception:
            pass
        full_url = WEBHOOK_URL.rstrip("/") + "/webhook"
        ok = bot.set_webhook(url=full_url)
        logger.info(f"Webhook set: {full_url} -> {ok}")
        run_flask()
    else:
        logger.info("Starting polling mode…")
        run_polling()
