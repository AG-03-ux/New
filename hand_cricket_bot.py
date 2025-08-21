# We'll write the improved Telegram hand-cricket bot to a file the user can download.
code = r'''
print("Starting Hand Cricket Pro Bot...")

import os
import random
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
import telebot
from telebot import types
from flask import Flask

# ------------- CONFIG & SETUP -------------
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set in environment variables (.env)")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s [%(levelname)s] %(message)s")
telebot.logger.setLevel(logging.DEBUG if LOG_LEVEL == "DEBUG" else logging.INFO)

DB_PATH = os.getenv("DB_PATH", "hand_cricket.sqlite3")
PORT = int(os.getenv("PORT", "5000"))
OVERSIZE = int(os.getenv("OVERSIZE", "6"))  # balls per over

bot = telebot.TeleBot(TOKEN, parse_mode="HTML")
app = Flask(__name__)

# ------------- PERSISTENCE (SQLite) -------------

def db_connect():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def db_init():
    with db_connect() as con:
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            total_runs INTEGER DEFAULT 0,
            matches INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            last_played_at INTEGER DEFAULT 0
        );
        """)
        con.commit()

def db_upsert_user(user_id: int, username: str):
    with db_connect() as con:
        cur = con.cursor()
        cur.execute("""
            INSERT INTO users (user_id, username, last_played_at)
            VALUES (?, ?, strftime('%s','now'))
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                last_played_at=strftime('%s','now');
        """, (user_id, username or ""))
        con.commit()

def db_add_match(user_id: int, runs: int, win: bool):
    with db_connect() as con:
        cur = con.cursor()
        cur.execute("""
            UPDATE users
            SET total_runs = total_runs + ?,
                matches = matches + 1,
                wins = wins + ?,
                last_played_at = strftime('%s','now')
            WHERE user_id = ?;
        """, (runs, 1 if win else 0, user_id))
        con.commit()

def db_user_stats(user_id: int) -> Optional[Tuple[int,int,int]]:
    with db_connect() as con:
        cur = con.cursor()
        cur.execute("SELECT total_runs, matches, wins FROM users WHERE user_id=?;", (user_id,))
        row = cur.fetchone()
        return row if row else None

def db_leaderboard(limit: int = 10):
    with db_connect() as con:
        cur = con.cursor()
        cur.execute("""
            SELECT username, total_runs, matches, wins
            FROM users
            ORDER BY wins DESC, total_runs DESC
            LIMIT ?;
        """, (limit,))
        return cur.fetchall()

db_init()

# ------------- GAME STATE -------------

@dataclass
class Game:
    chat_id: int
    user_id: int
    username: str
    stage: str = "menu"  # menu|toss|choose|first_innings|second_innings|finished
    innings: int = 0
    user_batting_first: Optional[bool] = None
    user_score: int = 0
    bot_score: int = 0
    target: Optional[int] = None
    balls_bowled: int = 0  # in current innings
    over_size: int = OVERSIZE
    difficulty: str = "easy"  # easy|medium|hard
    last_event: str = "Welcome! Press <b>Play</b> to begin."
    recent_user: List[int] = field(default_factory=list)
    recent_bot: List[int] = field(default_factory=list)

    def reset(self):
        self.stage = "toss"
        self.innings = 1
        self.user_batting_first = None
        self.user_score = 0
        self.bot_score = 0
        self.target = None
        self.balls_bowled = 0
        self.last_event = "New match! Call the toss: Heads or Tails."
        self.recent_user.clear()
        self.recent_bot.clear()

    # ---------- Bot AI ----------
    def bot_move(self, context: str, user_last: Optional[int]) -> int:
        # context: "bot_batting" or "bot_bowling"
        # Base pool
        nums = [1,2,3,4,5,6]

        if self.difficulty == "easy":
            return random.choice(nums)

        # medium: bias away from repeating last bot move and away from user_last (if bowling, try not to match; if batting, try not to match)
        if self.difficulty == "medium":
            weights = [1]*6
            if self.recent_bot:
                last_bot = self.recent_bot[-1]
                weights[last_bot-1] = 0.5  # discourage repeat
            if user_last:
                weights[user_last-1] *= 0.6  # avoid matching
            # normalize choice
            choices = []
            for n,w in zip(nums,weights):
                choices += [n]*int(w*10)
            return random.choice(choices or nums)

        # hard: predictive — if bowling, try to <b>match</b> user's likely pick (to get them out)
        # if batting, try to avoid user's likely pick (to avoid getting out), and lean toward higher runs
        if self.difficulty == "hard":
            # Predict user's next based on last 2-3
            prediction = None
            if len(self.recent_user) >= 2:
                # simple frequency on last 3
                window = self.recent_user[-3:]
                prediction = max(set(window), key=window.count)
            elif user_last:
                prediction = user_last

            if context == "bot_bowling":
                # try to match predicted user pick to take wicket
                if prediction:
                    return prediction
                # otherwise choose a random number, a bit skewed to mid values to increase match chance
                return random.choices(nums, weights=[1,2,3,3,2,1])[0]
            else:
                # bot batting: avoid predicted match; prefer higher scoring
                avoid = prediction
                weights = [1,1,2,3,4,4]  # bias to 3-6
                if avoid:
                    weights[avoid-1] = 0.1  # avoid matching
                return random.choices(nums, weights=weights)[0]

        return random.choice(nums)

# All games in memory by chat_id
games: Dict[int, Game] = {}

# ------------- UTIL -------------

def safe_send(chat_id: int, text: str, reply_markup: Optional[types.InlineKeyboardMarkup] = None):
    try:
        return bot.send_message(chat_id, text, reply_markup=reply_markup, disable_web_page_preview=True)
    except Exception as e:
        logging.error(f"Failed to send message to {chat_id}: {e}")

def menu_kb():
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("▶️ Play", callback_data="MENU_PLAY"),
           types.InlineKeyboardButton("ℹ️ Rules", callback_data="MENU_RULES"))
    kb.row(types.InlineKeyboardButton("📊 My Stats", callback_data="MENU_STATS"),
           types.InlineKeyboardButton("🏆 Leaderboard", callback_data="MENU_TOP"))
    kb.row(types.InlineKeyboardButton("🎚️ Difficulty", callback_data="MENU_DIFF"))
    return kb

def diff_kb(current: str):
    kb = types.InlineKeyboardMarkup()
    for d in ["easy","medium","hard"]:
        label = f"{'✅ ' if d==current else ''}{d.title()}"
        kb.add(types.InlineKeyboardButton(label, callback_data=f"DIFF_{d}"))
    kb.add(types.InlineKeyboardButton("⬅️ Back", callback_data="BACK_MENU"))
    return kb

def toss_kb():
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("Heads", callback_data="TOSS_heads"),
           types.InlineKeyboardButton("Tails", callback_data="TOSS_tails"))
    kb.add(types.InlineKeyboardButton("⬅️ Menu", callback_data="BACK_MENU"))
    return kb

def choice_kb():
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("🏏 Bat First", callback_data="CHOICE_bat"),
           types.InlineKeyboardButton("🎯 Bowl First", callback_data="CHOICE_bowl"))
    return kb

def bat_kb():
    kb = types.InlineKeyboardMarkup()
    row = []
    for n in range(1,7):
        row.append(types.InlineKeyboardButton(str(n), callback_data=f"BAT_{n}"))
        if len(row)==3:
            kb.row(*row); row=[]
    if row: kb.row(*row)
    kb.add(types.InlineKeyboardButton("📟 Score", callback_data="SHOW_SCORE"))
    return kb

def bowl_kb():
    kb = types.InlineKeyboardMarkup()
    row = []
    for n in range(1,7):
        row.append(types.InlineKeyboardButton(str(n), callback_data=f"BOWL_{n}"))
        if len(row)==3:
            kb.row(*row); row=[]
    if row: kb.row(*row)
    kb.add(types.InlineKeyboardButton("📟 Score", callback_data="SHOW_SCORE"))
    return kb

def format_score(g: Game) -> str:
    target = g.target if g.target is not None else "—"
    balls = g.balls_bowled
    overs = f"{balls//g.over_size}.{balls%g.over_size}"
    status = ""
    if g.stage in ("first_innings","second_innings"):
        status = f"\n<b>Innings</b>: {g.innings}  •  <b>Overs</b>: {overs}/{g.over_size*2//g.over_size}"
    return (f"<b>You</b>: {g.user_score}   <b>Bot</b>: {g.bot_score}\n"
            f"<b>Target</b>: {target}{status}\n"
            f"{g.last_event}")

def rules_text() -> str:
    return (
        "🏏 <b>Hand Cricket — Rules</b>\n"
        "• Toss → Winner chooses to Bat or Bowl.\n"
        "• During batting: pick 1–6. If bot picks the same number → Out.\n"
        "• During bowling: pick 1–6. If numbers match → Bot is Out.\n"
        "• Two innings. Target = First innings score + 1.\n"
        f"• {OVERSIZE} balls per over (soft limit shown; match ends on Out/Target or after you decide to end innings).\n"
        "• Difficulty changes how smart the bot is.\n"
    )

# ------------- COMMANDS -------------

@bot.message_handler(commands=["start","help"])
def cmd_start(message):
    user = message.from_user
    db_upsert_user(user.id, user.username or user.first_name or "")
    text = ("Welcome to <b>Hand Cricket Pro</b>! 🏏\n\n"
            "Tap <b>Play</b> to start a new match, browse <b>Rules</b>, check your <b>Stats</b>, "
            "or view the <b>Leaderboard</b>.")
    safe_send(message.chat.id, text, reply_markup=menu_kb())

@bot.message_handler(commands=["play"])
def cmd_play(message):
    start_new_game(message.chat.id, message.from_user)

@bot.message_handler(commands=["score"])
def cmd_score(message):
    g = games.get(message.chat.id)
    if not g:
        safe_send(message.chat.id, "No active game. Tap <b>Play</b> to start.", reply_markup=menu_kb())
        return
    safe_send(message.chat.id, format_score(g),
              reply_markup=bat_kb() if is_user_batting(g) else bowl_kb())

@bot.message_handler(commands=["reset"])
def cmd_reset(message):
    g = games.get(message.chat.id)
    if not g:
        start_new_game(message.chat.id, message.from_user)
        return
    g.reset()
    safe_send(message.chat.id, "Game reset.\nCall the toss!", reply_markup=toss_kb())

@bot.message_handler(commands=["leaderboard"])
def cmd_leaderboard(message):
    send_leaderboard(message.chat.id)

@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    send_stats(message.chat.id, message.from_user.id)

# ------------- CALLBACKS -------------

def start_new_game(chat_id: int, user):
    g = Game(chat_id=chat_id, user_id=user.id, username=user.username or user.first_name or "")
    g.reset()
    games[chat_id] = g
    db_upsert_user(user.id, g.username)
    safe_send(chat_id, "🪙 <b>Toss Time!</b> Choose Heads or Tails.", reply_markup=toss_kb())

def is_user_batting(g: Game) -> bool:
    return (g.user_batting_first and g.innings == 1) or ((not g.user_batting_first) and g.innings == 2)

def end_innings(g: Game, message_suffix: str):
    # Set target or finish match
    if g.innings == 1:
        # set target for next innings
        g.target = (g.user_score if is_user_batting(g) else g.bot_score) + 1
        g.innings = 2
        g.balls_bowled = 0
        if g.user_batting_first:
            g.last_event = f"{message_suffix} <b>Target for Bot</b>: {g.target}. You Bowl now."
        else:
            g.last_event = f"{message_suffix} <b>Target for You</b>: {g.target}. You Bat now."
        g.stage = "second_innings"
    else:
        # match end
        g.stage = "finished"
        if g.user_score > g.bot_score:
            g.last_event = f"{message_suffix} ✅ <b>You win!</b>"
            db_add_match(g.user_id, g.user_score, True)
        elif g.user_score < g.bot_score:
            g.last_event = f"{message_suffix} ❌ <b>Bot wins.</b>"
            db_add_match(g.user_id, g.user_score, False)
        else:
            g.last_event = f"{message_suffix} 🤝 <b>It's a tie!</b>"
            db_add_match(g.user_id, g.user_score, False)

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call: types.CallbackQuery):
    chat_id = call.message.chat.id
    user = call.from_user
    data = call.data

    # ensure user record exists
    db_upsert_user(user.id, user.username or user.first_name or "")

    g = games.get(chat_id)

    # MENU & NAV
    if data == "MENU_PLAY":
        start_new_game(chat_id, user)
        return
    if data == "MENU_RULES":
        safe_send(chat_id, rules_text(), reply_markup=menu_kb())
        return
    if data == "MENU_STATS":
        send_stats(chat_id, user.id)
        return
    if data == "MENU_TOP":
        send_leaderboard(chat_id)
        return
    if data == "MENU_DIFF":
        if not g:
            g = Game(chat_id=chat_id, user_id=user.id, username=user.username or user.first_name or "")
            games[chat_id] = g
        safe_send(chat_id, f"Current difficulty: <b>{g.difficulty.title()}</b>.\nChoose:", reply_markup=diff_kb(g.difficulty))
        return
    if data == "BACK_MENU":
        safe_send(chat_id, "Back to menu.", reply_markup=menu_kb())
        return

    # DIFFICULTY
    if data.startswith("DIFF_"):
        level = data.split("_",1)[1]
        if g:
            g.difficulty = level
        safe_send(chat_id, f"Difficulty set to <b>{level.title()}</b>.", reply_markup=menu_kb())
        return

    # TOSS
    if data.startswith("TOSS_"):
        choice = data.split("_",1)[1]
        coin = random.choice(["heads","tails"])
        if not g:
            start_new_game(chat_id, user)
            g = games[chat_id]
        g.stage = "toss"
        g.last_event = f"Toss: You chose <b>{choice}</b>, coin is <b>{coin}</b>."
        if choice == coin:
            g.last_event += "\n🎉 You won the toss. Choose to <b>Bat</b> or <b>Bowl</b>."
            g.stage = "choose"
            bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                                  text=g.last_event, reply_markup=choice_kb(), parse_mode="HTML")
        else:
            # bot decides: in hard mode it prefers bowling first
            bot_choice = "bowl" if g.difficulty == "hard" else random.choice(["bat","bowl"])
            g.user_batting_first = (bot_choice == "bowl")
            g.innings = 1
            g.stage = "first_innings"
            g.balls_bowled = 0
            if g.user_batting_first:
                g.last_event += f"\nBot won the toss and chose to <b>Bowl</b> first. You bat now. Pick 1–6."
                bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                                      text=format_score(g), reply_markup=bat_kb(), parse_mode="HTML")
            else:
                g.last_event += f"\nBot won the toss and chose to <b>Bat</b> first. You bowl now. Pick 1–6."
                bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                                      text=format_score(g), reply_markup=bowl_kb(), parse_mode="HTML")
        return

    # CHOICE AFTER WON TOSS
    if data.startswith("CHOICE_"):
        if not g: return
        pick = data.split("_",1)[1]
        g.user_batting_first = (pick == "bat")
        g.stage = "first_innings"
        g.innings = 1
        g.balls_bowled = 0
        g.last_event = "Innings 1 starts! Pick your number." if g.user_batting_first else "Innings 1 starts! Bowl your number."
        bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                              text=format_score(g),
                              reply_markup=bat_kb() if is_user_batting(g) else bowl_kb(), parse_mode="HTML")
        return

    if data == "SHOW_SCORE":
        if not g:
            safe_send(chat_id, "No active game.", reply_markup=menu_kb())
            return
        bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                              text=format_score(g),
                              reply_markup=bat_kb() if is_user_batting(g) else bowl_kb(), parse_mode="HTML")
        return

    # CORE GAMEPLAY
    if data.startswith("BAT_") or data.startswith("BOWL_"):
        if not g or g.stage not in ("first_innings","second_innings"):
            safe_send(chat_id, "No active innings. Start a new match.", reply_markup=menu_kb())
            return

        user_is_batting = is_user_batting(g)
        try:
            user_pick = int(data.split("_",1)[1])
        except:
            user_pick = 1

        if data.startswith("BAT_") and not user_is_batting:
            safe_send(chat_id, "You're bowling right now!", reply_markup=bowl_kb()); return
        if data.startswith("BOWL_") and user_is_batting:
            safe_send(chat_id, "You're batting right now!", reply_markup=bat_kb()); return

        # Bot move based on context
        user_last = g.recent_user[-1] if g.recent_user else None
        context = "bot_bowling" if user_is_batting else "bot_batting"
        bot_pick = g.bot_move(context=context, user_last=user_last)

        # record recency
        g.recent_user.append(user_pick)
        g.recent_user = g.recent_user[-10:]
        g.recent_bot.append(bot_pick)
        g.recent_bot = g.recent_bot[-10:]

        # apply outcome
        out = (user_pick == bot_pick)
        g.balls_bowled += 1

        if user_is_batting:
            if out:
                g.last_event = f"🏏 You: <b>{user_pick}</b>  •  Bot: <b>{bot_pick}</b>  — <b>OUT!</b>"
                end_innings(g, g.last_event)
            else:
                g.user_score += user_pick
                g.last_event = f"🏏 You: <b>{user_pick}</b>  •  Bot: <b>{bot_pick}</b>  — Runs <b>+{user_pick}</b>"
                if g.stage == "second_innings" and g.user_score >= (g.target or 0):
                    g.stage = "finished"
                    g.last_event += "  ✅ <b>You reached the target!</b>"
                    db_add_match(g.user_id, g.user_score, True)
        else:
            if out:
                g.last_event = f"🎯 You: <b>{user_pick}</b>  •  Bot: <b>{bot_pick}</b>  — <b>BOT OUT!</b>"
                end_innings(g, g.last_event)
            else:
                g.bot_score += bot_pick
                g.last_event = f"🎯 You: <b>{user_pick}</b>  •  Bot: <b>{bot_pick}</b>  — Bot scores <b>+{bot_pick}</b>"
                if g.stage == "second_innings" and g.bot_score >= (g.target or 0):
                    g.stage = "finished"
                    g.last_event += "  ❌ <b>Bot reached the target.</b>"
                    db_add_match(g.user_id, g.user_score, False)

        # Update UI
        kb = None
        if g.stage == "finished":
            kb = menu_kb()
        else:
            kb = bat_kb() if is_user_batting(g) else bowl_kb()

        try:
            bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id,
                                  text=format_score(g), reply_markup=kb, parse_mode="HTML")
        except Exception:
            # message might be too old to edit; send a new one
            safe_send(chat_id, format_score(g), reply_markup=kb)
        return

def send_stats(chat_id: int, user_id: int):
    row = db_user_stats(user_id)
    if not row:
        safe_send(chat_id, "No stats yet. Play your first match!", reply_markup=menu_kb()); return
    total_runs, matches, wins = row
    winrate = f"{(wins/matches*100):.1f}%" if matches else "0%"
    text = (f"📊 <b>Your Stats</b>\n"
            f"Matches: <b>{matches}</b>\n"
            f"Wins: <b>{wins}</b>\n"
            f"Total Runs: <b>{total_runs}</b>\n"
            f"Win Rate: <b>{winrate}</b>")
    safe_send(chat_id, text, reply_markup=menu_kb())

def send_leaderboard(chat_id: int):
    rows = db_leaderboard(10)
    if not rows:
        safe_send(chat_id, "No games played yet. Be the first on the board!", reply_markup=menu_kb()); return
    lines = ["🏆 <b>Leaderboard</b> (Wins, then Runs)"]
    for i,(username,total_runs,matches,wins) in enumerate(rows, start=1):
        name = username or "Anonymous"
        lines.append(f"{i}. {name}: <b>{wins}</b> wins · <b>{total_runs}</b> runs · {matches} matches")
    safe_send(chat_id, "\n".join(lines), reply_markup=menu_kb())

# ------------- HEALTH CHECK ROUTE -------------

@app.route('/')
def home():
    return "Hand Cricket Pro Bot is running.", 200

# ------------- MAIN -------------

def run_bot():
    logging.info("Starting bot polling...")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)

def run_flask():
    logging.info(f"Starting Flask on port {PORT}...")
    app.run(host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    t = threading.Thread(target=run_bot, daemon=True)
    t.start()
    run_flask()
'''
path = "hand_cricket_data.json"
with open(path, "w", encoding="utf-8") as f:
    f.write(code)

print(f"Saved improved bot to {path}")

