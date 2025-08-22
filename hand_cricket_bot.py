# hand_cricket_bot.py
# Production-ready Hand Cricket Bot with GIFs and webhook/polling support

import os
import random
import logging
import threading
from dotenv import load_dotenv
from flask import Flask, request
import telebot
from telebot import types

# =========================
# Config & Setup
# =========================
load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN (or BOT_TOKEN) is not set")

USE_WEBHOOK = os.getenv("USE_WEBHOOK", "0") == "1"  # 1 = use webhook/Render, 0 = polling
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")          # e.g., https://your-domain.onrender.com
PORT = int(os.getenv("PORT", "5000"))

# Optional GIFs
SIX_GIF_URL = os.getenv("SIX_GIF_URL", "")
WICKET_GIF_URL = os.getenv("WICKET_GIF_URL", "")
WIN_GIF_URL = os.getenv("WIN_GIF_URL", "")
LOSE_GIF_URL = os.getenv("LOSE_GIF_URL", "")
TIE_GIF_URL = os.getenv("TIE_GIF_URL", "")

# Logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=log_level)
telebot.logger.setLevel(log_level)

bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

# =========================
# Game State
# =========================
games = {}

def new_game(chat_id):
    games[chat_id] = {
        "stage": "toss",
        "user_batting_first": None,
        "innings": 0,
        "user_score": 0,
        "bot_score": 0,
        "target": None,
        "last_event": "New game! Use /toss heads or /toss tails.",
    }
    return games[chat_id]

def reset_game(chat_id):
    if chat_id in games:
        del games[chat_id]
    return new_game(chat_id)

def toss_result(user_choice=None):
    coin = random.choice(["heads", "tails"])
    if user_choice is None:
        user_choice = random.choice(["heads", "tails"])
    return coin, user_choice.lower(), (user_choice.lower() == coin)

def bot_move():
    return random.randint(1, 6)

def check_out(user_pick, bot_pick):
    return user_pick == bot_pick

def format_score(g):
    if g["stage"] == "toss":
        return f"Toss stage. {g['last_event']}"
    if g["stage"] in ("first_innings", "second_innings"):
        return (f"Innings {g['innings']} — You: {g['user_score']}  Bot: {g['bot_score']}\n"
                f"Target: {g['target'] if g['target'] else 'N/A'}\nLast: {g['last_event']}")
    if g["stage"] == "finished":
        return (f"Match finished!\nYou: {g['user_score']}  Bot: {g['bot_score']}\n{g['last_event']}")
    return "No active game. Use /play to start."

# =========================
# GIF helpers
# =========================
def _send_animation_by_url_or_asset(chat_id, url: str, asset_path: str):
    if url:
        try:
            bot.send_animation(chat_id, url)
            return
        except Exception as e:
            logging.warning(f"GIF URL send failed for {url}: {e}")
    try:
        with open(asset_path, "rb") as f:
            bot.send_animation(chat_id, f)
    except Exception as e:
        logging.info(f"No local GIF at {asset_path} (or send failed): {e}")

def gif_six(chat_id): _send_animation_by_url_or_asset(chat_id, SIX_GIF_URL, "assets/six.gif")
def gif_wicket(chat_id): _send_animation_by_url_or_asset(chat_id, WICKET_GIF_URL, "assets/wicket.gif")
def gif_win(chat_id): _send_animation_by_url_or_asset(chat_id, WIN_GIF_URL, "assets/win.gif")
def gif_lose(chat_id): _send_animation_by_url_or_asset(chat_id, LOSE_GIF_URL, "assets/lose.gif")
def gif_tie(chat_id): _send_animation_by_url_or_asset(chat_id, TIE_GIF_URL, "assets/tie.gif")

def safe_reply(message, text):
    try:
        bot.reply_to(message, text)
    except Exception as e:
        logging.error(f"Failed to send message: {e}")

# =========================
# Handlers
# =========================
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    safe_reply(
        message,
        "🏏 <b>Welcome to Hand Cricket Bot!</b>\n\n"
        "<b>Commands</b>\n"
        "/play - start a new game\n"
        "/toss heads|tails - call the toss\n"
        "/bat N - bat with number 1–6\n"
        "/bowl N - bowl with number 1–6\n"
        "/score - show the score\n"
        "/reset - reset the game\n\n"
        "Tip: Add GIFs in <code>assets/</code> or set GIF URLs via env vars!"
    )

@bot.message_handler(commands=['play'])
def cmd_play(message):
    chat_id = message.chat.id
    g = new_game(chat_id)
    safe_reply(message, "New game created. " + g["last_event"])

@bot.message_handler(commands=['toss'])
def cmd_toss(message):
    try:
        chat_id = message.chat.id
        args = message.text.split()
        if chat_id not in games:
            new_game(chat_id)
        g = games[chat_id]
        user_choice = args[1].lower() if len(args) >= 2 else None
        if user_choice and user_choice not in ("heads", "tails"):
            safe_reply(message, "Invalid choice. Use /toss heads or /toss tails.")
            return
        coin, choice, win = toss_result(user_choice)
        g["last_event"] = f"Coin: <b>{coin}</b>, You: <i>{choice}</i>."
        g["stage"] = "first_innings"
        g["innings"] = 1
        if win:
            g["user_batting_first"] = True
            g["last_event"] += " You won and bat first! Use <code>/bat N</code>."
        else:
            g["user_batting_first"] = False
            g["last_event"] += " Bot bats first! Use <code>/bowl N</code>."
        safe_reply(message, g["last_event"])
    except Exception as e:
        logging.error(f"Error in /toss: {e}")

@bot.message_handler(commands=['bat'])
def cmd_bat(message):
    try:
        chat_id = message.chat.id
        args = message.text.split()
        if chat_id not in games or games[chat_id]["stage"] not in ("first_innings", "second_innings"):
            safe_reply(message, "No active batting. Start a game first with /play.")
            return
        g = games[chat_id]
        user_batting = (g["user_batting_first"] and g["innings"] == 1) or (not g["user_batting_first"] and g["innings"] == 2)
        if not user_batting:
            safe_reply(message, "You are bowling now!")
            return
        try:
            pick = int(args[1])
        except:
            safe_reply(message, "Pick a number 1–6. Example: /bat 4")
            return
        if not 1 <= pick <= 6:
            safe_reply(message, "Pick must be 1–6.")
            return
        bot_pick = bot_move()
        if pick == 6 and not check_out(pick, bot_pick): gif_six(chat_id)
        if check_out(pick, bot_pick):
            g["last_event"] = f"You: {pick}, Bot: {bot_pick} — <b>OUT!</b>"
            gif_wicket(chat_id)
            if g["innings"] == 1:
                g["target"] = g["user_score"] + 1
                g["innings"] = 2
                g["stage"] = "second_innings"
                g["last_event"] += f" Target for bot: <b>{g['target']}</b>. Now bowl with <code>/bowl N</code>!"
            else:
                g["stage"] = "finished"
                if g["user_score"] > g["bot_score"]: g["last_event"] += " <b>You win!</b>"; gif_win(chat_id)
                elif g["user_score"] < g["bot_score"]: g["last_event"] += " <b>Bot wins!</b>"; gif_lose(chat_id)
                else: g["last_event"] += " <b>It's a tie!</b>"; gif_tie(chat_id)
        else:
            g["user_score"] += pick
            g["last_event"] = f"You: {pick}, Bot: {bot_pick} — Runs: +{pick}"
            if g["innings"] == 2 and g["user_score"] >= g["target"]:
                g["stage"] = "finished"
                g["last_event"] += " You reached target! <b>You win!</b>"
                gif_win(chat_id)
        safe_reply(message, g["last_event"])
    except Exception as e:
        logging.error(f"Error in /bat: {e}")

@bot.message_handler(commands=['bowl'])
def cmd_bowl(message):
    try:
        chat_id = message.chat.id
        args = message.text.split()
        if chat_id not in games or games[chat_id]["stage"] not in ("first_innings", "second_innings"):
            safe_reply(message, "No active bowling. Start a game first with /play.")
            return
        g = games[chat_id]
        user_batting = (g["user_batting_first"] and g["innings"] == 1) or (not g["user_batting_first"] and g["innings"] == 2)
        if user_batting:
            safe_reply(message, "You are batting now!")
            return
        try:
            pick = int(args[1])
        except:
            safe_reply(message, "Pick a number 1–6. Example: /bowl 3")
            return
        if not 1 <= pick <= 6:
            safe_reply(message, "Pick must be 1–6.")
            return
        bot_pick = bot_move()
        if check_out(pick, bot_pick):
            g["last_event"] = f"You: {pick}, Bot: {bot_pick} — <b>BOT OUT!</b>"
            gif_wicket(chat_id)
            if g["innings"] == 1:
                g["target"] = g["bot_score"] + 1
                g["innings"] = 2
                g["stage"] = "second_innings"
                g["last_event"] += f" Target for you: <b>{g['target']}</b>. Now bat with <code>/bat N</code>!"
            else:
                g["stage"] = "finished"
                if g["bot_score"] > g["user_score"]: g["last_event"] += " <b>Bot wins!</b>"; gif_lose(chat_id)
                elif g["bot_score"] < g["user_score"]: g["last_event"] += " <b>You win!</b>"; gif_win(chat_id)
                else: g["last_event"] += " <b>It's a tie!</b>"; gif_tie(chat_id)
        else:
            g["bot_score"] += bot_pick
            g["last_event"] = f"You: {pick}, Bot: {bot_pick} — Bot scores +{bot_pick}"
            if g["innings"] == 2 and g["bot_score"] >= g["target"]:
                g["stage"] = "finished"
                g["last_event"] += " Bot reached target! <b>Bot wins!</b>"
                gif_lose(chat_id)
        safe_reply(message, g["last_event"])
    except Exception as e:
        logging.error(f"Error in /bowl: {e}")

@bot.message_handler(commands=['score'])
def cmd_score(message):
    try:
        chat_id = message.chat.id
        if chat_id in games:
            safe_reply(message, format_score(games[chat_id]))
        else:
            safe_reply(message, "No active game.")
    except Exception as e:
        logging.error(f"Error in /score: {e}")

@bot.message_handler(commands=['reset'])
def cmd_reset(message):
    try:
        chat_id = message.chat.id
        reset_game(chat_id)
        safe_reply(message, "Game reset! Use /toss to start.")
    except Exception as e:
        logging.error(f"Error in /reset: {e}")

# =========================
# Flask (webhook)
# =========================
app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return "Bot is running!", 200

@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    try:
        json_str = request.get_data().decode("utf-8")
        update = types.Update.de_json(json_str)
        bot.process_new_updates([update])
    except Exception as e:
        logging.exception(f"Webhook processing failed: {e}")
        return "error", 500
    return "ok", 200

def run_flask():
    app.run(host="0.0.0.0", port=PORT)

def run_polling():
    bot.infinity_polling(skip_pending=True)

# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    if USE_WEBHOOK:
        if not WEBHOOK_URL:
            raise ValueError("USE_WEBHOOK=1 but WEBHOOK_URL is not set")
        
        # Remove old webhook safely
        try:
            bot.remove_webhook()
            logging.info("Old webhook removed successfully.")
        except Exception as e:
            logging.warning(f"Failed to remove old webhook: {e}")

        # Set new webhook
        full_url = WEBHOOK_URL.rstrip("/") + "/webhook"
        try:
            if bot.set_webhook(url=full_url):
                logging.info(f"Webhook successfully set to: {full_url}")
            else:
                logging.warning(f"Failed to set webhook via API to: {full_url}")
        except Exception as e:
            logging.error(f"Exception during set_webhook: {e}")

        # Start Flask server (keep service alive)
        run_flask()
    else:
        # Only for local dev; never used on Render with webhook
        run_polling()

