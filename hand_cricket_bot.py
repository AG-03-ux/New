import os
import logging
from flask import Flask, request
import telebot

# -------------------------
# Environment / Config
# -------------------------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # Set your token in environment or .env
USE_WEBHOOK = int(os.getenv("USE_WEBHOOK", "0"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
PORT = int(os.getenv("PORT", 5000))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=LOG_LEVEL)
telebot.logger.setLevel(LOG_LEVEL)

# -------------------------
# Initialize Bot
# -------------------------
bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

# -------------------------
# GIF Helpers (local only)
# -------------------------
def _send_animation(chat_id, asset_path: str):
    try:
        with open(asset_path, "rb") as f:
            bot.send_animation(chat_id, f)
    except Exception as e:
        logging.error(f"Failed to send GIF {asset_path}: {e}")

def gif_six(chat_id): _send_animation(chat_id, "assets/six.gif")
def gif_wicket(chat_id): _send_animation(chat_id, "assets/wicket.gif")
def gif_win(chat_id): _send_animation(chat_id, "assets/win.gif")
def gif_lose(chat_id): _send_animation(chat_id, "assets/lose.gif")
def gif_tie(chat_id): _send_animation(chat_id, "assets/tie.gif")

# -------------------------
# Game Data
# -------------------------
games = {}  # chat_id -> game state

# -------------------------
# Game Functions
# -------------------------
def start_game(chat_id):
    games[chat_id] = {"batting": None, "score": 0, "balls": []}
    bot.send_message(chat_id, "New game created! Use /toss heads or /toss tails to start the toss.")

def toss(chat_id, choice):
    import random
    coin = random.choice(["heads", "tails"])
    if choice == coin:
        games[chat_id]["batting"] = "player"
        bot.send_message(chat_id, f"Coin: {coin}, You: {choice}. You won and bat first! Use /bat N.")
    else:
        games[chat_id]["batting"] = "bot"
        bot.send_message(chat_id, f"Coin: {coin}, You: {choice}. You lost the toss. Bot bats first!")

def bat(chat_id, number):
    number = int(number)
    bot_number = telebot.util.rand_num(1, 6)
    if number == bot_number:
        bot.send_message(chat_id, f"Oops! You got out! Bot chose {bot_number}.")
        gif_wicket(chat_id)
    else:
        games[chat_id]["score"] += number
        bot.send_message(chat_id, f"You scored {number}. Total: {games[chat_id]['score']}")
        if number == 6:
            gif_six(chat_id)

def score(chat_id):
    s = games.get(chat_id, {}).get("score", 0)
    bot.send_message(chat_id, f"Current score: {s}")

def reset_game(chat_id):
    if chat_id in games: del games[chat_id]
    bot.send_message(chat_id, "Game reset. Use /play to start a new game.")

# -------------------------
# Bot Commands
# -------------------------
@bot.message_handler(commands=["start", "play"])
def cmd_start(message):
    start_game(message.chat.id)

@bot.message_handler(commands=["toss"])
def cmd_toss(message):
    parts = message.text.split()
    if len(parts) < 2: return
    toss(message.chat.id, parts[1].lower())

@bot.message_handler(commands=["bat"])
def cmd_bat(message):
    parts = message.text.split()
    if len(parts) < 2: return
    bat(message.chat.id, parts[1])

@bot.message_handler(commands=["score"])
def cmd_score(message):
    score(message.chat.id)

@bot.message_handler(commands=["reset"])
def cmd_reset(message):
    reset_game(message.chat.id)

# -------------------------
# Flask Webhook
# -------------------------
app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    update = telebot.types.Update.de_json(request.stream.read().decode("utf-8"))
    bot.process_new_updates([update])
    return "OK", 200

def run_flask():
    app.run(host="0.0.0.0", port=PORT)

def run_polling():
    bot.infinity_polling()

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    if USE_WEBHOOK:
        if not WEBHOOK_URL:
            raise ValueError("USE_WEBHOOK=1 but WEBHOOK_URL is not set")
        try: bot.remove_webhook()
        except: pass
        full_url = WEBHOOK_URL.rstrip("/") + "/webhook"
        if bot.set_webhook(url=full_url):
            logging.info(f"Webhook set to: {full_url}")
        else:
            logging.warning("Failed to set webhook via API")
        run_flask()
    else:
        run_polling()
