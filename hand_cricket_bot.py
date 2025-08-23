import os
import logging
import random
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
    games[chat_id] = {
        "toss_done": False,
        "batting": None,        # "player" or "bot"
        "innings": 1,           # 1 or 2
        "player_score": 0,
        "bot_score": 0,
        "target": None,
        "out": False,
        "balls": 0
    }
    bot.send_message(chat_id, "🏏 New game created!\nUse <b>/toss heads</b> or <b>/toss tails</b> to start the toss.")

def toss(chat_id, choice):
    state = games.get(chat_id)
    if not state: return

    coin = random.choice(["heads", "tails"])
    if choice == coin:
        state["batting"] = "player"
        bot.send_message(chat_id, f"🪙 Coin: {coin}. You won the toss and bat first!\nUse /bat N (1–6).")
    else:
        state["batting"] = "bot"
        bot.send_message(chat_id, f"🪙 Coin: {coin}. You lost the toss. Bot bats first!\nUse /bowl N (1–6).")
    state["toss_done"] = True

def player_bat(chat_id, number):
    state = games.get(chat_id)
    if not state or not state["toss_done"]: return

    bot_number = random.randint(1, 6)
    state["balls"] += 1

    if number == bot_number:
        bot.send_message(chat_id, f"❌ OUT! Bot chose {bot_number}.")
        gif_wicket(chat_id)
        end_innings(chat_id)
    else:
        state["player_score"] += number
        bot.send_message(chat_id, f"🏏 You scored {number} (Bot: {bot_number}) | Total: {state['player_score']}")
        if number == 6:
            gif_six(chat_id)

        if state["innings"] == 2 and state["player_score"] > state["target"]:
            end_match(chat_id)

def player_bowl(chat_id, number):
    state = games.get(chat_id)
    if not state or not state["toss_done"]: return

    bot_number = random.randint(1, 6)
    state["balls"] += 1

    if number == bot_number:
        bot.send_message(chat_id, f"❌ BOT OUT! You bowled {number}.")
        gif_wicket(chat_id)
        end_innings(chat_id)
    else:
        state["bot_score"] += bot_number
        bot.send_message(chat_id, f"🤖 Bot scored {bot_number} (You: {number}) | Total: {state['bot_score']}")
        if bot_number == 6:
            gif_six(chat_id)

        if state["innings"] == 2 and state["bot_score"] > state["target"]:
            end_match(chat_id)

def end_innings(chat_id):
    state = games.get(chat_id)
    if not state: return

    if state["innings"] == 1:
        # Switch innings
        if state["batting"] == "player":
            state["target"] = state["player_score"]
            state["batting"] = "bot"
            bot.send_message(chat_id, f"End of 1st innings! 🎯 Target for bot: {state['player_score']+1}\nNow use /bowl N.")
        else:
            state["target"] = state["bot_score"]
            state["batting"] = "player"
            bot.send_message(chat_id, f"End of 1st innings! 🎯 Target for you: {state['bot_score']+1}\nNow use /bat N.")
        state["innings"] = 2
        state["balls"] = 0
    else:
        # End match
        end_match(chat_id)

def end_match(chat_id):
    state = games.get(chat_id)
    if not state: return

    p, b = state["player_score"], state["bot_score"]
    msg = f"🏁 Match Over!\n\nYou: {p}\nBot: {b}\n\n"

    if p > b:
        msg += "🎉 You WIN!"
        gif_win(chat_id)
    elif b > p:
        msg += "🤖 Bot WINS!"
        gif_lose(chat_id)
    else:
        msg += "😮 It's a TIE!"
        gif_tie(chat_id)

    bot.send_message(chat_id, msg)
    del games[chat_id]  # reset game

def score(chat_id):
    state = games.get(chat_id)
    if not state:
        bot.send_message(chat_id, "No game in progress. Use /play to start.")
        return
    bot.send_message(chat_id, f"📊 Score:\nYou: {state['player_score']}\nBot: {state['bot_score']}")

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
    try:
        num = int(parts[1])
        if 1 <= num <= 6:
            player_bat(message.chat.id, num)
    except: pass

@bot.message_handler(commands=["bowl"])
def cmd_bowl(message):
    parts = message.text.split()
    if len(parts) < 2: return
    try:
        num = int(parts[1])
        if 1 <= num <= 6:
            player_bowl(message.chat.id, num)
    except: pass

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
