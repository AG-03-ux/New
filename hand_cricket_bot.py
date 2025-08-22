import os
import logging
from flask import Flask, request
from telebot import TeleBot, types
import random

# -------------------------
# Load environment variables
# -------------------------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
USE_WEBHOOK = int(os.getenv("USE_WEBHOOK", "0"))
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
PORT = int(os.getenv("PORT", 5000))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# -------------------------
# Logging setup
# -------------------------
logging.basicConfig(level=LOG_LEVEL)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

# -------------------------
# Bot setup
# -------------------------
bot = TeleBot(TOKEN, parse_mode="HTML")

# -------------------------
# Flask app for webhook
# -------------------------
app = Flask(__name__)

# -------------------------
# GIF paths
# -------------------------
GIF_PATHS = {
    "six": "assets/six.gif",
    "wicket": "assets/wicket.gif",
    "win": "assets/win.gif",
    "lose": "assets/lose.gif",
    "tie": "assets/tie.gif",
}

def send_gif(chat_id, gif_name):
    """Send a GIF by name from assets folder."""
    asset_path = GIF_PATHS.get(gif_name)
    if not asset_path:
        logging.error(f"Invalid GIF name: {gif_name}")
        return
    try:
        with open(asset_path, "rb") as f:
            bot.send_animation(chat_id, f)
    except Exception as e:
        logging.error(f"Failed to send GIF '{gif_name}': {e}")

# Shortcut functions
def gif_six(chat_id): send_gif(chat_id, "six")
def gif_wicket(chat_id): send_gif(chat_id, "wicket")
def gif_win(chat_id): send_gif(chat_id, "win")
def gif_lose(chat_id): send_gif(chat_id, "lose")
def gif_tie(chat_id): send_gif(chat_id, "tie")

def safe_reply(message, text):
    try:
        bot.reply_to(message, text)
    except Exception as e:
        logging.error(f"Failed to send message: {e}")

# -------------------------
# Game state storage
# -------------------------
# Structure: {chat_id: {"state": str, "player_score": int, "bot_score": int, "turn": "bat"| "bowl"}}
games = {}

# -------------------------
# Bot command handlers
# -------------------------
@bot.message_handler(commands=["start"])
def handle_start(message):
    safe_reply(message,
               "🏏 <b>Welcome to Hand Cricket Bot!</b>\n\n"
               "<b>Commands</b>\n"
               "/play - start a new game\n"
               "/toss heads|tails - call the toss\n"
               "/bat N - bat with number 1-6\n"
               "/bowl N - bowl with number 1-6\n"
               "/score - show the score\n"
               "/reset - reset the game\n\n"
               "Tip: Add GIFs in assets/ folder!"
               )

@bot.message_handler(commands=["play"])
def handle_play(message):
    chat_id = message.chat.id
    games[chat_id] = {"state": "new_game", "player_score": 0, "bot_score": 0, "turn": None}
    safe_reply(message, "New game created! Use /toss heads or /toss tails to start.")

@bot.message_handler(commands=["toss"])
def handle_toss(message):
    chat_id = message.chat.id
    if chat_id not in games:
        safe_reply(message, "Start a game first with /play")
        return
    user_call = message.text.split()[-1].lower()
    coin = random.choice(["heads", "tails"])
    if user_call == coin:
        games[chat_id]["turn"] = "bat"
        safe_reply(message, f"Coin: <b>{coin}</b>, You: <i>{user_call}</i>. You won and bat first! Use /bat N.")
    else:
        games[chat_id]["turn"] = "bowl"
        safe_reply(message, f"Coin: <b>{coin}</b>, You: <i>{user_call}</i>. You lost the toss. Bot bats first!")

@bot.message_handler(commands=["bat", "bowl"])
def handle_play_action(message):
    chat_id = message.chat.id
    if chat_id not in games:
        safe_reply(message, "Start a game first with /play")
        return
    game = games[chat_id]

    try:
        num = int(message.text.split()[-1])
        if num < 1 or num > 6:
            raise ValueError
    except ValueError:
        safe_reply(message, "Please use a number between 1 and 6.")
        return

    bot_num = random.randint(1,6)
    turn = game["turn"]

    if turn == "bat" and message.text.startswith("/bat"):
        if num == bot_num:
            safe_reply(message, f"You chose {num}, bot chose {bot_num}. You are out!")
            gif_wicket(chat_id)
            # Switch turn
            game["turn"] = "bowl"
        else:
            game["player_score"] += num
            if num == 6: gif_six(chat_id)
            safe_reply(message, f"You chose {num}, bot chose {bot_num}. Runs added: {num}")

    elif turn == "bowl" and message.text.startswith("/bowl"):
        if num == bot_num:
            safe_reply(message, f"Bot is out!")
            gif_wicket(chat_id)
            game["turn"] = "bat"
        else:
            game["bot_score"] += bot_num
            safe_reply(message, f"Bot scored {bot_num} runs.")

    # Check for game end (first inning done)
    if turn == "bat" and game["turn"] == "bowl":
        safe_reply(message, f"Your inning is over. Your score: {game['player_score']}. Now bowl to the bot using /bowl N.")
    elif turn == "bowl" and game["turn"] == "bat":
        safe_reply(message, f"Bot's inning is over. Bot score: {game['bot_score']}. You bat now using /bat N.")

    # Check for end of second inning
    if turn == "bowl" and game["turn"] == "bowl":
        # Player finished chasing
        if game["player_score"] > game["bot_score"]:
            safe_reply(message, f"You won! 🎉 Final score: You {game['player_score']} - Bot {game['bot_score']}")
            gif_win(chat_id)
        elif game["player_score"] < game["bot_score"]:
            safe_reply(message, f"You lost! 😢 Final score: You {game['player_score']} - Bot {game['bot_score']}")
            gif_lose(chat_id)
        else:
            safe_reply(message, f"It's a tie! 🤝 Final score: You {game['player_score']} - Bot {game['bot_score']}")
            gif_tie(chat_id)
        # Reset game automatically
        del games[chat_id]

@bot.message_handler(commands=["score"])
def handle_score(message):
    chat_id = message.chat.id
    if chat_id in games:
        game = games[chat_id]
        safe_reply(message, f"Current Score:\nYou: {game['player_score']}\nBot: {game['bot_score']}\nTurn: {game['turn']}")
    else:
        safe_reply(message, "No active game. Start one with /play")

@bot.message_handler(commands=["reset"])
def handle_reset(message):
    chat_id = message.chat.id
    if chat_id in games:
        del games[chat_id]
    safe_reply(message, "Game reset. Start a new game with /play")

# -------------------------
# Webhook endpoint
# -------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    json_data = request.get_json()
    bot.process_new_updates([types.Update.de_json(json_data)])
    return "", 200

# -------------------------
# Start the bot
# -------------------------
if __name__ == "__main__":
    if USE_WEBHOOK:
        if not WEBHOOK_URL:
            raise ValueError("USE_WEBHOOK=1 but WEBHOOK_URL is not set")
        try:
            bot.remove_webhook()
            full_url = WEBHOOK_URL.rstrip("/") + "/webhook"
            if bot.set_webhook(url=full_url):
                logging.info(f"Webhook set to: {full_url}")
            else:
                logging.warning("Failed to set webhook")
        except Exception as e:
            logging.error(f"Webhook setup failed: {e}")
        app.run(host="0.0.0.0", port=PORT)
    else:
        bot.polling(none_stop=True)
