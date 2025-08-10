print("Starting hand cricket bot...")
import telebot
from telebot import types
import random
import logging
import os
from dotenv import load_dotenv

# ---------- CONFIG ----------
load_dotenv()  # loads variables from .env file
TOKEN = "8167277248:AAFKcBe4YlDufX4z8wowACEXDU64FyaEAQs"
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is not set in environment variables")
# ----------------------------

# Enable detailed logging
logging.basicConfig(level=logging.DEBUG)
telebot.logger.setLevel(logging.DEBUG)

bot = telebot.TeleBot(TOKEN)

# Game states stored per chat_id
games = {}

def safe_reply(bot, message, text):
    """Safely send a reply without crashing if Telegram rejects it."""
    try:
        bot.reply_to(message, text)
    except Exception as e:
        logging.error(f"Failed to send message: {e}")

def new_game(chat_id):
    games[chat_id] = {
        "stage": "toss",
        "user_batting_first": None,
        "innings": 0,
        "user_score": 0,
        "bot_score": 0,
        "target": None,
        "last_event": "New game! Use /toss heads or /toss tails."
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

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    safe_reply(bot, message,
        "Welcome to Hand Cricket Bot!\n\n"
        "/play - start new game\n"
        "/toss heads|tails - toss call\n"
        "/bat N - bat with number 1–6\n"
        "/bowl N - bowl with number 1–6\n"
        "/score - show score\n"
        "/reset - reset game"
    )

@bot.message_handler(commands=['play'])
def cmd_play(message):
    chat_id = message.chat.id
    g = new_game(chat_id)
    safe_reply(bot, message, "New game created. " + g["last_event"])

@bot.message_handler(commands=['toss'])
def cmd_toss(message):
    try:
        chat_id = message.chat.id
        args = message.text.split()
        if chat_id not in games:
            new_game(chat_id)
        g = games[chat_id]

        user_choice = None
        if len(args) >= 2:
            user_choice = args[1].lower()
            if user_choice not in ("heads", "tails"):
                safe_reply(bot, message, "Invalid choice. Use /toss heads or /toss tails.")
                return

        coin, choice, win = toss_result(user_choice)
        g["last_event"] = f"Coin: {coin}, You: {choice}."
        if win:
            g["user_batting_first"] = True
            g["stage"] = "first_innings"
            g["innings"] = 1
            g["last_event"] += " You won and bat first! Use /bat N."
        else:
            g["user_batting_first"] = False
            g["stage"] = "first_innings"
            g["innings"] = 1
            g["last_event"] += " Bot bats first! Use /bowl N."
        safe_reply(bot, message, g["last_event"])
    except Exception as e:
        logging.error(f"Error in /toss: {e}")

@bot.message_handler(commands=['bat'])
def cmd_bat(message):
    try:
        chat_id = message.chat.id
        args = message.text.split()
        if chat_id not in games or games[chat_id]["stage"] not in ("first_innings", "second_innings"):
            safe_reply(bot, message, "No active batting. Start a game first.")
            return
        g = games[chat_id]

        user_batting = (g["user_batting_first"] and g["innings"] == 1) or (not g["user_batting_first"] and g["innings"] == 2)
        if not user_batting:
            safe_reply(bot, message, "You are bowling now!")
            return

        try:
            pick = int(args[1])
        except:
            safe_reply(bot, message, "Pick number 1–6.")
            return
        if not 1 <= pick <= 6:
            safe_reply(bot, message, "Pick must be 1–6.")
            return

        bot_pick = bot_move()
        if check_out(pick, bot_pick):
            g["last_event"] = f"You: {pick}, Bot: {bot_pick} — OUT!"
            if g["innings"] == 1:
                g["target"] = g["user_score"] + 1
                g["innings"] = 2
                g["stage"] = "second_innings"
                g["last_event"] += f" Target for bot: {g['target']}. Now bowl!"
            else:
                g["stage"] = "finished"
                if g["user_score"] > g["bot_score"]:
                    g["last_event"] += " You win!"
                elif g["user_score"] < g["bot_score"]:
                    g["last_event"] += " Bot wins!"
                else:
                    g["last_event"] += " It's a tie!"
        else:
            g["user_score"] += pick
            g["last_event"] = f"You: {pick}, Bot: {bot_pick} — Runs: +{pick}"
            if g["innings"] == 2 and g["user_score"] >= g["target"]:
                g["stage"] = "finished"
                g["last_event"] += " You reached target! You win!"
        safe_reply(bot, message, g["last_event"])
    except Exception as e:
        logging.error(f"Error in /bat: {e}")

@bot.message_handler(commands=['bowl'])
def cmd_bowl(message):
    try:
        chat_id = message.chat.id
        args = message.text.split()
        if chat_id not in games or games[chat_id]["stage"] not in ("first_innings", "second_innings"):
            safe_reply(bot, message, "No active bowling. Start a game first.")
            return
        g = games[chat_id]

        user_batting = (g["user_batting_first"] and g["innings"] == 1) or (not g["user_batting_first"] and g["innings"] == 2)
        if user_batting:
            safe_reply(bot, message, "You are batting now!")
            return

        try:
            pick = int(args[1])
        except:
            safe_reply(bot, message, "Pick number 1–6.")
            return
        if not 1 <= pick <= 6:
            safe_reply(bot, message, "Pick must be 1–6.")
            return

        bot_pick = bot_move()
        if check_out(pick, bot_pick):
            g["last_event"] = f"You: {pick}, Bot: {bot_pick} — BOT OUT!"
            if g["innings"] == 1:
                g["target"] = g["bot_score"] + 1
                g["innings"] = 2
                g["stage"] = "second_innings"
                g["last_event"] += f" Target for you: {g['target']}. Now bat!"
            else:
                g["stage"] = "finished"
                if g["bot_score"] > g["user_score"]:
                    g["last_event"] += " Bot wins!"
                elif g["bot_score"] < g["user_score"]:
                    g["last_event"] += " You win!"
                else:
                    g["last_event"] += " It's a tie!"
        else:
            g["bot_score"] += bot_pick
            g["last_event"] = f"You: {pick}, Bot: {bot_pick} — Bot scores +{bot_pick}"
            if g["innings"] == 2 and g["bot_score"] >= g["target"]:
                g["stage"] = "finished"
                g["last_event"] += " Bot reached target! Bot wins!"
        safe_reply(bot, message, g["last_event"])
    except Exception as e:
        logging.error(f"Error in /bowl: {e}")

@bot.message_handler(commands=['score'])
def cmd_score(message):
    try:
        chat_id = message.chat.id
        if chat_id in games:
            safe_reply(bot, message, format_score(games[chat_id]))
        else:
            safe_reply(bot, message, "No active game.")
    except Exception as e:
        logging.error(f"Error in /score: {e}")

@bot.message_handler(commands=['reset'])
def cmd_reset(message):
    try:
        chat_id = message.chat.id
        reset_game(chat_id)
        safe_reply(bot, message, "Game reset!")
    except Exception as e:
        logging.error(f"Error in /reset: {e}")


from flask import Flask
import threading

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!"


def run_bot():
    bot.infinity_polling()

# Start Flask server in a separate thread so it doesn't block the bot

if __name__ == '__main__':
    threading.Thread(target=run_bot).start()
    port = int(os.environ.get("PORT", 5000))
    print(f"Starting Flask server on port {port}")
    app.run(host="0.0.0.0", port=port)