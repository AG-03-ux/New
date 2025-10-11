#!/usr/bin/env python3
"""
Simplified Cricket Bot - For Testing Only
This removes complex features to isolate the core issue
"""

import os
import logging
import telebot
from telebot import types
from dotenv import load_dotenv

# Load environment
load_dotenv()

# Simple logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get token
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN not found!")

# Initialize bot
bot = telebot.TeleBot(TOKEN, parse_mode="HTML")

# Test if bot is working
try:
    bot_info = bot.get_me()
    logger.info(f"Bot connected: @{bot_info.username}")
except Exception as e:
    logger.error(f"Bot connection failed: {e}")
    raise

# Simple handlers for testing
@bot.message_handler(commands=['start'])
def cmd_start(message):
    try:
        logger.info(f"Received /start from user {message.from_user.id}")
        
        welcome_text = (
            f"🏏 Hello {message.from_user.first_name}!\n\n"
            f"This is a simplified test version of Cricket Bot.\n\n"
            f"Try these commands:\n"
            f"/play - Start a game\n"
            f"/help - Show help\n"
            f"/test - Test response"
        )
        
        # Simple inline keyboard
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("🎮 Test Button", callback_data="test_button"),
            types.InlineKeyboardButton("📊 Another Test", callback_data="test_two")
        )
        
        bot.send_message(message.chat.id, welcome_text, reply_markup=keyboard)
        logger.info(f"Sent welcome message to {message.from_user.id}")
        
    except Exception as e:
        logger.error(f"Error in /start: {e}")
        bot.reply_to(message, "Error occurred. Check logs.")

@bot.message_handler(commands=['test'])
def cmd_test(message):
    try:
        logger.info(f"Received /test from user {message.from_user.id}")
        bot.reply_to(message, "✅ Bot is responding! Test successful.")
    except Exception as e:
        logger.error(f"Error in /test: {e}")

@bot.message_handler(commands=['play'])
def cmd_play(message):
    try:
        logger.info(f"Received /play from user {message.from_user.id}")
        
        # Simple toss keyboard
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("🪙 Heads", callback_data="toss_heads"),
            types.InlineKeyboardButton("🪙 Tails", callback_data="toss_tails")
        )
        
        bot.send_message(
            message.chat.id, 
            "🪙 Let's play cricket! Choose heads or tails for the toss:",
            reply_markup=keyboard
        )
        
    except Exception as e:
        logger.error(f"Error in /play: {e}")
        bot.reply_to(message, "Error starting game. Check logs.")

@bot.message_handler(commands=['help'])
def cmd_help(message):
    try:
        help_text = (
            "🏏 <b>Cricket Bot - Test Version</b>\n\n"
            "<b>Commands:</b>\n"
            "/start - Start the bot\n"
            "/play - Start a cricket game\n"
            "/test - Test if bot responds\n"
            "/help - Show this help\n\n"
            "<b>Status:</b> This is a simplified test version."
        )
        bot.send_message(message.chat.id, help_text)
    except Exception as e:
        logger.error(f"Error in /help: {e}")

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    try:
        logger.info(f"Received callback: {call.data} from user {call.from_user.id}")
        
        if call.data == "test_button":
            bot.answer_callback_query(call.id, "Test button clicked!")
            bot.edit_message_text(
                "✅ Test button was clicked successfully!",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id
            )
            
        elif call.data == "test_two":
            bot.answer_callback_query(call.id, "Second test successful!")
            bot.send_message(call.message.chat.id, "📊 Second test button works!")
            
        elif call.data == "toss_heads":
            import random
            result = random.choice(["heads", "tails"])
            bot.answer_callback_query(call.id, f"You chose heads! Result: {result}")
            
            if result == "heads":
                bot.send_message(call.message.chat.id, "🎉 You won the toss! Game logic would continue here...")
            else:
                bot.send_message(call.message.chat.id, "😔 You lost the toss. Game logic would continue here...")
                
        elif call.data == "toss_tails":
            import random
            result = random.choice(["heads", "tails"])
            bot.answer_callback_query(call.id, f"You chose tails! Result: {result}")
            
            if result == "tails":
                bot.send_message(call.message.chat.id, "🎉 You won the toss! Game logic would continue here...")
            else:
                bot.send_message(call.message.chat.id, "😔 You lost the toss. Game logic would continue here...")
        
        else:
            bot.answer_callback_query(call.id, "Unknown button")
            logger.warning(f"Unknown callback data: {call.data}")
            
    except Exception as e:
        logger.error(f"Error in callback handler: {e}")
        bot.answer_callback_query(call.id, "Error occurred")

@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    try:
        logger.info(f"Received message: '{message.text}' from user {message.from_user.id}")
        
        if message.text and message.text.isdigit():
            bot.reply_to(message, f"You sent number: {message.text}")
        else:
            bot.reply_to(message, f"You said: {message.text}\n\nTry /start or /help")
            
    except Exception as e:
        logger.error(f"Error in message handler: {e}")

def main():
    logger.info("=" * 50)
    logger.info("SIMPLIFIED CRICKET BOT STARTING")
    logger.info("=" * 50)
    
    try:
        # Clear any webhook
        logger.info("Clearing webhook...")
        bot.remove_webhook()
        
        # Start polling
        logger.info("Starting polling...")
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()