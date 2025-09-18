import os
import logging
import random
import sqlite3
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager
from typing import Optional, Dict, Any, List, Tuple
import json
import requests
import threading
import time
import re

from flask import Flask, request, jsonify
import telebot
from telebot import types

from dotenv import load_dotenv
load_dotenv()

# ======================================================
# Environment / Config
# ======================================================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # required
USE_WEBHOOK = int(os.getenv("USE_WEBHOOK", "1"))  # 1 for webhook on Render
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")  # Set this in Render environment variables
PORT = int(os.getenv("PORT", 5000))  # Render sets this automatically
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DB_PATH = os.getenv("DB_PATH", "cricket_bot.db")
DEFAULT_OVERS = int(os.getenv("DEFAULT_OVERS", "2"))
DEFAULT_WICKETS = int(os.getenv("DEFAULT_WICKETS", "1"))  # classic hand-cricket: 1 wicket
MAX_OVERS = 20
MAX_WICKETS = 10
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(","))) if os.getenv("ADMIN_IDS") else []

# ======================================================
# Logging
# ======================================================
logging.basicConfig(level=LOG_LEVEL, format='[%(levelname)s] %(asctime)s - %(message)s')
telebot.logger.setLevel(LOG_LEVEL)
logger = logging.getLogger("cricket-bot")

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

# ======================================================
# Initialize Bot
# ======================================================
bot = telebot.TeleBot(TOKEN, parse_mode="HTML", threaded=True)

# ======================================================
# Enhanced GIF/Animation System with Online URLs
# ======================================================
CRICKET_GIFS = {
    "six": [
        "https://media.giphy.com/media/3o7TKSjRrfIPjeiVyM/giphy.gif",
        "https://media.tenor.com/images/8b5e4c1d9a0b4c5f8a7b3d2e1f0g9h8i/tenor.gif",
        "https://i.imgur.com/cricketSix1.gif"  # You can replace with actual working URLs
    ],
    "wicket": [
        "https://media.giphy.com/media/l0HlvtIPzPdt2usKs/giphy.gif",
        "https://media.tenor.com/images/wicket1.gif",
        "https://i.imgur.com/wicket1.gif"
    ],
    "win": [
        "https://media.giphy.com/media/26u4cqiYI30juCOGY/giphy.gif",
        "https://media.tenor.com/images/celebration1.gif"
    ],
    "lose": [
        "https://media.giphy.com/media/26tn33aiTi1jkl6H6/giphy.gif",
        "https://media.tenor.com/images/defeat1.gif"
    ],
    "tie": [
        "https://media.giphy.com/media/3o7527pa7qs9kCG78A/giphy.gif"
    ],
    "four": [
        "https://media.giphy.com/media/cricket_four_1/giphy.gif"
    ],
    "century": [
        "https://media.giphy.com/media/century_celebration/giphy.gif"
    ],
    "fifty": [
        "https://media.giphy.com/media/fifty_celebration/giphy.gif"
    ]
}

# Cricket emojis and reactions
CRICKET_EMOJIS = {
    "boundary": "🔥",
    "six": "🚀",
    "four": "⚡",
    "wicket": "💥",
    "maiden": "🛡️",
    "century": "💯",
    "fifty": "5️⃣0️⃣",
    "duck": "🦆",
    "hat_trick": "🎩",
    "win": "🏆",
    "lose": "😔",
    "tie": "🤝"
}

def send_cricket_animation(chat_id: int, event_type: str, caption: str = ""):
    """Send cricket-related animations with fallback to emojis"""
    try:
        # Try to send GIF
        if event_type in CRICKET_GIFS and CRICKET_GIFS[event_type]:
            gif_url = random.choice(CRICKET_GIFS[event_type])
            try:
                bot.send_animation(chat_id, gif_url, caption=caption)
                return True
            except Exception as e:
                logger.warning(f"Failed to send GIF {gif_url}: {e}")
        
        # Fallback to emoji reaction
        if event_type in CRICKET_EMOJIS:
            emoji = CRICKET_EMOJIS[event_type]
            message = f"{emoji} {caption}" if caption else emoji
            bot.send_message(chat_id, message)
            return True
            
    except Exception as e:
        logger.error(f"Failed to send animation for {event_type}: {e}")
    
    return False

# ======================================================
# Enhanced Persistence (SQLite)
# ======================================================

@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        conn.close()

def db_init():
    with db_conn() as db:
        # Users table with enhanced fields
        db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                language_code TEXT,
                is_premium BOOLEAN DEFAULT FALSE,
                created_at TEXT,
                last_active TEXT,
                total_messages INTEGER DEFAULT 0,
                favorite_format TEXT DEFAULT '2,1'
            )
        """)
        
        # Enhanced stats table
        db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                user_id INTEGER PRIMARY KEY,
                games_played INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                ties INTEGER DEFAULT 0,
                high_score INTEGER DEFAULT 0,
                best_chase INTEGER DEFAULT 0,
                fastest_50_balls INTEGER DEFAULT NULL,
                fastest_century_balls INTEGER DEFAULT NULL,
                total_runs INTEGER DEFAULT 0,
                total_balls_faced INTEGER DEFAULT 0,
                total_wickets_taken INTEGER DEFAULT 0,
                total_balls_bowled INTEGER DEFAULT 0,
                sixes_hit INTEGER DEFAULT 0,
                fours_hit INTEGER DEFAULT 0,
                ducks INTEGER DEFAULT 0,
                centuries INTEGER DEFAULT 0,
                fifties INTEGER DEFAULT 0,
                hat_tricks INTEGER DEFAULT 0,
                maidens_bowled INTEGER DEFAULT 0,
                best_bowling_figures TEXT,
                longest_winning_streak INTEGER DEFAULT 0,
                current_winning_streak INTEGER DEFAULT 0,
                avg_score REAL DEFAULT 0.0,
                strike_rate REAL DEFAULT 0.0,
                economy_rate REAL DEFAULT 0.0,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        
        # Enhanced games table
        db.execute("""
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
                match_format TEXT,
                difficulty_level TEXT DEFAULT 'medium',
                player_balls_faced INTEGER DEFAULT 0,
                bot_balls_faced INTEGER DEFAULT 0,
                player_fours INTEGER DEFAULT 0,
                player_sixes INTEGER DEFAULT 0,
                bot_fours INTEGER DEFAULT 0,
                bot_sixes INTEGER DEFAULT 0,
                extras INTEGER DEFAULT 0,
                powerplay_overs INTEGER DEFAULT 0,
                is_powerplay BOOLEAN DEFAULT FALSE,
                weather_condition TEXT DEFAULT 'clear',
                pitch_condition TEXT DEFAULT 'normal',
                created_at TEXT,
                updated_at TEXT
            )
        """)
        
        # Match history with detailed tracking
        db.execute("""
            CREATE TABLE IF NOT EXISTS match_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                user_id INTEGER,
                match_format TEXT,
                player_score INTEGER,
                bot_score INTEGER,
                player_wickets INTEGER,
                bot_wickets INTEGER,
                overs_played REAL,
                result TEXT,
                margin TEXT,
                player_strike_rate REAL,
                match_duration_minutes INTEGER,
                match_events TEXT,
                created_at TEXT,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)
        
        # Ball-by-ball commentary
        db.execute("""
            CREATE TABLE IF NOT EXISTS ball_by_ball (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                match_id INTEGER,
                innings INTEGER,
                over_number INTEGER,
                ball_number INTEGER,
                batting_team TEXT,
                batsman_score INTEGER,
                bowler_score INTEGER,
                runs_scored INTEGER,
                is_wicket BOOLEAN,
                extras INTEGER,
                commentary TEXT,
                created_at TEXT
            )
        """)
        
        # Tournament system
        db.execute("""
            CREATE TABLE IF NOT EXISTS tournaments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                format TEXT,
                max_participants INTEGER,
                entry_fee INTEGER DEFAULT 0,
                prize_pool INTEGER DEFAULT 0,
                status TEXT DEFAULT 'upcoming',
                start_date TEXT,
                end_date TEXT,
                created_by INTEGER,
                created_at TEXT,
                FOREIGN KEY (created_by) REFERENCES users (user_id)
            )
        """)
        
        # Tournament participants
        db.execute("""
            CREATE TABLE IF NOT EXISTS tournament_participants (
                tournament_id INTEGER,
                user_id INTEGER,
                joined_at TEXT,
                is_eliminated BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (tournament_id, user_id),
                FOREIGN KEY (tournament_id) REFERENCES tournaments (id),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)
        
        # Achievements system
        db.execute("""
            CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                description TEXT,
                icon TEXT,
                points INTEGER DEFAULT 0,
                requirement_type TEXT,
                requirement_value INTEGER
            )
        """)
        
        # User achievements
        db.execute("""
            CREATE TABLE IF NOT EXISTS user_achievements (
                user_id INTEGER,
                achievement_id INTEGER,
                unlocked_at TEXT,
                PRIMARY KEY (user_id, achievement_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (achievement_id) REFERENCES achievements (id)
            )
        """)
        
        # Daily challenges
        db.execute("""
            CREATE TABLE IF NOT EXISTS daily_challenges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                challenge_date TEXT,
                challenge_type TEXT,
                target_value INTEGER,
                reward_points INTEGER,
                description TEXT
            )
        """)
        
        # User challenge progress
        db.execute("""
            CREATE TABLE IF NOT EXISTS user_challenges (
                user_id INTEGER,
                challenge_id INTEGER,
                current_progress INTEGER DEFAULT 0,
                completed BOOLEAN DEFAULT FALSE,
                completed_at TEXT,
                PRIMARY KEY (user_id, challenge_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (challenge_id) REFERENCES daily_challenges (id)
            )
        """)

        # Initialize default achievements
        achievements = [
            ("First Victory", "Win your first match", "🏆", 10, "wins", 1),
            ("Century Maker", "Score 100 runs in a match", "💯", 50, "high_score", 100),
            ("Hat-trick Hero", "Take 3 wickets in consecutive balls", "🎩", 75, "hat_tricks", 1),
            ("Consistent Player", "Win 5 matches in a row", "🔥", 100, "winning_streak", 5),
            ("Big Hitter", "Hit 50 sixes", "🚀", 25, "sixes_hit", 50),
            ("Boundary King", "Hit 100 fours", "⚡", 30, "fours_hit", 100),
            ("Marathon Player", "Play 100 matches", "🏃", 150, "games_played", 100),
            ("Perfect Game", "Win without losing a wicket", "👑", 200, "perfect_game", 1),
        ]
        
        for name, desc, icon, points, req_type, req_val in achievements:
            db.execute("""
                INSERT OR IGNORE INTO achievements 
                (name, description, icon, points, requirement_type, requirement_value)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (name, desc, icon, points, req_type, req_val))

def upsert_user(u: types.User):
    with db_conn() as db:
        now = datetime.now(timezone.utc).isoformat()
        db.execute("""
            INSERT INTO users (
                user_id, username, first_name, last_name, language_code, 
                is_premium, created_at, last_active, total_messages
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                language_code=excluded.language_code,
                is_premium=excluded.is_premium,
                last_active=excluded.last_active,
                total_messages=total_messages + 1
        """, (u.id, u.username, u.first_name, u.last_name, 
              u.language_code, getattr(u, 'is_premium', False), now, now))
        
        db.execute("""
            INSERT INTO stats (user_id, created_at, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET updated_at=excluded.updated_at
        """, (u.id, now, now))

def log_event(chat_id: int, event: str, meta: str = ""):
    # Using the existing history table for events
    with db_conn() as db:
        db.execute(
            "INSERT INTO history (chat_id, event, meta, created_at) VALUES (?, ?, ?, ?)",
            (chat_id, event, meta, datetime.now(timezone.utc).isoformat())
        )

# ======================================================
# Enhanced Game Mechanics
# ======================================================

DIFFICULTY_SETTINGS = {
    "easy": {"bot_aggression": 0.3, "bot_skill": 0.4, "description": "Bot plays defensively"},
    "medium": {"bot_aggression": 0.5, "bot_skill": 0.6, "description": "Balanced gameplay"},  
    "hard": {"bot_aggression": 0.7, "bot_skill": 0.8, "description": "Bot plays aggressively"},
    "expert": {"bot_aggression": 0.9, "bot_skill": 0.9, "description": "Maximum difficulty"}
}

WEATHER_CONDITIONS = {
    "clear": {"description": "Perfect conditions", "effect": 1.0},
    "cloudy": {"description": "Overcast skies", "effect": 0.95},
    "windy": {"description": "Strong winds", "effect": 0.9},
    "rain": {"description": "Light rain", "effect": 0.8}
}

PITCH_CONDITIONS = {
    "normal": {"description": "Good batting pitch", "batting_bonus": 1.0, "bowling_bonus": 1.0},
    "batting": {"description": "Flat batting track", "batting_bonus": 1.2, "bowling_bonus": 0.8},
    "bowling": {"description": "Bowler-friendly pitch", "batting_bonus": 0.8, "bowling_bonus": 1.2},
    "turning": {"description": "Spin-friendly pitch", "batting_bonus": 0.9, "bowling_bonus": 1.1}
}

def default_game(overs: int = DEFAULT_OVERS, wickets: int = DEFAULT_WICKETS, 
                difficulty: str = "medium") -> Dict[str, Any]:
    overs = max(1, min(overs, MAX_OVERS))
    wickets = max(1, min(wickets, MAX_WICKETS))
    
    # Determine powerplay overs (first 6 overs or 1/4 of total overs, whichever is less)
    powerplay = min(6, max(1, overs // 4))
    
    return dict(
        state="toss",
        innings=1,
        batting=None,
        player_score=0,
        bot_score=0,
        player_wkts=0,
        bot_wkts=0,
        balls_in_over=0,
        overs_bowled=0,
        target=None,
        overs_limit=overs,
        wickets_limit=wickets,
        match_format=f"T{overs}",
        difficulty_level=difficulty,
        player_balls_faced=0,
        bot_balls_faced=0,
        player_fours=0,
        player_sixes=0,
        bot_fours=0,
        bot_sixes=0,
        extras=0,
        powerplay_overs=powerplay,
        is_powerplay=True,
        weather_condition=random.choice(list(WEATHER_CONDITIONS.keys())),
        pitch_condition=random.choice(list(PITCH_CONDITIONS.keys())),
        created_at=datetime.now(timezone.utc).isoformat(),
        updated_at=datetime.now(timezone.utc).isoformat(),
    )

def calculate_bot_move(g: Dict[str, Any], user_value: int) -> int:
    """Enhanced bot AI with difficulty-based decision making"""
    difficulty = g.get("difficulty_level", "medium")
    settings = DIFFICULTY_SETTINGS[difficulty]
    
    # Base random choice
    bot_choice = random.randint(1, 6)
    
    # Apply AI logic based on difficulty
    if random.random() < settings["bot_skill"]:
        # Strategic decision making
        if g["batting"] == "bot":
            # Bot is batting
            if g["innings"] == 2 and g["target"]:
                # Chase mode - calculate required rate
                balls_left = (g["overs_limit"] - g["overs_bowled"]) * 6 - g["balls_in_over"]
                runs_needed = g["target"] - g["bot_score"] + 1
                
                if balls_left > 0:
                    required_rate = runs_needed / balls_left
                    
                    if required_rate > 8:  # Aggressive mode
                        bot_choice = random.choices([4, 5, 6], weights=[2, 3, 4])[0]
                    elif required_rate < 4:  # Conservative mode
                        bot_choice = random.choices([1, 2, 3], weights=[3, 2, 1])[0]
            
            # Avoid user's common patterns (if we tracked them)
            if random.random() < settings["bot_aggression"]:
                # Try to avoid user's last few choices
                avoid_value = user_value if random.random() < 0.7 else random.randint(1, 6)
                while bot_choice == avoid_value:
                    bot_choice = random.randint(1, 6)
        
        else:
            # Bot is bowling - try to get user out
            if random.random() < 0.6:  # 60% chance to try matching user
                bot_choice = user_value
            else:
                # Try common user patterns
                common_choices = [1, 6, 4]  # Most common human choices
                bot_choice = random.choice(common_choices)
    
    return bot_choice

def get_commentary(g: Dict[str, Any], user_value: int, bot_value: int, 
                  runs_scored: int, is_wicket: bool) -> str:
    """Generate dynamic cricket commentary"""
    commentaries = []
    
    if is_wicket:
        wicket_comments = [
            f"💥 BOWLED! What a delivery! {user_value} meets {bot_value}",
            f"🎯 CAUGHT! Brilliant bowling! Both played {user_value}",
            f"⚡ CLEAN BOWLED! The stumps are shattered! {user_value} = {bot_value}",
            f"🔥 WICKET! The crowd goes wild! Matching {user_value}s",
            f"💀 PLUMB LBW! Dead in front! {user_value} vs {bot_value}"
        ]
        commentary = random.choice(wicket_comments)
        
        # Special wicket situations
        if g["player_score"] == 0 and g["player_wkts"] == 1:
            commentary += " 🦆 GOLDEN DUCK!"
        elif g["balls_in_over"] == 5 and g["player_wkts"] == g["wickets_limit"]:
            commentary += " 🎩 WHAT A FINISH!"
            
    else:
        # Scoring shots
        if runs_scored == 6:
            six_comments = [
                f"🚀 MAXIMUM! Into the stands! {runs_scored} runs!",
                f"💥 SIX! What a shot! {runs_scored} runs added!",
                f"🔥 BOOM! That's out of here! {runs_scored} runs!",
                f"⭐ STELLAR HIT! {runs_scored} runs to the total!"
            ]
            commentary = random.choice(six_comments)
            
            # Milestone checks
            if (g["player_score"] + runs_scored) == 50:
                commentary += " 🎉 FIFTY UP!"
            elif (g["player_score"] + runs_scored) == 100:
                commentary += " 💯 CENTURY! INCREDIBLE!"
                
        elif runs_scored == 4:
            four_comments = [
                f"⚡ FOUR! Races to the boundary! {runs_scored} runs!",
                f"🎯 CRACKING SHOT! {runs_scored} runs added!",
                f"🏏 BEAUTIFUL STROKE! {runs_scored} runs!"
            ]
            commentary = random.choice(four_comments)
            
        elif runs_scored == 0:
            dot_comments = [
                "🛡️ Solid defense! No run.",
                "⭕ Dot ball! Tight bowling.",
                "🎯 Well left! No run taken."
            ]
            commentary = random.choice(dot_comments)
            
        else:
            regular_comments = [
                f"🏏 Well played! {runs_scored} run{'s' if runs_scored > 1 else ''}",
                f"✅ Good running! {runs_scored} run{'s' if runs_scored > 1 else ''}",
                f"👍 Nicely done! {runs_scored} run{'s' if runs_scored > 1 else ''}"
            ]
            commentary = random.choice(regular_comments)
    
    # Add situational context
    if g["innings"] == 2 and g["target"]:
        runs_needed = g["target"] - (g["player_score"] if g["batting"] == "player" else g["bot_score"])
        balls_left = (g["overs_limit"] - g["overs_bowled"]) * 6 - g["balls_in_over"]
        
        if runs_needed <= 6:
            commentary += f" 🎯 Just {runs_needed} needed!"
        elif balls_left <= 6:
            commentary += f" ⏰ Only {balls_left} balls left!"
    
    return commentary

def save_game(chat_id: int, g: Dict[str, Any]):
    with db_conn() as db:
        now = datetime.now(timezone.utc).isoformat()
        g["updated_at"] = now
        
        db.execute("""
            INSERT INTO games (
                chat_id, state, innings, batting, player_score, bot_score,
                player_wkts, bot_wkts, balls_in_over, overs_bowled, target,
                overs_limit, wickets_limit, match_format, difficulty_level,
                player_balls_faced, bot_balls_faced, player_fours, player_sixes,
                bot_fours, bot_sixes, extras, powerplay_overs, is_powerplay,
                weather_condition, pitch_condition, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                state=excluded.state, innings=excluded.innings, batting=excluded.batting,
                player_score=excluded.player_score, bot_score=excluded.bot_score,
                player_wkts=excluded.player_wkts, bot_wkts=excluded.bot_wkts,
                balls_in_over=excluded.balls_in_over, overs_bowled=excluded.overs_bowled,
                target=excluded.target, player_balls_faced=excluded.player_balls_faced,
                bot_balls_faced=excluded.bot_balls_faced, player_fours=excluded.player_fours,
                player_sixes=excluded.player_sixes, bot_fours=excluded.bot_fours,
                bot_sixes=excluded.bot_sixes, extras=excluded.extras,
                is_powerplay=excluded.is_powerplay, updated_at=excluded.updated_at
        """, (
            chat_id, g["state"], g["innings"], g["batting"],
            g["player_score"], g["bot_score"], g["player_wkts"], g["bot_wkts"],
            g["balls_in_over"], g["overs_bowled"], g["target"],
            g["overs_limit"], g["wickets_limit"], g["match_format"], g["difficulty_level"],
            g["player_balls_faced"], g["bot_balls_faced"], g["player_fours"], g["player_sixes"],
            g["bot_fours"], g["bot_sixes"], g["extras"], g["powerplay_overs"], g["is_powerplay"],
            g["weather_condition"], g["pitch_condition"], g["created_at"], g["updated_at"]
        ))

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
# Enhanced Keyboards
# ======================================================

def kb_main_menu() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🏏 Quick Play", callback_data="quick_play"),
        types.InlineKeyboardButton("⚙️ Custom Match", callback_data="custom_match")
    )
    kb.add(
        types.InlineKeyboardButton("🏆 Tournament", callback_data="tournament"),
        types.InlineKeyboardButton("🎯 Challenges", callback_data="challenges")
    )
    kb.add(
        types.InlineKeyboardButton("📊 My Stats", callback_data="my_stats"),
        types.InlineKeyboardButton("🥇 Leaderboard", callback_data="leaderboard")
    )
    kb.add(
        types.InlineKeyboardButton("🏅 Achievements", callback_data="achievements"),
        types.InlineKeyboardButton("ℹ️ Help", callback_data="help")
    )
    return kb

def kb_difficulty_select() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    for diff, settings in DIFFICULTY_SETTINGS.items():
        kb.add(types.InlineKeyboardButton(
            f"{'🟢' if diff == 'easy' else '🟡' if diff == 'medium' else '🔴' if diff == 'hard' else '⚫'} {diff.title()}",
            callback_data=f"diff_{diff}"
        ))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="back_main"))
    return kb

def kb_format_select() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    formats = [
        ("🏃 T1 (1 over)", "format_1_1"),
        ("⚡ T2 (2 overs)", "format_2_1"), 
        ("🎯 T5 (5 overs)", "format_5_2"),
        ("🏏 T10 (10 overs)", "format_10_3"),
        ("🏆 T20 (20 overs)", "format_20_5"),
        ("🎲 Random Format", "format_random")
    ]
    for text, callback in formats:
        kb.add(types.InlineKeyboardButton(text, callback_data=callback))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="back_main"))
    return kb

def kb_toss_choice() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton("🪙 Heads", callback_data="toss_heads"),
        types.InlineKeyboardButton("🪙 Tails", callback_data="toss_tails")
    )
    return kb

def kb_bat_bowl_choice() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton("🏏 Bat First", callback_data="choose_bat"),
        types.InlineKeyboardButton("🎯 Bowl First", callback_data="choose_bowl")
    )
    return kb

def kb_batting_numbers() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=3, one_time_keyboard=False)
    row1 = [types.KeyboardButton("1️⃣"), types.KeyboardButton("2️⃣"), types.KeyboardButton("3️⃣")]
    row2 = [types.KeyboardButton("4️⃣"), types.KeyboardButton("5️⃣"), types.KeyboardButton("6️⃣")]
    kb.add(*row1)
    kb.add(*row2)
    # Add special buttons
    kb.add(types.KeyboardButton("📊 Score"), types.KeyboardButton("🏳️ Forfeit"))
    return kb

def kb_match_actions() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=3)
    kb.add(
        types.InlineKeyboardButton("📊 Score", callback_data="live_score"),
        types.InlineKeyboardButton("📈 Stats", callback_data="live_stats"),
        types.InlineKeyboardButton("💬 Commentary", callback_data="commentary")
    )
    kb.add(
        types.InlineKeyboardButton("🏳️ Forfeit", callback_data="forfeit_confirm"),
        types.InlineKeyboardButton("⏸️ Pause", callback_data="pause_match")
    )
    return kb

def kb_post_match() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🔄 Play Again", callback_data="play_again"),
        types.InlineKeyboardButton("📊 Match Summary", callback_data="match_summary")
    )
    kb.add(
        types.InlineKeyboardButton("🏆 View Stats", callback_data="my_stats"),
        types.InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
    )
    return kb

# ======================================================
# Enhanced Game Flow
# ======================================================

def start_new_game(chat_id: int, overs: int = DEFAULT_OVERS, wickets: int = DEFAULT_WICKETS, 
                   difficulty: str = "medium", user_id: int = None):
    g = default_game(overs, wickets, difficulty)
    save_game(chat_id, g)
    
    # Get weather and pitch info
    weather = WEATHER_CONDITIONS[g["weather_condition"]]
    pitch = PITCH_CONDITIONS[g["pitch_condition"]]
    
    match_info = (
        f"🏏 <b>New Match Started!</b>\n\n"
        f"📋 <b>Match Details:</b>\n"
        f"Format: <b>{g['match_format']} ({g['overs_limit']} over{'s' if g['overs_limit'] > 1 else ''}, "
        f"{g['wickets_limit']} wicket{'s' if g['wickets_limit'] > 1 else ''})</b>\n"
        f"Difficulty: <b>{difficulty.title()}</b> {DIFFICULTY_SETTINGS[difficulty]['description']}\n"
        f"Powerplay: <b>{g['powerplay_overs']} over{'s' if g['powerplay_overs'] > 1 else ''}</b>\n\n"
        f"🌤️ <b>Conditions:</b>\n"
        f"Weather: {weather['description']}\n"
        f"Pitch: {pitch['description']}\n\n"
        f"🪙 <b>Time for the toss!</b> Call it:"
    )
    
    bot.send_message(chat_id, match_info, reply_markup=kb_toss_choice())
    log_event(chat_id, "match_start", 
              f"format={g['match_format']} difficulty={difficulty} user={user_id}")

def set_batting_order(chat_id: int, first_batting: str):
    g = load_game(chat_id)
    if not g:
        return
    
    g["state"] = "play"
    g["batting"] = first_batting
    g["is_powerplay"] = True
    save_game(chat_id, g)
    
    powerplay_text = f"⚡ <b>Powerplay active</b> (first {g['powerplay_overs']} overs)" if g['powerplay_overs'] > 0 else ""
    
    if first_batting == "player":
        msg = (
            f"🏏 <b>You're batting first!</b>\n\n"
            f"{powerplay_text}\n"
            f"Send a number 1️⃣-6️⃣ to play each ball.\n"
            f"Match the bot's number = <b>OUT!</b> ❌\n"
            f"Different numbers = <b>RUNS!</b> ✅"
        )
    else:
        msg = (
            f"🎯 <b>Bot batting first! You're bowling.</b>\n\n"
            f"{powerplay_text}\n"
            f"Send a number 1️⃣-6️⃣ to bowl each ball.\n"
            f"Match the bot's choice = <b>WICKET!</b> ✅\n"
            f"Different numbers = Bot scores runs ❌"
        )
    
    bot.send_message(chat_id, msg, reply_markup=kb_batting_numbers())

def check_powerplay_status(g: Dict[str, Any]) -> bool:
    """Check if powerplay should end"""
    if g["is_powerplay"] and g["overs_bowled"] >= g["powerplay_overs"]:
        g["is_powerplay"] = False
        return True  # Powerplay just ended
    return False

def check_over_completion(g: Dict[str, Any]) -> bool:
    """Check and handle over completion"""
    if g["balls_in_over"] >= 6:
        g["balls_in_over"] = 0
        g["overs_bowled"] += 1
        return True
    return False

def check_innings_end(g: Dict[str, Any]) -> bool:
    """Check if current innings should end"""
    current_batting = g["batting"]
    
    # Check wickets
    if current_batting == "player" and g["player_wkts"] >= g["wickets_limit"]:
        return True
    elif current_batting == "bot" and g["bot_wkts"] >= g["wickets_limit"]:
        return True
    
    # Check overs
    if g["overs_bowled"] >= g["overs_limit"]:
        return True
    
    # Check target achieved in 2nd innings
    if g["innings"] == 2 and g["target"]:
        current_score = g["player_score"] if current_batting == "player" else g["bot_score"]
        if current_score > g["target"]:
            return True
    
    return False

def process_ball(chat_id: int, user_value: int):
    """Enhanced ball processing with commentary and stats"""
    g = load_game(chat_id)
    if not g or g["state"] != "play":
        return
    
    if not (1 <= user_value <= 6):
        bot.send_message(chat_id, "❌ Please send a number between 1️⃣ and 6️⃣")
        return
    
    # Calculate bot's move using AI
    bot_value = calculate_bot_move(g, user_value)
    
    # Update ball counts
    g["balls_in_over"] += 1
    
    # Determine runs and wickets
    is_wicket = (user_value == bot_value)
    runs_scored = 0
    
    if g["batting"] == "player":
        g["player_balls_faced"] += 1
        if is_wicket:
            g["player_wkts"] += 1
        else:
            runs_scored = user_value
            g["player_score"] += runs_scored
            if runs_scored == 4:
                g["player_fours"] += 1
            elif runs_scored == 6:
                g["player_sixes"] += 1
    else:
        g["bot_balls_faced"] += 1
        if is_wicket:
            g["bot_wkts"] += 1
        else:
            runs_scored = bot_value
            g["bot_score"] += runs_scored
            if runs_scored == 4:
                g["bot_fours"] += 1
            elif runs_scored == 6:
                g["bot_sixes"] += 1
    
    # Generate commentary
    commentary = get_commentary(g, user_value, bot_value, runs_scored, is_wicket)
    
    # Send appropriate animation
    if is_wicket:
        send_cricket_animation(chat_id, "wicket", commentary)
    elif runs_scored == 6:
        send_cricket_animation(chat_id, "six", commentary)
    elif runs_scored == 4:
        send_cricket_animation(chat_id, "four", commentary)
    else:
        bot.send_message(chat_id, commentary)
    
    # Check for over completion
    over_completed = check_over_completion(g)
    powerplay_ended = check_powerplay_status(g)
    
    # Additional messages for over completion
    if over_completed:
        over_summary = f"🏁 <b>Over {g['overs_bowled']} completed</b>"
        if powerplay_ended:
            over_summary += "\n⚡ <b>Powerplay ended</b>"
        bot.send_message(chat_id, over_summary)
    
    # Check for milestone achievements
    check_milestones(chat_id, g)
    
    # Save ball-by-ball data
    save_ball_data(chat_id, g, user_value, bot_value, runs_scored, is_wicket, commentary)
    
    # Check if innings/match should end
    if check_innings_end(g):
        save_game(chat_id, g)
        end_innings_or_match(chat_id)
        return
    
    # Save game state and show current status
    save_game(chat_id, g)
    show_live_score(chat_id, g, detailed=False)

def check_milestones(chat_id: int, g: Dict[str, Any]):
    """Check and announce milestones"""
    current_batting = g["batting"]
    current_score = g["player_score"] if current_batting == "player" else g["bot_score"]
    
    # Check for fifty
    if current_score == 50 and current_batting == "player":
        send_cricket_animation(chat_id, "fifty", "🎉 FIFTY! Well played!")
    
    # Check for century
    elif current_score == 100 and current_batting == "player":
        send_cricket_animation(chat_id, "century", "💯 CENTURY! Outstanding innings!")

def save_ball_data(chat_id: int, g: Dict[str, Any], user_val: int, bot_val: int, 
                   runs: int, wicket: bool, commentary: str):
    """Save detailed ball-by-ball data"""
    with db_conn() as db:
        db.execute("""
            INSERT INTO ball_by_ball 
            (chat_id, innings, over_number, ball_number, batting_team, 
             batsman_score, bowler_score, runs_scored, is_wicket, commentary, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            chat_id, g["innings"], g["overs_bowled"], g["balls_in_over"],
            g["batting"], user_val, bot_val, runs, wicket, commentary,
            datetime.now(timezone.utc).isoformat()
        ))

def show_live_score(chat_id: int, g: Dict[str, Any], detailed: bool = True):
    """Display current match status"""
    # Calculate required run rate for chases
    req_rate_text = ""
    if g["innings"] == 2 and g["target"]:
        balls_left = (g["overs_limit"] - g["overs_bowled"]) * 6 - g["balls_in_over"]
        runs_needed = g["target"] - (g["player_score"] if g["batting"] == "player" else g["bot_score"]) + 1
        
        if balls_left > 0:
            req_rate = (runs_needed * 6) / balls_left
            req_rate_text = f"Required Rate: <b>{req_rate:.1f}</b> per over"
    
    score_text = (
        f"📊 <b>Live Score</b>\n\n"
        f"🏏 You: <b>{g['player_score']}/{g['player_wkts']}</b> "
        f"({g['player_balls_faced']} balls)\n"
        f"🤖 Bot: <b>{g['bot_score']}/{g['bot_wkts']}</b> "
        f"({g['bot_balls_faced']} balls)\n\n"
        f"🎯 Innings: <b>{g['innings']}</b> | "
        f"Batting: <b>{'You' if g['batting'] == 'player' else 'Bot'}</b>\n"
        f"⏱️ Over: <b>{g['overs_bowled']}.{g['balls_in_over']}</b> / {g['overs_limit']}"
    )
    
    if g["is_powerplay"]:
        score_text += " ⚡"
    
    if g["target"]:
        target_team = "You" if g["batting"] == "player" else "Bot"
        score_text += f"\n🎯 Target: <b>{g['target'] + 1}</b> for {target_team}"
        if req_rate_text:
            score_text += f"\n{req_rate_text}"
    
    if detailed:
        # Add boundaries info
        if g["batting"] == "player":
            score_text += f"\n🏏 Boundaries: {g['player_fours']}×4️⃣ {g['player_sixes']}×6️⃣"
        else:
            score_text += f"\n🤖 Boundaries: {g['bot_fours']}×4️⃣ {g['bot_sixes']}×6️⃣"
    
    bot.send_message(chat_id, score_text)

def end_innings_or_match(chat_id: int):
    """Handle innings break or match completion"""
    g = load_game(chat_id)
    if not g:
        return
    
    if g["innings"] == 1:
        # End of first innings
        start_second_innings(chat_id, g)
    else:
        # End of match
        complete_match(chat_id, g)

def start_second_innings(chat_id: int, g: Dict[str, Any]):
    """Start the second innings"""
    first_innings_summary = ""
    
    if g["batting"] == "player":
        g["target"] = g["player_score"]
        g["batting"] = "bot"
        first_innings_summary = (
            f"🏁 <b>First Innings Complete!</b>\n\n"
            f"🏏 Your Score: <b>{g['player_score']}/{g['player_wkts']}</b>\n"
            f"⏱️ Overs: <b>{g['overs_bowled']}.{g['balls_in_over']}</b>\n"
            f"🏏 Boundaries: {g['player_fours']}×4️⃣ {g['player_sixes']}×6️⃣\n\n"
            f"🎯 <b>Target for Bot: {g['target'] + 1}</b>\n\n"
            f"🎯 <b>Second Innings</b>\n"
            f"Bot is batting now. Bowl to defend your total!"
        )
    else:
        g["target"] = g["bot_score"]
        g["batting"] = "player"
        first_innings_summary = (
            f"🏁 <b>First Innings Complete!</b>\n\n"
            f"🤖 Bot's Score: <b>{g['bot_score']}/{g['bot_wkts']}</b>\n"
            f"⏱️ Overs: <b>{g['overs_bowled']}.{g['balls_in_over']}</b>\n"
            f"🤖 Boundaries: {g['bot_fours']}×4️⃣ {g['bot_sixes']}×6️⃣\n\n"
            f"🎯 <b>Target for You: {g['target'] + 1}</b>\n\n"
            f"🏏 <b>Second Innings</b>\n"
            f"Your turn to chase! Good luck!"
        )
    
    # Reset for second innings
    g["innings"] = 2
    g["balls_in_over"] = 0
    g["overs_bowled"] = 0
    g["is_powerplay"] = True if g["powerplay_overs"] > 0 else False
    
    save_game(chat_id, g)
    bot.send_message(chat_id, first_innings_summary, reply_markup=kb_batting_numbers())

def complete_match(chat_id: int, g: Dict[str, Any]):
    """Complete the match and show results"""
    player_score, bot_score = g["player_score"], g["bot_score"]
    
    # Determine winner
    if player_score > bot_score:
        result = "win"
        margin = player_score - bot_score
        margin_text = f"by {margin} runs"
        result_emoji = "🏆"
        result_text = "YOU WIN!"
        send_cricket_animation(chat_id, "win", "🎉 Congratulations! You won!")
    elif bot_score > player_score:
        result = "loss"
        wickets_left = g["wickets_limit"] - g["bot_wkts"]
        margin_text = f"by {wickets_left} wickets" if wickets_left > 0 else "on last ball"
        result_emoji = "😔"
        result_text = "BOT WINS!"
        send_cricket_animation(chat_id, "lose", "😔 Better luck next time!")
    else:
        result = "tie"
        margin_text = "Match Tied!"
        result_emoji = "🤝"
        result_text = "IT'S A TIE!"
        send_cricket_animation(chat_id, "tie", "🤝 What a thrilling tie!")
    
    # Calculate match statistics
    match_summary = generate_match_summary(g, result, margin_text)
    
    # Save match to history and update user stats
    save_match_history(chat_id, g, result, margin_text)
    
    # Show final summary
    final_message = (
        f"🏁 <b>MATCH OVER</b>\n\n"
        f"{result_emoji} <b>{result_text}</b>\n"
        f"Margin: <b>{margin_text}</b>\n\n"
        f"{match_summary}\n\n"
        f"Well played! 🏏"
    )
    
    bot.send_message(chat_id, final_message, reply_markup=kb_post_match())
    
    # Clean up game state
    delete_game(chat_id)
    
    # Check and award achievements
    check_achievements(chat_id, g, result)

def generate_match_summary(g: Dict[str, Any], result: str, margin: str) -> str:
    """Generate detailed match summary"""
    summary = (
        f"📋 <b>Match Summary</b>\n\n"
        f"🏏 <b>Your Innings:</b> {g['player_score']}/{g['player_wkts']}\n"
        f"   Balls: {g['player_balls_faced']} | "
        f"4s: {g['player_fours']} | 6s: {g['player_sixes']}\n"
        f"   Strike Rate: {(g['player_score']/max(g['player_balls_faced'], 1)*100):.1f}\n\n"
        f"🤖 <b>Bot's Innings:</b> {g['bot_score']}/{g['bot_wkts']}\n"
        f"   Balls: {g['bot_balls_faced']} | "
        f"4s: {g['bot_fours']} | 6s: {g['bot_sixes']}\n"
        f"   Strike Rate: {(g['bot_score']/max(g['bot_balls_faced'], 1)*100):.1f}"
    )
    
    return summary

def save_match_history(chat_id: int, g: Dict[str, Any], result: str, margin: str):
    """Save match to history and update user stats"""
    with db_conn() as db:
        # Get the last user who played
        cur = db.execute("""
            SELECT meta FROM history 
            WHERE chat_id=? AND event='ball_input' 
            ORDER BY id DESC LIMIT 1
        """, (chat_id,))
        
        row = cur.fetchone()
        user_id = None
        
        if row and row["meta"]:
            try:
                # Extract user_id from meta like "from=123456 n=4"
                parts = dict(kv.split("=") for kv in row["meta"].split())
                user_id = int(parts.get("from", "0"))
            except:
                pass
        
        if user_id:
            # Calculate match duration (estimate)
            total_balls = g["player_balls_faced"] + g["bot_balls_faced"]
            duration_minutes = max(1, total_balls // 12)  # Rough estimate
            
            # Save match history
            now = datetime.now(timezone.utc).isoformat()
            db.execute("""
                INSERT INTO match_history (
                    chat_id, user_id, match_format, player_score, bot_score,
                    player_wickets, bot_wickets, overs_played, result, margin,
                    player_strike_rate, match_duration_minutes, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                chat_id, user_id, g["match_format"], g["player_score"], g["bot_score"],
                g["player_wkts"], g["bot_wkts"], 
                g["overs_bowled"] + (g["balls_in_over"]/6.0),
                result, margin,
                (g["player_score"]/max(g["player_balls_faced"], 1)*100),
                duration_minutes, now
            ))
            
            # Update user stats
            update_user_stats(user_id, g, result)

def update_user_stats(user_id: int, g: Dict[str, Any], result: str):
    """Update comprehensive user statistics"""
    with db_conn() as db:
        now = datetime.now(timezone.utc).isoformat()
        
        # Basic game result updates
        if result == "win":
            db.execute("UPDATE stats SET wins = wins + 1 WHERE user_id = ?", (user_id,))
        elif result == "loss":
            db.execute("UPDATE stats SET losses = losses + 1 WHERE user_id = ?", (user_id,))
        else:
            db.execute("UPDATE stats SET ties = ties + 1 WHERE user_id = ?", (user_id,))
        
        # Comprehensive stats update
        db.execute("""
            UPDATE stats SET 
                games_played = games_played + 1,
                total_runs = total_runs + ?,
                total_balls_faced = total_balls_faced + ?,
                sixes_hit = sixes_hit + ?,
                fours_hit = fours_hit + ?,
                high_score = MAX(high_score, ?),
                updated_at = ?
            WHERE user_id = ?
        """, (
            g["player_score"], g["player_balls_faced"], g["player_sixes"],
            g["player_fours"], g["player_score"], now, user_id
        ))
        
        # Calculate and update averages
        db.execute("""
            UPDATE stats SET 
                avg_score = CAST(total_runs AS REAL) / NULLIF(games_played, 0),
                strike_rate = CAST(total_runs AS REAL) * 100.0 / NULLIF(total_balls_faced, 0)
            WHERE user_id = ?
        """, (user_id,))

def check_achievements(chat_id: int, g: Dict[str, Any], result: str):
    """Check and award achievements"""
    # This would implement the achievement system
    # For brevity, showing structure only
    pass

# ======================================================
# Enhanced Command Handlers  
# ======================================================

@bot.message_handler(commands=["start"])
def cmd_start(message: types.Message):
    logger.info(f"User {message.from_user.id} started the bot")
    ensure_user(message)
    
    welcome_text = (
        f"🏏 <b>Welcome to Cricket Bot, {message.from_user.first_name}!</b>\n\n"
        f"🎮 The most advanced hand-cricket experience on Telegram!\n\n"
        f"✨ <b>Features:</b>\n"
        f"• 🎯 Multiple game formats (T1 to T20)\n"
        f"• 🤖 Smart AI opponents\n" 
        f"• 🏆 Tournaments & Leaderboards\n"
        f"• 🏅 Achievements & Challenges\n"
        f"• 📊 Detailed statistics\n"
        f"• 🎬 Live commentary\n\n"
        f"Ready to play some cricket? 🏏"
    )
    
    bot.send_message(message.chat.id, welcome_text, reply_markup=kb_main_menu())

@bot.message_handler(commands=["help"])  
def cmd_help(message: types.Message):
    ensure_user(message)
    
    help_text = (
        f"🏏 <b>Cricket Bot Help</b>\n\n"
        f"<b>📖 How to Play:</b>\n"
        f"• Choose numbers 1-6 for each ball\n"
        f"• Same numbers = OUT! ❌\n"
        f"• Different numbers = RUNS! ✅\n\n"
        f"<b>🎮 Game Modes:</b>\n"
        f"• Quick Play - instant T2 match\n"
        f"• Custom Match - choose format & difficulty\n"
        f"• Tournament - compete with others\n\n"
        f"<b>⚡ Commands:</b>\n"
        f"/play - Start quick match\n"
        f"/stats - Your statistics  \n"
        f"/leaderboard - Top players\n"
        f"/achievements - Your achievements\n"
        f"/challenges - Daily challenges\n"
        f"/settings - Bot preferences\n\n"
        f"<b>🎯 Pro Tips:</b>\n"
        f"• Use powerplay overs wisely\n"
        f"• Watch the required run rate\n"
        f"• Different difficulties change bot behavior\n"
        f"• Complete challenges for extra points!"
    )
    
    bot.send_message(message.chat.id, help_text)

@bot.message_handler(commands=["play"])
def cmd_quick_play(message: types.Message):
    logger.info(f"Quick play requested by user {message.from_user.id}")
    ensure_user(message)
    
    # Check if there's already an active game
    existing_game = load_game(message.chat.id)
    if existing_game and existing_game["state"] in ["toss", "play"]:
        bot.send_message(message.chat.id, 
                        "⚠️ You have an active match! Use /forfeit to abandon it, or continue playing.",
                        reply_markup=kb_match_actions())
        return
    
    start_new_game(message.chat.id, 2, 1, "medium", message.from_user.id)

@bot.message_handler(commands=["stats"])
def cmd_stats(message: types.Message):
    ensure_user(message)
    show_user_stats(message.chat.id, message.from_user.id)

@bot.message_handler(commands=["leaderboard", "top"])
def cmd_leaderboard(message: types.Message):
    ensure_user(message)
    show_leaderboard(message.chat.id)

@bot.message_handler(commands=["achievements"])
def cmd_achievements(message: types.Message):
    ensure_user(message)
    show_achievements(message.chat.id, message.from_user.id)

@bot.message_handler(commands=["score"])
def cmd_live_score(message: types.Message):
    ensure_user(message)
    g = load_game(message.chat.id)
    if not g or g["state"] != "play":
        bot.send_message(message.chat.id, "❌ No active match found. Start one with /play")
        return
    show_live_score(message.chat.id, g, detailed=True)

@bot.message_handler(commands=["forfeit"])
def cmd_forfeit(message: types.Message):
    ensure_user(message)
    g = load_game(message.chat.id)
    if not g or g["state"] == "finished":
        bot.send_message(message.chat.id, "❌ No active match to forfeit.")
        return
    
    delete_game(message.chat.id)
    bot.send_message(message.chat.id, "🏳️ Match forfeited. Use /play to start a new match.")

# Admin commands
@bot.message_handler(commands=["admin"])
def cmd_admin(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        bot.send_message(message.chat.id, "❌ Access denied.")
        return
    
    with db_conn() as db:
        # Get bot statistics
        user_count = db.execute("SELECT COUNT(*) as count FROM users").fetchone()["count"]
        game_count = db.execute("SELECT COUNT(*) as count FROM match_history").fetchone()["count"] 
        active_games = db.execute("SELECT COUNT(*) as count FROM games").fetchone()["count"]
    
    admin_text = (
        f"🔧 <b>Admin Panel</b>\n\n"
        f"📊 <b>Statistics:</b>\n"
        f"• Users: {user_count}\n"
        f"• Total Matches: {game_count}\n"
        f"• Active Matches: {active_games}\n\n"
        f"<b>Commands:</b>\n"
        f"/broadcast [message] - Send to all users\n"
        f"/cleanup - Clean old data\n"
        f"/backup - Backup database"
    )
    
    bot.send_message(message.chat.id, admin_text)

def ensure_user(message: types.Message):
    if message.from_user:
        upsert_user(message.from_user)

# ======================================================
# Stats and Leaderboard Functions
# ======================================================

def show_user_stats(chat_id: int, user_id: int):
    with db_conn() as db:
        cur = db.execute("SELECT * FROM stats WHERE user_id=?", (user_id,))
        stats = cur.fetchone()
        
        if not stats or stats["games_played"] == 0:
            bot.send_message(chat_id, "📊 No statistics yet! Play your first match with /play")
            return
        
        # Calculate win percentage
        win_rate = (stats["wins"] / stats["games_played"] * 100) if stats["games_played"] > 0 else 0
        
        # Get recent form (last 5 matches)
        cur = db.execute("""
            SELECT result FROM match_history 
            WHERE user_id=? ORDER BY id DESC LIMIT 5
        """, (user_id,))
        recent = cur.fetchall()
        form = "".join("W" if r["result"] == "win" else "L" if r["result"] == "loss" else "T" for r in recent)
        
        stats_text = (
            f"📊 <b>Your Cricket Stats</b>\n\n"
            f"🎮 <b>Matches:</b> {stats['games_played']}\n"
            f"🏆 Wins: {stats['wins']} ({win_rate:.1f}%)\n"
            f"😔 Losses: {stats['losses']}\n"
            f"🤝 Ties: {stats['ties']}\n\n"
            f"🏏 <b>Batting:</b>\n"
            f"• High Score: {stats['high_score']}\n"
            f"• Average: {stats['avg_score']:.1f}\n"
            f"• Strike Rate: {stats['strike_rate']:.1f}\n"
            f"• Total Runs: {stats['total_runs']}\n\n"
            f"🎯 <b>Milestones:</b>\n"
            f"• Centuries: {stats['centuries']}\n"
            f"• Fifties: {stats['fifties']}\n"
            f"• Sixes Hit: {stats['sixes_hit']}\n"
            f"• Fours Hit: {stats['fours_hit']}\n\n"
            f"🔥 <b>Streaks:</b>\n"
            f"• Best Streak: {stats['longest_winning_streak']}\n"
            f"• Current: {stats['current_winning_streak']}\n"
            f"• Recent Form: {form or 'N/A'}"
        )
        
        bot.send_message(chat_id, stats_text)

def show_leaderboard(chat_id: int, category: str = "wins"):
    with db_conn() as db:
        if category == "wins":
            query = """
                SELECT u.first_name, u.username, s.wins, s.games_played, s.high_score
                FROM stats s JOIN users u ON u.user_id = s.user_id
                WHERE s.games_played >= 5
                ORDER BY s.wins DESC, s.high_score DESC
                LIMIT 10
            """
        elif category == "average":
            query = """
                SELECT u.first_name, u.username, s.avg_score, s.games_played, s.wins
                FROM stats s JOIN users u ON u.user_id = s.user_id  
                WHERE s.games_played >= 5
                ORDER BY s.avg_score DESC
                LIMIT 10
            """
        else:  # high_score
            query = """
                SELECT u.first_name, u.username, s.high_score, s.games_played, s.wins
                FROM stats s JOIN users u ON u.user_id = s.user_id
                ORDER BY s.high_score DESC
                LIMIT 10
            """
        
        cur = db.execute(query)
        players = cur.fetchall()
        
        if not players:
            bot.send_message(chat_id, "🏆 No players on leaderboard yet! Be the first to play 5+ matches!")
            return
        
        category_title = {"wins": "Most Wins", "average": "Best Average", "high_score": "Highest Scores"}
        
        leaderboard_text = f"🏆 <b>Leaderboard - {category_title.get(category, 'Top Players')}</b>\n\n"
        
        for i, player in enumerate(players, 1):
            name = player["first_name"] or (f"@{player['username']}" if player["username"] else "Anonymous")
            
            if category == "wins":
                stat = f"{player['wins']} wins"
            elif category == "average":
                stat = f"{player['avg_score']:.1f} avg"
            else:
                stat = f"{player['high_score']} runs"
            
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            leaderboard_text += f"{medal} {name} - {stat}\n"
        
        # Add category selection buttons
        kb = types.InlineKeyboardMarkup(row_width=3)
        kb.add(
            types.InlineKeyboardButton("🏆 Wins", callback_data="lb_wins"),
            types.InlineKeyboardButton("📊 Average", callback_data="lb_average"), 
            types.InlineKeyboardButton("🎯 High Score", callback_data="lb_high_score")
        )
        
        bot.send_message(chat_id, leaderboard_text, reply_markup=kb)

def show_achievements(chat_id: int, user_id: int):
    with db_conn() as db:
        # Get user's unlocked achievements
        cur = db.execute("""
            SELECT a.name, a.description, a.icon, a.points, ua.unlocked_at
            FROM achievements a
            JOIN user_achievements ua ON a.id = ua.achievement_id  
            WHERE ua.user_id = ?
            ORDER BY ua.unlocked_at DESC
        """, (user_id,))
        unlocked = cur.fetchall()
        
        # Get total achievements
        total_achievements = db.execute("SELECT COUNT(*) as count FROM achievements").fetchone()["count"]
        unlocked_count = len(unlocked)
        
        if unlocked_count == 0:
            achievements_text = (
                f"🏅 <b>Achievements</b>\n\n"
                f"🎯 Unlocked: 0/{total_achievements}\n\n"
                f"Play matches to unlock achievements!\n"
                f"Try winning your first match, scoring a century, or hitting boundaries!"
            )
        else:
            total_points = sum(a["points"] for a in unlocked)
            achievements_text = (
                f"🏅 <b>Your Achievements</b>\n\n"
                f"🎯 Unlocked: {unlocked_count}/{total_achievements}\n"
                f"⭐ Total Points: {total_points}\n\n"
            )
            
            for achievement in unlocked[:10]:  # Show latest 10
                date = datetime.fromisoformat(achievement["unlocked_at"]).strftime("%b %d")
                achievements_text += (
                    f"{achievement['icon']} <b>{achievement['name']}</b>\n"
                    f"   {achievement['description']}\n"
                    f"   +{achievement['points']} points • {date}\n\n"
                )
            
            if unlocked_count > 10:
                achievements_text += f"... and {unlocked_count - 10} more!"
        
        bot.send_message(chat_id, achievements_text)

# ======================================================
# Callback Query Handlers
# ======================================================

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call: types.CallbackQuery):
    try:
        data = call.data
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Main menu navigation
        if data == "quick_play":
            bot.answer_callback_query(call.id)
            start_new_game(chat_id, 2, 1, "medium", user_id)
            
        elif data == "custom_match":
            bot.answer_callback_query(call.id)
            bot.edit_message_text("🎮 Choose your match format:", chat_id, call.message.message_id,
                                reply_markup=kb_format_select())
            
        elif data.startswith("format_"):
            bot.answer_callback_query(call.id)
            if data == "format_random":
                overs = random.randint(1, 10)
                wickets = random.randint(1, 3)
            else:
                parts = data.split("_")
                overs, wickets = int(parts[1]), int(parts[2])
            
            # Show difficulty selection
            bot.edit_message_text(f"🎯 Format: T{overs} ({wickets} wickets)\nChoose difficulty:",
                                chat_id, call.message.message_id, reply_markup=kb_difficulty_select())
            # Store format in user session (simplified - you might want to use proper session storage)
            
        elif data.startswith("diff_"):
            bot.answer_callback_query(call.id)
            difficulty = data.split("_")[1]
            # For demo, using default format - in real implementation, get from stored session
            start_new_game(chat_id, 5, 2, difficulty, user_id)
            
        elif data == "my_stats":
            bot.answer_callback_query(call.id)
            show_user_stats(chat_id, user_id)
            
        elif data == "leaderboard":
            bot.answer_callback_query(call.id)
            show_leaderboard(chat_id)
            
        elif data.startswith("lb_"):
            bot.answer_callback_query(call.id)
            category = data.split("_", 1)[1]
            show_leaderboard(chat_id, category)
            
        elif data == "achievements":
            bot.answer_callback_query(call.id)
            show_achievements(chat_id, user_id)
            
        # Toss handling
        elif data.startswith("toss_"):
            handle_toss(call)
            
        elif data in ["choose_bat", "choose_bowl"]:
            handle_batting_choice(call)
            
        # Match actions
        elif data == "live_score":
            bot.answer_callback_query(call.id)
            g = load_game(chat_id)
            if g and g["state"] == "play":
                show_live_score(chat_id, g, detailed=True)
            
        elif data == "forfeit_confirm":
            bot.answer_callback_query(call.id, "Match forfeited!")
            delete_game(chat_id)
            bot.edit_message_text("🏳️ Match forfeited.", chat_id, call.message.message_id)
            
        elif data == "play_again":
            bot.answer_callback_query(call.id)
            start_new_game(chat_id, 2, 1, "medium", user_id)
            
        elif data == "main_menu":
            bot.answer_callback_query(call.id)
            bot.edit_message_text("🏏 <b>Cricket Bot</b> - Main Menu", 
                                chat_id, call.message.message_id, reply_markup=kb_main_menu())
        
        else:
            bot.answer_callback_query(call.id, "Feature coming soon!")
            
    except Exception as e:
        logger.error(f"Error in callback handler: {e}")
        bot.answer_callback_query(call.id, "❌ Something went wrong!")

def handle_toss(call: types.CallbackQuery):
    chat_id = call.message.chat.id
    g = load_game(chat_id)
    
    if not g or g["state"] != "toss":
        bot.answer_callback_query(call.id, "❌ Invalid game state")
        return
    
    user_choice = call.data.split("_")[1]  # heads or tails
    coin_result = random.choice(["heads", "tails"])
    won_toss = (user_choice == coin_result)
    
    if won_toss:
        toss_text = (
            f"🪙 <b>Toss Result:</b> {coin_result.title()}\n"
            f"🎉 <b>You won the toss!</b>\n\n"
            f"Choose what you want to do:"
        )
        markup = kb_bat_bowl_choice()
    else:
        # Bot chooses (randomly for now, could be strategic)
        bot_choice = random.choice(["bat", "bowl"])
        toss_text = (
            f"🪙 <b>Toss Result:</b> {coin_result.title()}\n"
            f"😔 <b>You lost the toss.</b>\n\n"
            f"🤖 Bot chose to <b>{bot_choice} first</b>."
        )
        markup = None
        
        # Set batting order immediately
        first_batting = "bot" if bot_choice == "bat" else "player"
        set_batting_order(chat_id, first_batting)
    
    bot.edit_message_text(toss_text, chat_id, call.message.message_id, reply_markup=markup)
    bot.answer_callback_query(call.id)

def handle_batting_choice(call: types.CallbackQuery):
    chat_id = call.message.chat.id
    choice = call.data.split("_")[1]  # bat or bowl
    
    first_batting = "player" if choice == "bat" else "bot"
    
    choice_text = (
        f"✅ <b>You chose to {choice} first!</b>\n\n"
        f"{'🏏 Get ready to bat!' if choice == 'bat' else '🎯 Get ready to bowl!'}"
    )
    
    bot.edit_message_text(choice_text, chat_id, call.message.message_id)
    bot.answer_callback_query(call.id)
    
    # Start the match
    set_batting_order(chat_id, first_batting)

# ======================================================
# Text Message Handler
# ======================================================

@bot.message_handler(content_types=["text"])
def handle_text_messages(message: types.Message):
    try:
        ensure_user(message)
        text = message.text.strip()
        
        # Handle numeric input (1-6 for cricket)
        if text.isdigit():
            number = int(text)
            if 1 <= number <= 6:
                log_event(message.chat.id, "ball_input", f"from={message.from_user.id} n={number}")
                process_ball(message.chat.id, number)
                return
            else:
                bot.reply_to(message, "🎯 Please send a number between 1️⃣ and 6️⃣")
                return
        
        # Handle emoji numbers
        emoji_to_num = {
            "1️⃣": 1, "2️⃣": 2, "3️⃣": 3, "4️⃣": 4, "5️⃣": 5, "6️⃣": 6
        }
        if text in emoji_to_num:
            number = emoji_to_num[text]
            log_event(message.chat.id, "ball_input", f"from={message.from_user.id} n={number}")
            process_ball(message.chat.id, number)
            return
        
        # Handle quick commands through text
        text_lower = text.lower()
        
        if text_lower in ["score", "📊 score"]:
            g = load_game(message.chat.id)
            if g and g["state"] == "play":
                show_live_score(message.chat.id, g)
            else:
                bot.reply_to(message, "❌ No active match. Start one with /play")
        
        elif text_lower in ["forfeit", "🏳️ forfeit", "quit"]:
            g = load_game(message.chat.id)
            if g:
                delete_game(message.chat.id)
                bot.reply_to(message, "🏳️ Match forfeited. Use /play for a new match.")
            else:
                bot.reply_to(message, "❌ No active match to forfeit.")
        
        elif text_lower in ["help", "?", "commands"]:
            cmd_help(message)
        
        elif text_lower in ["play", "start match", "new game"]:
            cmd_quick_play(message)
        
        elif text_lower in ["stats", "my stats", "statistics"]:
            show_user_stats(message.chat.id, message.from_user.id)
        
        elif text_lower in ["leaderboard", "top", "rankings"]:
            show_leaderboard(message.chat.id)
        
        # Fun cricket responses
        elif any(word in text_lower for word in ["cricket", "bat", "bowl", "wicket", "six", "four"]):
            responses = [
                "🏏 Love the cricket spirit! Ready for a match?",
                "🎯 Cricket talk! Want to play a quick game?", 
                "⚡ That's the cricket fever! /play to start!",
                "🏆 Cricket fan detected! Let's play!"
            ]
            bot.reply_to(message, random.choice(responses), reply_markup=kb_main_menu())
        
        # Default response for unrecognized text
        else:
            bot.reply_to(message, 
                        "🏏 I didn't understand that! Use /help for commands or /play to start a match.",
                        reply_markup=kb_main_menu())
    
    except Exception as e:
        logger.error(f"Error handling text message: {e}")
        bot.reply_to(message, "❌ Something went wrong! Try /help for assistance.")

# ======================================================
# Flask Webhook Application
# ======================================================

app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        if request.headers.get('content-type') == 'application/json':
            json_string = request.get_data().decode('utf-8')
            update = telebot.types.Update.de_json(json_string)
            logger.info(f"Received webhook update: {update.update_id}")
            bot.process_new_updates([update])
            return "OK", 200
        else:
            logger.warning("Webhook received non-JSON content")
            return "Bad Request", 400
    except Exception as e:
        logger.exception(f"Error processing webhook: {e}")
        return "Internal Server Error", 500

@app.route("/", methods=["GET"])
def index():
    return """
    <html>
        <head><title>Cricket Bot</title></head>
        <body style="font-family: Arial; text-align: center; padding: 50px;">
            <h1>🏏 Cricket Bot is Running!</h1>
            <p>The most advanced hand-cricket bot on Telegram</p>
            <p>Status: <span style="color: green;">✅ Online</span></p>
        </body>
    </html>
    """, 200

@app.route("/health", methods=["GET"])
def health():
    try:
        # Quick database check
        with db_conn() as db:
            db.execute("SELECT 1").fetchone()
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": "connected"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy", 
            "error": str(e)
        }), 500

@app.route("/stats", methods=["GET"])
def api_stats():
    """API endpoint for bot statistics"""
    try:
        with db_conn() as db:
            stats = {
                "total_users": db.execute("SELECT COUNT(*) as count FROM users").fetchone()["count"],
                "total_matches": db.execute("SELECT COUNT(*) as count FROM match_history").fetchone()["count"],
                "active_matches": db.execute("SELECT COUNT(*) as count FROM games").fetchone()["count"],
                "top_scorer": db.execute("SELECT first_name, high_score FROM stats s JOIN users u ON s.user_id = u.user_id ORDER BY high_score DESC LIMIT 1").fetchone()
            }
        return jsonify(stats), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/webhook-info", methods=["GET"])
def webhook_info():
    try:
        info = bot.get_webhook_info()
        return jsonify({
            "webhook_url": info.url,
            "pending_updates": info.pending_update_count,
            "last_error_date": info.last_error_date,
            "last_error_message": info.last_error_message
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ======================================================
# Daily Challenges System (Background Task)
# ======================================================

def create_daily_challenges():
    """Create daily challenges for users"""
    with db_conn() as db:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Check if challenges already exist for today
        existing = db.execute("SELECT id FROM daily_challenges WHERE challenge_date = ?", (today,)).fetchone()
        if existing:
            return
        
        # Create today's challenges
        challenges = [
            ("score_runs", random.randint(30, 80), 20, f"Score {random.randint(30, 80)} runs in a match"),
            ("win_matches", random.randint(2, 5), 30, f"Win {random.randint(2, 5)} matches"),
            ("hit_boundaries", random.randint(5, 15), 25, f"Hit {random.randint(5, 15)} boundaries"),
            ("play_overs", random.randint(10, 25), 15, f"Play {random.randint(10, 25)} overs total")
        ]
        
        for challenge_type, target, reward, description in challenges:
            db.execute("""
                INSERT INTO daily_challenges (challenge_date, challenge_type, target_value, reward_points, description)
                VALUES (?, ?, ?, ?, ?)
            """, (today, challenge_type, target, reward, description))

def start_background_tasks():
    """Start background tasks like daily challenges"""
    def daily_task():
        while True:
            try:
                create_daily_challenges()
                time.sleep(86400)  # 24 hours
            except Exception as e:
                logger.error(f"Error in daily task: {e}")
                time.sleep(3600)  # Retry in 1 hour
    
    thread = threading.Thread(target=daily_task, daemon=True)
    thread.start()
    logger.info("Background tasks started")

# ======================================================
# Application Bootstrap
# ======================================================

def setup_bot():
    """Setup bot configuration"""
    logger.info("Setting up Cricket Bot...")
    
    # Test bot connection
    try:
        bot_info = bot.get_me()
        logger.info(f"Bot connected: @{bot_info.username} (ID: {bot_info.id})")
    except Exception as e:
        logger.error(f"Failed to connect to bot: {e}")
        raise
    
    # Initialize database
    try:
        db_init()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    
    # Start background tasks
    start_background_tasks()
    
    logger.info("Cricket Bot setup completed successfully!")


def upsert_user(u: types.User):
    with db_conn() as db:
        now = datetime.now(timezone.utc).isoformat()
        
        try:
            # Try the full insert first
            db.execute("""
                INSERT INTO users (
                    user_id, username, first_name, last_name, language_code, 
                    is_premium, created_at, last_active, total_messages
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(user_id) DO UPDATE SET
                    username=excluded.username,
                    first_name=excluded.first_name,
                    last_name=excluded.last_name,
                    language_code=excluded.language_code,
                    is_premium=excluded.is_premium,
                    last_active=excluded.last_active,
                    total_messages=total_messages + 1
            """, (u.id, u.username, u.first_name, u.last_name, 
                  u.language_code, getattr(u, 'is_premium', False), now, now))
                  
        except sqlite3.OperationalError as e:
            if "no column named" in str(e):
                # Fallback to basic user insert for old schema
                logger.warning(f"Using basic user upsert due to schema mismatch: {e}")
                db.execute("""
                    INSERT INTO users (user_id, username, first_name, last_name)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username=excluded.username,
                        first_name=excluded.first_name,
                        last_name=excluded.last_name
                """, (u.id, u.username, u.first_name, u.last_name))
            else:
                raise
        
        try:
            db.execute("""
                INSERT INTO stats (user_id, created_at, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET updated_at=excluded.updated_at
            """, (u.id, now, now))
        except sqlite3.OperationalError:
            db.execute("INSERT OR IGNORE INTO stats (user_id) VALUES (?)", (u.id,))


            
def main():
    """Main application entry point"""
    setup_bot()
    
    if USE_WEBHOOK:
        if not WEBHOOK_URL:
            logger.error("WEBHOOK_URL not set but webhook mode enabled")
            return
        
        try:
            # Remove existing webhook
            bot.remove_webhook()
            logger.info("Removed existing webhook")
            
            # Set new webhook
            webhook_url = f"{WEBHOOK_URL.rstrip('/')}/webhook"
            result = bot.set_webhook(url=webhook_url)
            
            if result:
                logger.info(f"Webhook set successfully: {webhook_url}")
                logger.info(f"Starting Flask server on port {PORT}")
                app.run(host="0.0.0.0", port=PORT, debug=False)
            else:
                logger.error("Failed to set webhook")
                
        except Exception as e:
            logger.exception(f"Webhook setup failed: {e}")
    else:
        logger.info("Starting bot in polling mode...")
        try:
            bot.infinity_polling(timeout=20, long_polling_timeout=10)
        except Exception as e:
            logger.exception(f"Polling failed: {e}")

if __name__ == "__main__":
    main()