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
import sys
from flask import Flask, request, jsonify
import telebot
from telebot import types
from dotenv import load_dotenv
from enum import Enum
from collections import defaultdict, deque
import uuid
from functools import wraps
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Load environment variables first
load_dotenv()

# Environment / Config
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
USE_WEBHOOK = int(os.getenv("USE_WEBHOOK", "0"))  # Default to polling
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
PORT = int(os.getenv("PORT", 5000))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DB_PATH = os.getenv("DB_PATH", "cricket_bot.db")
DEFAULT_OVERS = int(os.getenv("DEFAULT_OVERS", "2"))
DEFAULT_WICKETS = int(os.getenv("DEFAULT_WICKETS", "1"))
MAX_OVERS = 20
MAX_WICKETS = 10

try:
    ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
except (ValueError, AttributeError):
    ADMIN_IDS = []

# Logging setup
logging.basicConfig(
    level=LOG_LEVEL, 
    format='[%(levelname)s] %(asctime)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("cricket-bot")

logger.info("=== MODULE LOADING STARTED ===")
logger.info(f"TOKEN present: {bool(TOKEN)}")
logger.info(f"USE_WEBHOOK: {USE_WEBHOOK}")

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set. Please set it in your environment variables or .env file")

# Initialize Bot
bot = telebot.TeleBot(TOKEN, parse_mode="HTML", threaded=True)

# Store user session data temporarily
user_sessions = {}

def get_user_session(user_id: int) -> dict:
    if user_id not in user_sessions:
        user_sessions[user_id] = {}
    return user_sessions[user_id]

def set_session_data(user_id: int, key: str, value):
    session = get_user_session(user_id)
    session[key] = value

def get_session_data(user_id: int, key: str, default=None):
    session = get_user_session(user_id)
    return session.get(key, default)

# Tournament Status Enum
class TournamentStatus(Enum):
    UPCOMING = "upcoming"
    REGISTRATION = "registration"
    ONGOING = "ongoing"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

TOURNAMENT_FORMATS = {
    "blitz": {"overs": 1, "wickets": 1, "name": "⚡ Blitz T1", "entry_fee": 10},
    "quick": {"overs": 2, "wickets": 1, "name": "🏃 Quick T2", "entry_fee": 20},
    "classic": {"overs": 5, "wickets": 2, "name": "🏏 Classic T5", "entry_fee": 50},
    "power": {"overs": 10, "wickets": 3, "name": "⚡ Power T10", "entry_fee": 100},
    "premier": {"overs": 20, "wickets": 5, "name": "🏆 Premier T20", "entry_fee": 200}
}

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

# Enhanced GIF URLs for animations with priority system
CRICKET_GIFS = {
    "six": [
        "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExdGZ2dzVjNXFmMHVxbmYwbHBqZzZqbnZoc3J0MTJxM3lqejZhcjN1aiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/1wqqlaQ7T4J0aI548K/giphy.gif",
        "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExM3N2aWFmeWNzdzRmbjNnMzRkZ3V5bzJ2ZmdybnQ0bGR2cG5yNmRrayZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/iIGy2K38b2gXC/giphy.gif",
        "https://media.giphy.com/media/3o7TKSjRrfIPjeiVyM/giphy.gif",
        "https://media.giphy.com/media/l0MYt5jPR6QX5pnqM/giphy.gif"
    ],
    "four": [
        "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExZjBqYTl6N2ZpNTN2bm1xNHNocWFpcXRiZGV6ZzVqb3NjcjB2a3oyOSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/U67BigGk1pfTW/giphy.gif",
        "https://media.giphy.com/media/3o7TKSjRrfIPjeiVyM/giphy.gif",
        "https://media.giphy.com/media/l0MYt5jPR6QX5pnqM/giphy.gif"
    ],
    "wicket": [
        "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExaWN0b2l0aWxscDVuY3Q3c3VmaHI2aXlmbGNuMnZqcmZpa2JmMnl6MSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/LpdlqT1z0Jv4k/giphy.gif",
        "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExZ2Y0NHE4NjZidjhkb25uY3g2MWdwaHdzZnV3aXZsNnprcTNzZWVwNiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/2wZfMDb005M9W/giphy.gif",
        "https://media.giphy.com/media/3o7TKSjRrfIPjeiVyM/giphy.gif"
    ],
    "tournament_win": [
        "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExMnU2cmI0ZTV0MTJpYTA0ajEzbXk2bnNocW5wYXJjaTlzcDd0NjE0MyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/3oEhn6803hZ36dSY2A/giphy.gif",
        "https://media.giphy.com/media/26u4cqiYI30juCOGY/giphy.gif",
        "https://media.giphy.com/media/3o7TKSjRrfIPjeiVyM/giphy.gif"
    ],
    "century": [
        "https://media.giphy.com/media/26u4cqiYI30juCOGY/giphy.gif",
        "https://media.giphy.com/media/3o7TKSjRrfIPjeiVyM/giphy.gif"
    ],
    "celebration": [
        "https://media.giphy.com/media/26u4cqiYI30juCOGY/giphy.gif",
        "https://media.giphy.com/media/3o7TKSjRrfIPjeiVyM/giphy.gif",
        "https://media.giphy.com/media/l0MYt5jPR6QX5pnqM/giphy.gif"
    ],
    "tournament_start": [
        "https://media.giphy.com/media/26u4cqiYI30juCOGY/giphy.gif",
        "https://media.giphy.com/media/3o7TKSjRrfIPjeiVyM/giphy.gif"
    ]
}

# Tournament themes with specific configurations
TOURNAMENT_THEMES = {
    "world_cup": {
        "name": "🏆 Cricket World Cup",
        "description": "The ultimate cricket championship",
        "emoji": "🏆",
        "min_players": 4,
        "max_players": 16,
        "entry_multiplier": 2.0
    },
    "ipl": {
        "name": "🏏 IPL Championship", 
        "description": "Indian Premier League style tournament",
        "emoji": "🏏",
        "min_players": 4,
        "max_players": 8,
        "entry_multiplier": 1.5
    },
    "ashes": {
        "name": "🔥 The Ashes",
        "description": "Historic rivalry tournament",
        "emoji": "🔥", 
        "min_players": 2,
        "max_players": 8,
        "entry_multiplier": 1.8
    },
    "champions_trophy": {
        "name": "🏅 Champions Trophy",
        "description": "Elite players championship",
        "emoji": "🏅",
        "min_players": 4,
        "max_players": 8,
        "entry_multiplier": 2.2
    },
    "t20_blast": {
        "name": "⚡ T20 Blast",
        "description": "Fast-paced T20 tournament",
        "emoji": "⚡",
        "min_players": 4,
        "max_players": 16,
        "entry_multiplier": 1.3
    }
}

# Daily challenge types
CHALLENGE_TYPES = {
    "score_runs": {
        "name": "Score Master",
        "description": "Score {target}+ runs in a single match",
        "targets": [30, 50, 75, 100],
        "base_reward": 50,
        "xp_reward": 25
    },
    "hit_sixes": {
        "name": "Big Hitter", 
        "description": "Hit {target} sixes in a single match",
        "targets": [3, 5, 8, 10],
        "base_reward": 40,
        "xp_reward": 20
    },
    "win_matches": {
        "name": "Victory Streak",
        "description": "Win {target} matches in a row",
        "targets": [2, 3, 5, 7],
        "base_reward": 75,
        "xp_reward": 35
    },
    "play_tournament": {
        "name": "Tournament Player",
        "description": "Participate in {target} tournament matches",
        "targets": [1, 3, 5, 10],
        "base_reward": 100,
        "xp_reward": 50
    }
}

# XP and level system
XP_LEVELS = {
    1: 0, 2: 100, 3: 250, 4: 450, 5: 700, 6: 1000, 7: 1350, 8: 1750, 9: 2200, 10: 2700,
    11: 3250, 12: 3850, 13: 4500, 14: 5200, 15: 5950, 16: 6750, 17: 7600, 18: 8500, 19: 9450, 20: 10500
}

def get_level_from_xp(xp: int) -> int:
    """Calculate user level from XP"""
    for level in range(20, 0, -1):
        if xp >= XP_LEVELS[level]:
            return level
    return 1

def get_xp_for_next_level(current_xp: int) -> int:
    """Get XP needed for next level"""
    current_level = get_level_from_xp(current_xp)
    if current_level >= 20:
        return 0
    return XP_LEVELS[current_level + 1] - current_xp

# Rate Limiter
class RateLimiter:
    def __init__(self):
        self.user_actions = defaultdict(lambda: deque())
        self.limits = {
            'ball_input': (10, 10),  # 10 actions per 10 seconds (more lenient)
            'command': (5, 60),      
            'callback': (10, 60),    
        }
    
    def is_allowed(self, user_id: int, action_type: str = 'default') -> bool:
        if action_type not in self.limits:
            action_type = 'default'
            
        max_actions, window = self.limits.get(action_type, (10, 60))
        now = time.time()
        user_queue = self.user_actions[f"{user_id}:{action_type}"]
        
        # Clean old entries
        while user_queue and user_queue[0] <= now - window:
            user_queue.popleft()
        
        if len(user_queue) >= max_actions:
            return False
        
        user_queue.append(now)
        return True
    
    def get_wait_time(self, user_id: int, action_type: str = 'default') -> float:
        if action_type not in self.limits:
            return 0
            
        max_actions, window = self.limits[action_type]
        user_queue = self.user_actions[f"{user_id}:{action_type}"]
        
        if len(user_queue) < max_actions:
            return 0
        
        return max(0, user_queue[0] + window - time.time())

rate_limiter = RateLimiter()

def rate_limit_check(action_type: str = 'default'):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            user_id = None
            if args and hasattr(args[0], 'from_user') and args[0].from_user:
                user_id = args[0].from_user.id
            elif 'user_id' in kwargs:
                user_id = kwargs['user_id']
            
            if user_id and not rate_limiter.is_allowed(user_id, action_type):
                wait_time = rate_limiter.get_wait_time(user_id, action_type)
                logger.warning(f"Rate limit exceeded for user {user_id}, action {action_type}")
                return f"Please wait {wait_time:.1f} seconds before trying again."
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Database Connection - Choose one based on your environment
@contextmanager
def get_db_connection():
    """Database connection - handles both SQLite and PostgreSQL"""
    conn = None
    try:
        # Check if DATABASE_URL exists (for PostgreSQL/Heroku)
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            # PostgreSQL connection
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(db_url)
            conn.cursor_factory = psycopg2.extras.RealDictCursor
        else:
            # SQLite connection (local development)
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
        
        yield conn
        conn.commit()
        logger.debug("Database transaction committed successfully")
        
    except Exception as e:
        if conn:
            conn.rollback()
            logger.error(f"Database transaction rolled back due to error: {e}")
        logger.error(f"Database error: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

def db_init():
    """Initialize database tables - compatible with both SQLite and PostgreSQL"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            # Determine database type
            db_url = os.getenv("DATABASE_URL")
            is_postgres = bool(db_url)
            
            # Use appropriate data types and syntax
            if is_postgres:
                bigint_type = "BIGINT"
                autoincrement = "SERIAL"
            else:
                bigint_type = "INTEGER"
                autoincrement = "INTEGER PRIMARY KEY AUTOINCREMENT"
            
            # Users table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS users (
                    user_id {bigint_type} PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    language_code TEXT,
                    is_premium BOOLEAN DEFAULT FALSE,
                    coins INTEGER DEFAULT 100,
                    created_at TEXT,
                    last_active TEXT,
                    total_messages INTEGER DEFAULT 0,
                    favorite_format TEXT DEFAULT '2,1'
                )
            """)
            
            # Stats table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS stats (
                    user_id {bigint_type} PRIMARY KEY,
                    games_played INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    ties INTEGER DEFAULT 0,
                    high_score INTEGER DEFAULT 0,
                    total_runs INTEGER DEFAULT 0,
                    total_balls_faced INTEGER DEFAULT 0,
                    sixes_hit INTEGER DEFAULT 0,
                    fours_hit INTEGER DEFAULT 0,
                    ducks INTEGER DEFAULT 0,
                    centuries INTEGER DEFAULT 0,
                    fifties INTEGER DEFAULT 0,
                    hat_tricks INTEGER DEFAULT 0,
                    longest_winning_streak INTEGER DEFAULT 0,
                    current_winning_streak INTEGER DEFAULT 0,
                    avg_score REAL DEFAULT 0.0,
                    strike_rate REAL DEFAULT 0.0,
                    tournaments_played INTEGER DEFAULT 0,
                    tournaments_won INTEGER DEFAULT 0,
                    tournament_points INTEGER DEFAULT 0,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            
            # Games table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS games (
                    chat_id {bigint_type} PRIMARY KEY,
                    state TEXT,
                    innings INTEGER,
                    batting TEXT,
                    player_score INTEGER DEFAULT 0,
                    bot_score INTEGER DEFAULT 0,
                    player_wkts INTEGER DEFAULT 0,
                    bot_wkts INTEGER DEFAULT 0,
                    balls_in_over INTEGER DEFAULT 0,
                    overs_bowled INTEGER DEFAULT 0,
                    target INTEGER,
                    overs_limit INTEGER DEFAULT 2,
                    wickets_limit INTEGER DEFAULT 1,
                    match_format TEXT,
                    difficulty_level TEXT,
                    player_balls_faced INTEGER DEFAULT 0,
                    bot_balls_faced INTEGER DEFAULT 0,
                    player_fours INTEGER DEFAULT 0,
                    player_sixes INTEGER DEFAULT 0,
                    bot_fours INTEGER DEFAULT 0,
                    bot_sixes INTEGER DEFAULT 0,
                    extras INTEGER DEFAULT 0,
                    powerplay_overs INTEGER DEFAULT 0,
                    is_powerplay BOOLEAN DEFAULT FALSE,
                    weather_condition TEXT,
                    pitch_condition TEXT,
                    tournament_id INTEGER,
                    tournament_round INTEGER,
                    opponent_id {bigint_type},
                    is_tournament_match BOOLEAN DEFAULT FALSE,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            
            # Match history
            if is_postgres:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS match_history (
                        id SERIAL PRIMARY KEY,
                        chat_id BIGINT,
                        user_id BIGINT,
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
                        created_at TEXT,
                        FOREIGN KEY (user_id) REFERENCES users (user_id)
                    )
                """)
            else:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS match_history (
                        id {autoincrement},
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
                        created_at TEXT,
                        FOREIGN KEY (user_id) REFERENCES users (user_id)
                    )
                """)
            
            # History/Events table
            if is_postgres:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS history (
                        id SERIAL PRIMARY KEY,
                        chat_id BIGINT,
                        event TEXT,
                        meta TEXT,
                        created_at TEXT
                    )
                """)
            else:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS history (
                        id {autoincrement},
                        chat_id INTEGER,
                        event TEXT,
                        meta TEXT,
                        created_at TEXT
                    )
                """)
                
            # Tournament table
            if is_postgres:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tournaments (
                        id SERIAL PRIMARY KEY,
                        name TEXT,
                        theme TEXT,
                        type TEXT,
                        status TEXT,
                        entry_fee INTEGER,
                        prize_pool INTEGER,
                        creator_id BIGINT,
                        created_at TEXT,
                        updated_at TEXT
                    )
                """)
            else:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tournaments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT,
                        theme TEXT,
                        type TEXT,
                        status TEXT,
                        entry_fee INTEGER,
                        prize_pool INTEGER,
                        creator_id INTEGER,
                        created_at TEXT,
                        updated_at TEXT
                    )
                """)
            
            # Tournament participants
            if is_postgres:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tournament_players (
                        id SERIAL PRIMARY KEY,
                        tournament_id INTEGER,
                        user_id BIGINT,
                        registered_at TEXT,
                        coins_paid INTEGER,
                        status TEXT,
                        FOREIGN KEY (tournament_id) REFERENCES tournaments(id),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                """)
            else:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tournament_players (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        tournament_id INTEGER,
                        user_id INTEGER,
                        registered_at TEXT,
                        coins_paid INTEGER,
                        status TEXT,
                        FOREIGN KEY (tournament_id) REFERENCES tournaments(id),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                    )
                """)
            
            # Tournament matches
            if is_postgres:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tournament_matches (
                        id SERIAL PRIMARY KEY,
                        tournament_id INTEGER,
                        round INTEGER,
                        player1_id BIGINT,
                        player2_id BIGINT,
                        winner_id BIGINT,
                        match_state TEXT,
                        match_data TEXT,
                        started_at TEXT,
                        finished_at TEXT,
                        FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
                    )
                """)
            else:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tournament_matches (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        tournament_id INTEGER,
                        round INTEGER,
                        player1_id INTEGER,
                        player2_id INTEGER,
                        winner_id INTEGER,
                        match_state TEXT,
                        match_data TEXT,
                        started_at TEXT,
                        finished_at TEXT,
                        FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
                    )
                """)
            
            # Tournament history
            if is_postgres:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tournament_history (
                        id SERIAL PRIMARY KEY,
                        tournament_id INTEGER,
                        event TEXT,
                        meta TEXT,
                        created_at TEXT,
                        FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
                    )
                """)
            else:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tournament_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        tournament_id INTEGER,
                        event TEXT,
                        meta TEXT,
                        created_at TEXT,
                        FOREIGN KEY (tournament_id) REFERENCES tournaments(id)
                    )
                """)
            # Daily challenges
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS daily_challenges (
                    id {autoincrement},
                    challenge_type TEXT,
                    description TEXT,
                    target INTEGER,
                    reward_coins INTEGER,
                    reward_xp INTEGER,
                    created_at TEXT,
                    expires_at TEXT
                )
            """)
            # User challenge progress
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS user_challenge_progress (
                    id {autoincrement},
                    user_id {bigint_type},
                    challenge_id INTEGER,
                    progress INTEGER,
                    completed BOOLEAN DEFAULT FALSE,
                    completed_at TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (challenge_id) REFERENCES daily_challenges(id)
                )
            """)
            # User XP/levels
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS user_levels (
                    user_id {bigint_type} PRIMARY KEY,
                    xp INTEGER DEFAULT 0,
                    level INTEGER DEFAULT 1,
                    tournament_points INTEGER DEFAULT 0,
                    updated_at TEXT
                )
            """)
            
            logger.info("Database tables initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

def log_event(chat_id: int, event: str, meta: str = ""):
    """Log events safely"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            # Use appropriate parameter style based on database type
            if os.getenv("DATABASE_URL"):  # PostgreSQL
                cur.execute(
                    "INSERT INTO history (chat_id, event, meta, created_at) VALUES (%s, %s, %s, %s)",
                    (chat_id, event, meta, datetime.now(timezone.utc).isoformat())
                )
            else:  # SQLite
                cur.execute(
                    "INSERT INTO history (chat_id, event, meta, created_at) VALUES (?, ?, ?, ?)",
                    (chat_id, event, meta, datetime.now(timezone.utc).isoformat())
                )
    except Exception as e:
        logger.error(f"Error logging event: {e}")

def default_game(chat_id: int, overs: int = DEFAULT_OVERS, wickets: int = DEFAULT_WICKETS, 
                difficulty: str = "medium") -> Dict[str, Any]:
    """Create default game state"""
    overs = max(1, min(overs, MAX_OVERS))
    wickets = max(1, min(wickets, MAX_WICKETS))
    powerplay = min(6, max(1, overs // 4)) if overs > 2 else 0
    
    return {
        "chat_id": chat_id,  # Add this line
        "state": "toss",
        "innings": 1,
        "batting": None,
        "player_score": 0,
        "bot_score": 0,
        "player_wkts": 0,
        "bot_wkts": 0,
        "balls_in_over": 0,
        "overs_bowled": 0,
        "target": None,
        "overs_limit": overs,
        "wickets_limit": wickets,
        "match_format": f"T{overs}",
        "difficulty_level": difficulty,
        "player_balls_faced": 0,
        "bot_balls_faced": 0,
        "player_fours": 0,
        "player_sixes": 0,
        "bot_fours": 0,
        "bot_sixes": 0,
        "extras": 0,
        "powerplay_overs": powerplay,
        "is_powerplay": powerplay > 0,
        "weather_condition": random.choice(list(WEATHER_CONDITIONS.keys())),
        "pitch_condition": random.choice(list(PITCH_CONDITIONS.keys())),
        "tournament_id": None,
        "tournament_round": None,
        "opponent_id": None,
        "is_tournament_match": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


# Game State Management
class GameState:
    def __init__(self, chat_id: int):
        self.chat_id = chat_id
        self.data = self._load_or_create()
    
    def _load_or_create(self) -> Dict[str, Any]:
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                # Use appropriate parameter style based on database type
                if os.getenv("DATABASE_URL"):  # PostgreSQL
                    cur.execute("SELECT * FROM games WHERE chat_id = %s", (self.chat_id,))
                else:  # SQLite
                    cur.execute("SELECT * FROM games WHERE chat_id = ?", (self.chat_id,))
                row = cur.fetchone()
                if row:
                    game_data = dict(row)
                    game_data['chat_id'] = self.chat_id  # Ensure chat_id is set
                    return game_data
                else:
                    return self._create_default_game()
        except Exception as e:
            logger.error(f"Error loading game state: {e}")
            return self._create_default_game()
    
    def _create_default_game(self) -> Dict[str, Any]:
        return default_game(self.chat_id)  # Pass chat_id to default_game
    
    def save(self) -> bool:
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                self.data['updated_at'] = datetime.now(timezone.utc).isoformat()
                self.data['chat_id'] = self.chat_id  # Ensure chat_id is always set
                
                is_postgres = bool(os.getenv("DATABASE_URL"))
                
                # Ensure all required fields have default values
                default_values = {
                    'state': 'toss',
                    'innings': 1,
                    'batting': None,
                    'player_score': 0,
                    'bot_score': 0,
                    'player_wkts': 0,
                    'bot_wkts': 0,
                    'balls_in_over': 0,
                    'overs_bowled': 0,
                    'target': None,
                    'overs_limit': DEFAULT_OVERS,
                    'wickets_limit': DEFAULT_WICKETS,
                    'match_format': 'T2',
                    'difficulty_level': 'medium',
                    'player_balls_faced': 0,
                    'bot_balls_faced': 0,
                    'player_fours': 0,
                    'player_sixes': 0,
                    'bot_fours': 0,
                    'bot_sixes': 0,
                    'extras': 0,
                    'powerplay_overs': 0,
                    'is_powerplay': False,
                    'weather_condition': 'clear',
                    'pitch_condition': 'normal',
                    'tournament_id': None,
                    'tournament_round': None,
                    'opponent_id': None,
                    'is_tournament_match': False,
                    'created_at': datetime.now(timezone.utc).isoformat(),
                    'updated_at': datetime.now(timezone.utc).isoformat()
                }
                
                # Apply defaults for missing values
                for key, default_val in default_values.items():
                    if key not in self.data or self.data[key] is None:
                        self.data[key] = default_val
                
                if is_postgres:
                    # PostgreSQL upsert
                    cur.execute("""
                        INSERT INTO games (
                            chat_id, state, innings, batting, player_score, bot_score,
                            player_wkts, bot_wkts, balls_in_over, overs_bowled, target,
                            overs_limit, wickets_limit, match_format, difficulty_level,
                            player_balls_faced, bot_balls_faced, player_fours, player_sixes,
                            bot_fours, bot_sixes, extras, powerplay_overs, is_powerplay,
                            weather_condition, pitch_condition, tournament_id, tournament_round,
                            opponent_id, is_tournament_match, created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s
                        )
                        ON CONFLICT (chat_id) DO UPDATE SET
                            state = EXCLUDED.state,
                            innings = EXCLUDED.innings,
                            batting = EXCLUDED.batting,
                            player_score = EXCLUDED.player_score,
                            bot_score = EXCLUDED.bot_score,
                            player_wkts = EXCLUDED.player_wkts,
                            bot_wkts = EXCLUDED.bot_wkts,
                            balls_in_over = EXCLUDED.balls_in_over,
                            overs_bowled = EXCLUDED.overs_bowled,
                            target = EXCLUDED.target,
                            overs_limit = EXCLUDED.overs_limit,
                            wickets_limit = EXCLUDED.wickets_limit,
                            match_format = EXCLUDED.match_format,
                            difficulty_level = EXCLUDED.difficulty_level,
                            player_balls_faced = EXCLUDED.player_balls_faced,
                            bot_balls_faced = EXCLUDED.bot_balls_faced,
                            player_fours = EXCLUDED.player_fours,
                            player_sixes = EXCLUDED.player_sixes,
                            bot_fours = EXCLUDED.bot_fours,
                            bot_sixes = EXCLUDED.bot_sixes,
                            extras = EXCLUDED.extras,
                            powerplay_overs = EXCLUDED.powerplay_overs,
                            is_powerplay = EXCLUDED.is_powerplay,
                            weather_condition = EXCLUDED.weather_condition,
                            pitch_condition = EXCLUDED.pitch_condition,
                            tournament_id = EXCLUDED.tournament_id,
                            tournament_round = EXCLUDED.tournament_round,
                            opponent_id = EXCLUDED.opponent_id,
                            is_tournament_match = EXCLUDED.is_tournament_match,
                            updated_at = EXCLUDED.updated_at
                    """, tuple(self.data.get(k) for k in [
                        'chat_id', 'state', 'innings', 'batting', 'player_score', 'bot_score',
                        'player_wkts', 'bot_wkts', 'balls_in_over', 'overs_bowled', 'target',
                        'overs_limit', 'wickets_limit', 'match_format', 'difficulty_level',
                        'player_balls_faced', 'bot_balls_faced', 'player_fours', 'player_sixes',
                        'bot_fours', 'bot_sixes', 'extras', 'powerplay_overs', 'is_powerplay',
                        'weather_condition', 'pitch_condition', 'tournament_id', 'tournament_round',
                        'opponent_id', 'is_tournament_match', 'created_at', 'updated_at'
                    ]))
                else:
                    # SQLite upsert - Check if record exists first
                    cur.execute("SELECT chat_id FROM games WHERE chat_id = ?", (self.chat_id,))
                    if cur.fetchone():
                        # Update existing record
                        cur.execute("""
                            UPDATE games SET 
                                state=?, innings=?, batting=?, player_score=?, bot_score=?,
                                player_wkts=?, bot_wkts=?, balls_in_over=?, overs_bowled=?, 
                                target=?, overs_limit=?, wickets_limit=?, match_format=?, 
                                difficulty_level=?, player_balls_faced=?, bot_balls_faced=?,
                                player_fours=?, player_sixes=?, bot_fours=?, bot_sixes=?,
                                extras=?, powerplay_overs=?, is_powerplay=?, weather_condition=?,
                                pitch_condition=?, tournament_id=?, tournament_round=?, 
                                opponent_id=?, is_tournament_match=?, updated_at=?
                            WHERE chat_id=?
                        """, tuple(self.data.get(k) for k in [
                            'state', 'innings', 'batting', 'player_score', 'bot_score',
                            'player_wkts', 'bot_wkts', 'balls_in_over', 'overs_bowled', 'target',
                            'overs_limit', 'wickets_limit', 'match_format', 'difficulty_level',
                            'player_balls_faced', 'bot_balls_faced', 'player_fours', 'player_sixes',
                            'bot_fours', 'bot_sixes', 'extras', 'powerplay_overs', 'is_powerplay',
                            'weather_condition', 'pitch_condition', 'tournament_id', 'tournament_round',
                            'opponent_id', 'is_tournament_match', 'updated_at'
                        ]) + (self.chat_id,))
                    else:
                        # Insert new record
                        cur.execute("""
                            INSERT INTO games (
                                chat_id, state, innings, batting, player_score, bot_score,
                                player_wkts, bot_wkts, balls_in_over, overs_bowled, target,
                                overs_limit, wickets_limit, match_format, difficulty_level,
                                player_balls_faced, bot_balls_faced, player_fours, player_sixes,
                                bot_fours, bot_sixes, extras, powerplay_overs, is_powerplay,
                                weather_condition, pitch_condition, tournament_id, tournament_round,
                                opponent_id, is_tournament_match, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            self.chat_id,
                            *tuple(self.data.get(k) for k in [
                                'state', 'innings', 'batting', 'player_score', 'bot_score',
                                'player_wkts', 'bot_wkts', 'balls_in_over', 'overs_bowled', 'target',
                                'overs_limit', 'wickets_limit', 'match_format', 'difficulty_level',
                                'player_balls_faced', 'bot_balls_faced', 'player_fours', 'player_sixes',
                                'bot_fours', 'bot_sixes', 'extras', 'powerplay_overs', 'is_powerplay',
                                'weather_condition', 'pitch_condition', 'tournament_id', 'tournament_round',
                                'opponent_id', 'is_tournament_match', 'created_at', 'updated_at'
                            ])
                        ))
                return True
        except Exception as e:
            logger.error(f"Failed to save game state: {e}")
            return False
        
    def delete(self) -> bool:
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                if os.getenv("DATABASE_URL"):  # PostgreSQL
                    cur.execute("DELETE FROM games WHERE chat_id = %s", (self.chat_id,))
                else:  # SQLite
                    cur.execute("DELETE FROM games WHERE chat_id = ?", (self.chat_id,))
                return True
        except Exception as e:
            logger.error(f"Failed to delete game: {e}")
            return False

    def update(self, **kwargs):
        self.data.update(kwargs)
        self.data['updated_at'] = datetime.now(timezone.utc).isoformat()

# Game Logic Functions
def safe_load_game(chat_id: int) -> Optional[Dict[str, Any]]:
    try:
        game_state = GameState(chat_id)
        return game_state.data
    except Exception as e:
        logger.error(f"Failed to load game: {e}")
        return None

def safe_save_game(chat_id: int, g: Dict[str, Any]):
    try:
        game_state = GameState(chat_id)
        game_state.data = g
        game_state.save()
    except Exception as e:
        logger.error(f"Failed to save game: {e}")

def delete_game(chat_id: int):
    try:
        game_state = GameState(chat_id)
        game_state.delete()
    except Exception as e:
        logger.error(f"Error deleting game: {e}")

def safe_start_new_game(chat_id: int, overs: int = DEFAULT_OVERS, wickets: int = DEFAULT_WICKETS, 
                       difficulty: str = "medium", user_id: int = None):
    try:
        g = default_game(chat_id, overs, wickets, difficulty)  # Pass chat_id
        safe_save_game(chat_id, g)
        
        weather = WEATHER_CONDITIONS.get(g.get("weather_condition", "clear"), {"description": "Clear skies"})
        pitch = PITCH_CONDITIONS.get(g.get("pitch_condition", "normal"), {"description": "Normal pitch"})
        
        match_info = (
            f"🏏 <b>New Match Started!</b>\n\n"
            f"📋 <b>Match Details:</b>\n"
            f"Format: <b>{g.get('match_format', 'T2')} ({g.get('overs_limit', 2)} over{'s' if g.get('overs_limit', 2) > 1 else ''}, "
            f"{g.get('wickets_limit', 1)} wicket{'s' if g.get('wickets_limit', 1) > 1 else ''})</b>\n"
            f"Difficulty: <b>{difficulty.title()}</b>\n"
        )
        
        if g.get('powerplay_overs', 0) > 0:
            match_info += f"Powerplay: <b>{g.get('powerplay_overs', 0)} over{'s' if g.get('powerplay_overs', 0) > 1 else ''}</b>\n"
        
        match_info += (
            f"\n🌤️ <b>Conditions:</b>\n"
            f"Weather: {weather['description']}\n"
            f"Pitch: {pitch['description']}\n\n"
            f"🪙 <b>Time for the toss!</b> Call it:"
        )
        
        bot.send_message(chat_id, match_info, reply_markup=kb_toss_choice())
        log_event(chat_id, "match_start", f"format={g.get('match_format', 'T2')} difficulty={difficulty} user={user_id}")
        
    except Exception as e:
        logger.error(f"Error starting new game: {e}")
        bot.send_message(chat_id, "❌ Error starting match. Please try again.")

def calculate_bot_move(g: Dict[str, Any], user_value: int) -> int:
    difficulty = g.get("difficulty_level", "medium")
    settings = DIFFICULTY_SETTINGS[difficulty]
    
    bot_choice = random.randint(1, 6)
    
    # Smart bot behavior based on difficulty
    if random.random() < settings["bot_skill"]:
        if g["batting"] == "bot":
            # Bot is batting - try to avoid user's number or play aggressively
            if g["innings"] == 2 and g["target"]:
                balls_left = (g["overs_limit"] - g["overs_bowled"]) * 6 - g["balls_in_over"]
                runs_needed = g["target"] - g["bot_score"] + 1
                
                if balls_left > 0:
                    required_rate = runs_needed / balls_left
                    
                    if required_rate > 8:  # Need aggressive shots
                        bot_choice = random.choices([4, 5, 6], weights=[2, 3, 4])[0]
                    elif required_rate < 4:  # Can play safely
                        bot_choice = random.choices([1, 2, 3], weights=[3, 2, 1])[0]
            
            # Try to avoid user's number with some probability
            if random.random() < settings["bot_aggression"]:
                avoid_value = user_value if random.random() < 0.7 else random.randint(1, 6)
                attempts = 0
                while bot_choice == avoid_value and attempts < 3:
                    bot_choice = random.randint(1, 6)
                    attempts += 1
        else:
            # Bot is bowling - try to match user's number
            if random.random() < 0.6:
                bot_choice = user_value
            else:
                # Common bowling choices
                common_choices = [1, 6, 4]
                bot_choice = random.choice(common_choices)
    
    return bot_choice

def get_commentary(g: Dict[str, Any], user_value: int, bot_value: int, 
                  runs_scored: int, is_wicket: bool) -> str:
    if is_wicket:
        wicket_comments = [
            f"💥 BOWLED! What a delivery! {user_value} meets {bot_value}",
            f"🎯 CAUGHT! Brilliant bowling! Both played {user_value}",
            f"⚡ CLEAN BOWLED! The stumps are shattered! {user_value} = {bot_value}",
            f"🔥 WICKET! The crowd goes wild! Matching {user_value}s",
            f"💀 PLUMB LBW! Dead in front! {user_value} vs {bot_value}"
        ]
        return random.choice(wicket_comments)
    else:
        if runs_scored == 6:
            return f"🚀 MAXIMUM! Into the stands! {runs_scored} runs!"
        elif runs_scored == 4:
            return f"⚡ FOUR! Races to the boundary! {runs_scored} runs!"
        elif runs_scored == 0:
            return "🛡️ Solid defense! No run."
        else:
            return f"🏏 Well played! {runs_scored} run{'s' if runs_scored > 1 else ''}"

def check_over_completion(g: Dict[str, Any]) -> bool:
    if g["balls_in_over"] >= 6:
        g["balls_in_over"] = 0
        g["overs_bowled"] += 1
        return True
    return False

def check_powerplay_status(g: Dict[str, Any]) -> bool:
    if g["is_powerplay"] and g["overs_bowled"] >= g["powerplay_overs"]:
        g["is_powerplay"] = False
        return True
    return False

def check_innings_end(g: Dict[str, Any]) -> bool:
    current_batting = g["batting"]
    
    # Check wickets
    if current_batting == "player" and g["player_wkts"] >= g["wickets_limit"]:
        return True
    elif current_batting == "bot" and g["bot_wkts"] >= g["wickets_limit"]:
        return True
    
    # Check overs
    if g["overs_bowled"] >= g["overs_limit"]:
        return True
    
    # Check target achieved in second innings
    if g["innings"] == 2 and g["target"]:
        current_score = g["player_score"] if current_batting == "player" else g["bot_score"]
        if current_score > g["target"]:
            return True
    
    return False

@rate_limit_check('ball_input')
def enhanced_process_ball_v2(chat_id: int, user_value: int, user_id: int):
    if not (1 <= user_value <= 6):
        return "Please send a number between 1 and 6"
    
    try:
        # Log the ball input event
        log_event(chat_id, "ball_input", f"user={user_value} from={user_id}")
        
        game_state = GameState(chat_id)
        logger.info(f"Game state loaded for chat {chat_id}: state={game_state.data.get('state', 'unknown')}")
        
        if game_state.data['state'] != 'play':
            logger.warning(f"Game state is '{game_state.data['state']}' instead of 'play' for chat {chat_id}")
            return "No active match found. Use /play to start a new match."
        
        # Calculate bot move
        bot_value = calculate_bot_move(game_state.data, user_value)
        logger.debug(f"User: {user_value}, Bot: {bot_value}")
        
        # Update balls count
        game_state.update(balls_in_over=game_state.data['balls_in_over'] + 1)
        
        is_wicket = (user_value == bot_value)
        runs_scored = 0
        
        current_batting = game_state.data['batting']
        
        if current_batting == "player":
            # Player is batting
            game_state.update(player_balls_faced=game_state.data['player_balls_faced'] + 1)
            
            if is_wicket:
                game_state.update(player_wkts=game_state.data['player_wkts'] + 1)
            else:
                runs_scored = user_value
                new_score = game_state.data['player_score'] + runs_scored
                updates = {'player_score': new_score}
                
                if runs_scored == 4:
                    updates['player_fours'] = game_state.data['player_fours'] + 1
                elif runs_scored == 6:
                    updates['player_sixes'] = game_state.data['player_sixes'] + 1
                
                game_state.update(**updates)
        
        else:  # bot batting
            game_state.update(bot_balls_faced=game_state.data['bot_balls_faced'] + 1)
            
            if is_wicket:
                game_state.update(bot_wkts=game_state.data['bot_wkts'] + 1)
            else:
                runs_scored = bot_value
                new_score = game_state.data['bot_score'] + runs_scored
                updates = {'bot_score': new_score}
                
                if runs_scored == 4:
                    updates['bot_fours'] = game_state.data['bot_fours'] + 1
                elif runs_scored == 6:
                    updates['bot_sixes'] = game_state.data['bot_sixes'] + 1
                
                game_state.update(**updates)
        
        # Get commentary
        commentary = get_commentary(game_state.data, user_value, bot_value, runs_scored, is_wicket)
        
        # Check over completion and powerplay
        over_completed = check_over_completion(game_state.data)
        powerplay_ended = check_powerplay_status(game_state.data)
        
        # Save game state
        if not game_state.save():
            logger.error("Failed to save game state - this could cause state inconsistency")
            return "Error saving game state. Please try again."
        
        # Check if innings/match ends
        if check_innings_end(game_state.data):
            result = end_innings_or_match_v2(game_state, user_id)
            return {
                'commentary': commentary,
                'is_wicket': is_wicket,
                'runs_scored': runs_scored,
                'over_completed': over_completed,
                'powerplay_ended': powerplay_ended,
                'game_state': game_state.data,
                'match_ended': True,
                'result': result
            }
        
        return {
            'commentary': commentary,
            'is_wicket': is_wicket,
            'runs_scored': runs_scored,
            'over_completed': over_completed,
            'powerplay_ended': powerplay_ended,
            'game_state': game_state.data,
            'match_ended': False
        }
        
    except Exception as e:
        logger.error(f"Error processing ball for chat {chat_id}, user {user_id}: {e}", exc_info=True)
        return "An error occurred while processing your move. Please try again."

def show_live_score(chat_id: int, g: Dict[str, Any], detailed: bool = True):
    try:
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
            if g["batting"] == "player":
                score_text += f"\n🏏 Boundaries: {g['player_fours']}×4️⃣ {g['player_sixes']}×6️⃣"
            else:
                score_text += f"\n🤖 Boundaries: {g['bot_fours']}×4️⃣ {g['bot_sixes']}×6️⃣"
        
        bot.send_message(chat_id, score_text, reply_markup=kb_match_actions())
    except Exception as e:
        logger.error(f"Error showing live score: {e}")

def safe_set_batting_order(chat_id: int, first_batting: str):
    try:
        g = safe_load_game(chat_id)
        if not g:
            logger.error(f"No game found for chat {chat_id}")
            return
        
        g["state"] = "play"
        g["batting"] = first_batting
        g["is_powerplay"] = g.get("powerplay_overs", 0) > 0
        safe_save_game(chat_id, g)
        
        powerplay_overs = g.get('powerplay_overs', 0)
        powerplay_text = f"⚡ <b>Powerplay active</b> (first {powerplay_overs} overs)\n" if powerplay_overs > 0 else ""
        
        if first_batting == "player":
            msg = (
                f"🏏 <b>You're batting first!</b>\n\n"
                f"{powerplay_text}"
                f"Send a number 1-6 to play each ball.\n"
                f"Match the bot's number = <b>OUT!</b> ❌\n"
                f"Different numbers = <b>RUNS!</b> ✅"
            )
        else:
            msg = (
                f"🎯 <b>Bot batting first! You're bowling.</b>\n\n"
                f"{powerplay_text}"
                f"Send a number 1-6 to bowl each ball.\n"
                f"Match the bot's choice = <b>WICKET!</b> ✅\n"
                f"Different numbers = Bot scores runs ❌"
            )
        
        bot.send_message(chat_id, msg, reply_markup=kb_batting_numbers())
        
    except Exception as e:
        logger.error(f"Error setting batting order: {e}")
        bot.send_message(chat_id, "❌ Error starting match. Please try /play again.")

def end_innings_or_match_v2(game_state: GameState, user_id: int):
    if game_state.data['innings'] == 1:
        # End first innings, start second
        if game_state.data['batting'] == 'player':
            game_state.update(
                target=game_state.data['player_score'],
                batting='bot',
                innings=2,
                balls_in_over=0,
                overs_bowled=0,
                is_powerplay=game_state.data['powerplay_overs'] > 0
            )
        else:
            game_state.update(
                target=game_state.data['bot_score'],
                batting='player', 
                innings=2,
                balls_in_over=0,
                overs_bowled=0,
                is_powerplay=game_state.data['powerplay_overs'] > 0
            )
        
        game_state.save()
        start_second_innings(game_state.chat_id, game_state.data)
        return "second_innings"
    
    else:
        # End match
        result = determine_match_result(game_state.data)
        save_match_history_v2(game_state.chat_id, game_state.data, result['result_type'], f"{result['margin']} {result['margin_type']}")
        update_user_stats_v2(user_id, game_state.data, result['result_type'])
        complete_match(game_state.chat_id, game_state.data)
        game_state.delete()
        return result

def start_second_innings(chat_id: int, g: Dict[str, Any]):
    first_innings_summary = ""
    
    if g["batting"] == "player":
        # Player is now batting, bot batted first
        first_innings_summary = (
            f"🏁 <b>First Innings Complete!</b>\n\n"
            f"🤖 Bot's Score: <b>{g['bot_score']}/{g['bot_wkts']}</b>\n"
            f"⏱️ Overs: <b>{g['overs_bowled']}.{g['balls_in_over']}</b>\n\n"
            f"🎯 <b>Target for You: {g['target'] + 1}</b>\n\n"
            f"🏏 <b>Second Innings</b>\n"
            f"Your turn to chase! Good luck!"
        )
    else:
        # Bot is now batting, player batted first
        first_innings_summary = (
            f"🏁 <b>First Innings Complete!</b>\n\n"
            f"🏏 Your Score: <b>{g['player_score']}/{g['player_wkts']}</b>\n"
            f"⏱️ Overs: <b>{g['overs_bowled']}.{g['balls_in_over']}</b>\n\n"
            f"🎯 <b>Target for Bot: {g['target'] + 1}</b>\n\n"
            f"🎯 <b>Second Innings</b>\n"
            f"Bot is batting now. Bowl to defend your total!"
        )
    
    bot.send_message(chat_id, first_innings_summary, reply_markup=kb_batting_numbers())

def complete_match(chat_id: int, g: Dict[str, Any]):
    try:
        player_score, bot_score = g["player_score"], g["bot_score"]
        
        if player_score > bot_score:
            result = "win"
            margin = player_score - bot_score
            margin_text = f"by {margin} runs"
            result_emoji = "🏆"
            result_text = "YOU WIN!"
        elif bot_score > player_score:
            result = "loss"
            wickets_left = g["wickets_limit"] - g["bot_wkts"]
            margin_text = f"by {wickets_left} wickets" if wickets_left > 0 else "on last ball"
            result_emoji = "😔"
            result_text = "BOT WINS!"
        else:
            result = "tie"
            margin_text = "Match Tied!"
            result_emoji = "🤝"
            result_text = "IT'S A TIE!"
        
        match_summary = generate_match_summary(g, result, margin_text)
        
        final_message = (
            f"🏁 <b>MATCH OVER</b>\n\n"
            f"{result_emoji} <b>{result_text}</b>\n"
            f"Margin: <b>{margin_text}</b>\n\n"
            f"{match_summary}\n\n"
            f"Well played! 🏏"
        )
        
        bot.send_message(chat_id, final_message, reply_markup=kb_post_match())
        
    except Exception as e:
        logger.error(f"Error completing match: {e}")
        bot.send_message(chat_id, "Match completed! Use /play for a new match.")

def generate_match_summary(g: Dict[str, Any], result: str, margin: str) -> str:
    try:
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
    except Exception as e:
        logger.error(f"Error generating match summary: {e}")
        return "Match completed successfully!"

def determine_match_result(game_data: Dict[str, Any]) -> Dict[str, Any]:
    player_score = game_data['player_score']
    bot_score = game_data['bot_score']
    
    if player_score > bot_score:
        result = {
            'winner': 'player',
            'result_type': 'win',
            'margin': player_score - bot_score,
            'margin_type': 'runs'
        }
    elif bot_score > player_score:
        wickets_left = game_data['wickets_limit'] - game_data['bot_wkts']
        result = {
            'winner': 'bot',
            'result_type': 'loss',
            'margin': wickets_left if wickets_left > 0 else 0,
            'margin_type': 'wickets'
        }
    else:
        result = {
            'winner': 'tie',
            'result_type': 'tie',
            'margin': 0,
            'margin_type': 'tie'
        }
    
    return result

def save_match_history_v2(chat_id: int, g: Dict[str, Any], result: str, margin: str):
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            # Get user_id from recent history
            if is_postgres:
                cur.execute("""
                    SELECT meta FROM history 
                    WHERE chat_id=%s AND event='ball_input' 
                    ORDER BY id DESC LIMIT 1
                """, (chat_id,))
            else:
                cur.execute("""
                    SELECT meta FROM history 
                    WHERE chat_id=? AND event='ball_input' 
                    ORDER BY id DESC LIMIT 1
                """, (chat_id,))
            
            row = cur.fetchone()
            user_id = None
            
            if row and row["meta"]:
                try:
                    parts = dict(kv.split("=") for kv in row["meta"].split() if "=" in kv)
                    user_id = int(parts.get("from", "0"))
                except:
                    pass
            
            if user_id and user_id > 0:
                total_balls = g["player_balls_faced"] + g["bot_balls_faced"]
                duration_minutes = max(1, total_balls // 12)
                now = datetime.now(timezone.utc).isoformat()
                
                if is_postgres:
                    cur.execute("""
                        INSERT INTO match_history (
                            chat_id, user_id, match_format, player_score, bot_score,
                            player_wickets, bot_wickets, overs_played, result, margin,
                            player_strike_rate, match_duration_minutes, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        chat_id, user_id, g["match_format"], g["player_score"], g["bot_score"],
                        g["player_wkts"], g["bot_wkts"], 
                        g["overs_bowled"] + (g["balls_in_over"]/6.0),
                        result, margin,
                        (g["player_score"]/max(g["player_balls_faced"], 1)*100),
                        duration_minutes, now
                    ))
                else:
                    cur.execute("""
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
                    
    except Exception as e:
        logger.error(f"Error saving match history: {e}")

def update_user_stats_v2(user_id: int, g: Dict[str, Any], result: str):
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            now = datetime.now(timezone.utc).isoformat()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            # Update win/loss/tie counts
            if result == "win":
                if is_postgres:
                    cur.execute("""
                        UPDATE stats SET 
                            wins = wins + 1,
                            current_winning_streak = current_winning_streak + 1,
                            longest_winning_streak = GREATEST(longest_winning_streak, current_winning_streak + 1)
                        WHERE user_id = %s
                    """, (user_id,))
                else:
                    cur.execute("""
                        UPDATE stats SET 
                            wins = wins + 1,
                            current_winning_streak = current_winning_streak + 1,
                            longest_winning_streak = MAX(longest_winning_streak, current_winning_streak + 1)
                        WHERE user_id = ?
                    """, (user_id,))
            elif result == "loss":
                param = (user_id,) if is_postgres else (user_id,)
                param_style = "%s" if is_postgres else "?"
                cur.execute(f"""
                    UPDATE stats SET 
                        losses = losses + 1,
                        current_winning_streak = 0
                    WHERE user_id = {param_style}
                """, param)
            else:  # tie
                param = (user_id,) if is_postgres else (user_id,)
                param_style = "%s" if is_postgres else "?"
                cur.execute(f"""
                    UPDATE stats SET 
                        ties = ties + 1,
                        current_winning_streak = 0
                    WHERE user_id = {param_style}
                """, param)
            
            # Update other stats
            centuries_increment = 1 if g["player_score"] >= 100 else 0
            fifties_increment = 1 if g["player_score"] >= 50 and g["player_score"] < 100 else 0
            ducks_increment = 1 if g["player_score"] == 0 and g["player_balls_faced"] > 0 else 0
            
            if is_postgres:
                cur.execute("""
                    UPDATE stats SET 
                        games_played = games_played + 1,
                        total_runs = total_runs + %s,
                        total_balls_faced = total_balls_faced + %s,
                        sixes_hit = sixes_hit + %s,
                        fours_hit = fours_hit + %s,
                        centuries = centuries + %s,
                        fifties = fifties + %s,
                        ducks = ducks + %s,
                        high_score = GREATEST(high_score, %s),
                        updated_at = %s
                    WHERE user_id = %s
                """, (
                    g["player_score"], g["player_balls_faced"], g["player_sixes"],
                    g["player_fours"], centuries_increment, fifties_increment, 
                    ducks_increment, g["player_score"], now, user_id
                ))
            else:
                cur.execute("""
                    UPDATE stats SET 
                        games_played = games_played + 1,
                        total_runs = total_runs + ?,
                        total_balls_faced = total_balls_faced + ?,
                        sixes_hit = sixes_hit + ?,
                        fours_hit = fours_hit + ?,
                        centuries = centuries + ?,
                        fifties = fifties + ?,
                        ducks = ducks + ?,
                        high_score = MAX(high_score, ?),
                        updated_at = ?
                    WHERE user_id = ?
                """, (
                    g["player_score"], g["player_balls_faced"], g["player_sixes"],
                    g["player_fours"], centuries_increment, fifties_increment, 
                    ducks_increment, g["player_score"], now, user_id
                ))
            
            # Update calculated stats
            param_style = "%s" if is_postgres else "?"
            cur.execute(f"""
                UPDATE stats SET 
                    avg_score = CAST(total_runs AS REAL) / NULLIF(games_played, 0),
                    strike_rate = CAST(total_runs AS REAL) * 100.0 / NULLIF(total_balls_faced, 0)
                WHERE user_id = {param_style}
            """, (user_id,))
            
    except Exception as e:
        logger.error(f"Error updating user stats: {e}")

def upsert_user(u: types.User):
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            now = datetime.now(timezone.utc).isoformat()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            # Check if user exists first
            param_style = "%s" if is_postgres else "?"
            cur.execute(f"SELECT user_id FROM users WHERE user_id = {param_style}", (u.id,))
            exists = cur.fetchone()
            
            if exists:
                # Update existing user
                if is_postgres:
                    cur.execute("""
                        UPDATE users SET
                            username = %s,
                            first_name = %s,
                            last_name = %s,
                            language_code = %s,
                            is_premium = %s,
                            last_active = %s,
                            total_messages = total_messages + 1
                        WHERE user_id = %s
                    """, (
                        u.username, u.first_name, u.last_name, 
                        u.language_code, getattr(u, 'is_premium', False),
                        now, u.id
                    ))
                else:
                    cur.execute("""
                        UPDATE users SET
                            username = ?,
                            first_name = ?,
                            last_name = ?,
                            language_code = ?,
                            is_premium = ?,
                            last_active = ?,
                            total_messages = total_messages + 1
                        WHERE user_id = ?
                    """, (
                        u.username, u.first_name, u.last_name, 
                        u.language_code, getattr(u, 'is_premium', False),
                        now, u.id
                    ))
            else:
                # Insert new user
                if is_postgres:
                    cur.execute("""
                        INSERT INTO users (
                            user_id, username, first_name, last_name, language_code, 
                            is_premium, coins, created_at, last_active, total_messages
                        ) VALUES (%s, %s, %s, %s, %s, %s, 100, %s, %s, 1)
                    """, (
                        u.id, u.username, u.first_name, u.last_name, 
                        u.language_code, getattr(u, 'is_premium', False),
                        now, now
                    ))
                else:
                    cur.execute("""
                        INSERT INTO users (
                            user_id, username, first_name, last_name, language_code, 
                            is_premium, coins, created_at, last_active, total_messages
                        ) VALUES (?, ?, ?, ?, ?, ?, 100, ?, ?, 1)
                    """, (
                        u.id, u.username, u.first_name, u.last_name, 
                        u.language_code, getattr(u, 'is_premium', False),
                        now, now
                    ))
            
            # Ensure stats record exists
            cur.execute(f"SELECT user_id FROM stats WHERE user_id = {param_style}", (u.id,))
            if not cur.fetchone():
                if is_postgres:
                    cur.execute("""
                        INSERT INTO stats (user_id, created_at, updated_at) 
                        VALUES (%s, %s, %s)
                    """, (u.id, now, now))
                else:
                    cur.execute("""
                        INSERT INTO stats (user_id, created_at, updated_at) 
                        VALUES (?, ?, ?)
                    """, (u.id, now, now))
            
            logger.info(f"User {u.id} upserted successfully")
            
    except Exception as e:
        logger.error(f"Error upserting user {u.id}: {e}", exc_info=True)

# Keyboard definitions
def kb_main_menu() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🏏 Quick Play", callback_data="quick_play"),
        types.InlineKeyboardButton("⚙️ Custom Match", callback_data="custom_match")
    )
    kb.add(
        types.InlineKeyboardButton("🏆 Tournament", callback_data="tournament_menu"),
        types.InlineKeyboardButton("📊 My Stats", callback_data="my_stats")
    )
    kb.add(
        types.InlineKeyboardButton("🥇 Leaderboard", callback_data="leaderboard"),
        types.InlineKeyboardButton("🏅 Achievements", callback_data="achievements")
    )
    kb.add(types.InlineKeyboardButton("ℹ️ Help", callback_data="help"))
    return kb

def kb_difficulty_select() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    for diff, settings in DIFFICULTY_SETTINGS.items():
        emoji = "🟢" if diff == "easy" else "🟡" if diff == "medium" else "🔴" if diff == "hard" else "⚫"
        kb.add(types.InlineKeyboardButton(
            f"{emoji} {diff.title()}",
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
    row1 = [types.KeyboardButton("1"), types.KeyboardButton("2"), types.KeyboardButton("3")]
    row2 = [types.KeyboardButton("4"), types.KeyboardButton("5"), types.KeyboardButton("6")]
    kb.add(*row1)
    kb.add(*row2)
    kb.add(types.KeyboardButton("📊 Score"), types.KeyboardButton("🏳️ Forfeit"))
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

def kb_match_actions() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=3)
    kb.add(
        types.InlineKeyboardButton("📊 Score", callback_data="live_score"),
        types.InlineKeyboardButton("🏳️ Forfeit", callback_data="forfeit_confirm")
    )
    return kb

def kb_forfeit_confirm() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.add(
        types.InlineKeyboardButton("✅ Yes, Forfeit", callback_data="forfeit_yes"),
        types.InlineKeyboardButton("❌ No, Continue", callback_data="forfeit_no")
    )
    return kb

# Stats functions
def show_user_stats(chat_id: int, user_id: int):
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            cur.execute(f"SELECT * FROM stats WHERE user_id={param_style}", (user_id,))
            stats = cur.fetchone()
            if not stats or stats["games_played"] == 0:
                bot.send_message(chat_id, "📊 No statistics yet! Play your first match with /play")
                return
            
            win_rate = (stats["wins"] / stats["games_played"] * 100) if stats["games_played"] > 0 else 0
            
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
                f"• Fours Hit: {stats['fours_hit']}\n"
                f"• Ducks: {stats['ducks']}\n\n"
                f"🔥 <b>Best Streak:</b> {stats['longest_winning_streak']}\n"
                f"🎯 <b>Current Streak:</b> {stats['current_winning_streak']}"
            )
            
            bot.send_message(chat_id, stats_text)
            
    except Exception as e:
        logger.error(f"Error showing user stats: {e}")
        bot.send_message(chat_id, "❌ Error loading statistics. Please try again.")

def show_leaderboard(chat_id: int, category: str = "wins"):
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            if category == "wins":
                query = """
                    SELECT u.first_name, u.username, s.wins, s.games_played, s.high_score
                    FROM stats s JOIN users u ON u.user_id = s.user_id
                    WHERE s.games_played >= 1
                    ORDER BY s.wins DESC, s.high_score DESC
                    LIMIT 10
                """
            else:
                query = """
                    SELECT u.first_name, u.username, s.high_score, s.games_played, s.wins
                    FROM stats s JOIN users u ON u.user_id = s.user_id
                    WHERE s.games_played >= 1
                    ORDER BY s.high_score DESC
                    LIMIT 10
                """
            
            cur.execute(query)
            players = cur.fetchall()
            
            if not players:
                bot.send_message(chat_id, "🏆 No players on leaderboard yet! Be the first to play!")
                return
            
            category_title = {"wins": "Most Wins", "high_score": "Highest Scores"}
            
            leaderboard_text = f"🏆 <b>Leaderboard - {category_title.get(category, 'Top Players')}</b>\n\n"
            
            for i, player in enumerate(players, 1):
                name = player["first_name"] or (f"@{player['username']}" if player["username"] else "Anonymous")
                
                if category == "wins":
                    stat = f"{player['wins']} wins"
                else:
                    stat = f"{player['high_score']} runs"
                
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                leaderboard_text += f"{medal} {name} - {stat}\n"
            
            bot.send_message(chat_id, leaderboard_text)
            
    except Exception as e:
        logger.error(f"Error showing leaderboard: {e}")
        bot.send_message(chat_id, "❌ Error loading leaderboard. Please try again.")

def show_achievements(chat_id: int, user_id: int):
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            cur.execute(f"SELECT * FROM stats WHERE user_id={param_style}", (user_id,))
            stats = cur.fetchone()
        
        achievements_text = f"🏅 <b>Your Achievements</b>\n\n"
        
        if not stats:
            achievements_text += "Play matches to unlock achievements!"
            bot.send_message(chat_id, achievements_text)
            return
        
        # Check achievements
        unlocked = []
        locked = []
        
        # First Victory
        if stats["wins"] >= 1:
            unlocked.append("🏆 First Victory - Win your first match")
        else:
            locked.append("🔒 First Victory - Win your first match")
        
        # Century Maker
        if stats["centuries"] >= 1:
            unlocked.append("💯 Century Maker - Score 100+ runs")
        else:
            locked.append("🔒 Century Maker - Score 100+ runs")
            
        # Consistent Player
        if stats["longest_winning_streak"] >= 5:
            unlocked.append("🔥 Consistent Player - Win 5 matches in a row")
        else:
            locked.append("🔒 Consistent Player - Win 5 matches in a row")
            
        # Big Hitter
        if stats["sixes_hit"] >= 50:
            unlocked.append("🚀 Big Hitter - Hit 50 sixes")
        else:
            locked.append("🔒 Big Hitter - Hit 50 sixes")
            
        # Experience Player
        if stats["games_played"] >= 10:
            unlocked.append("🎮 Experienced Player - Play 10 matches")
        else:
            locked.append("🔒 Experienced Player - Play 10 matches")
        
        if unlocked:
            achievements_text += "<b>Unlocked:</b>\n"
            for achievement in unlocked:
                achievements_text += f"✅ {achievement}\n"
            achievements_text += "\n"

        if locked:
            achievements_text += "<b>Locked:</b>\n"
            for achievement in locked:
                achievements_text += f"{achievement}\n"

        bot.send_message(chat_id, achievements_text)
        
    except Exception as e:
        logger.error(f"Error showing achievements: {e}")
        bot.send_message(chat_id, "❌ Error loading achievements. Please try again.")

# GIF Animation System with Priority-Based Fallback
def send_gif_with_fallback(chat_id: int, gif_type: str, fallback_text: str, reply_markup=None):
    """Send GIF with automatic fallback to text animation if GIF fails"""
    try:
        if gif_type in CRICKET_GIFS and CRICKET_GIFS[gif_type]:
            gif_url = random.choice(CRICKET_GIFS[gif_type])
            try:
                # Try to send GIF first (priority)
                bot.send_animation(chat_id, gif_url, reply_markup=reply_markup)
                return True
            except Exception as gif_error:
                logger.warning(f"GIF failed for {gif_type}: {gif_error}")
                # Fallback to text animation
                bot.send_message(chat_id, fallback_text, reply_markup=reply_markup)
                return False
        else:
            # No GIF available, send text
            bot.send_message(chat_id, fallback_text, reply_markup=reply_markup)
            return False
    except Exception as e:
        logger.error(f"Error in gif fallback system: {e}")
        bot.send_message(chat_id, fallback_text, reply_markup=reply_markup)
        return False

# Tournament System Functions
def create_tournament(creator_id: int, theme: str, tournament_format: str) -> Optional[int]:
    """Create a new tournament"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            now = datetime.now(timezone.utc).isoformat()
            
            theme_config = TOURNAMENT_THEMES.get(theme, TOURNAMENT_THEMES["world_cup"])
            format_config = TOURNAMENT_FORMATS.get(tournament_format, TOURNAMENT_FORMATS["quick"])
            
            entry_fee = int(format_config["entry_fee"] * theme_config["entry_multiplier"])
            
            tournament_name = f"{theme_config['emoji']} {theme_config['name']} - {format_config['name']}"
            
            if is_postgres:
                cur.execute("""
                    INSERT INTO tournaments (name, theme, type, status, entry_fee, prize_pool, creator_id, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (tournament_name, theme, tournament_format, TournamentStatus.REGISTRATION.value, 
                      entry_fee, 0, creator_id, now, now))
                tournament_id = cur.fetchone()[0]
            else:
                cur.execute("""
                    INSERT INTO tournaments (name, theme, type, status, entry_fee, prize_pool, creator_id, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (tournament_name, theme, tournament_format, TournamentStatus.REGISTRATION.value,
                      entry_fee, 0, creator_id, now, now))
                tournament_id = cur.lastrowid
            
            # Auto-register creator
            register_for_tournament(tournament_id, creator_id)
            
            return tournament_id
            
    except Exception as e:
        logger.error(f"Error creating tournament: {e}")
        return None

def register_for_tournament(tournament_id: int, user_id: int) -> bool:
    """Register user for tournament"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            now = datetime.now(timezone.utc).isoformat()
            
            # Check if already registered
            cur.execute(f"""
                SELECT id FROM tournament_players 
                WHERE tournament_id = {param_style} AND user_id = {param_style}
            """, (tournament_id, user_id))
            
            if cur.fetchone():
                return False  # Already registered
            
            # Get tournament details
            cur.execute(f"SELECT entry_fee, status FROM tournaments WHERE id = {param_style}", (tournament_id,))
            tournament = cur.fetchone()
            
            if not tournament or tournament["status"] != TournamentStatus.REGISTRATION.value:
                return False
            
            # Check user coins
            cur.execute(f"SELECT coins FROM users WHERE user_id = {param_style}", (user_id,))
            user = cur.fetchone()
            
            if not user or user["coins"] < tournament["entry_fee"]:
                return False
            
            # Deduct coins and register
            cur.execute(f"""
                UPDATE users SET coins = coins - {param_style} WHERE user_id = {param_style}
            """, (tournament["entry_fee"], user_id))
            
            cur.execute(f"""
                INSERT INTO tournament_players (tournament_id, user_id, registered_at, coins_paid, status)
                VALUES ({param_style}, {param_style}, {param_style}, {param_style}, 'active')
            """, (tournament_id, user_id, now, tournament["entry_fee"]))
            
            # Update prize pool
            cur.execute(f"""
                UPDATE tournaments SET prize_pool = prize_pool + {param_style} WHERE id = {param_style}
            """, (tournament["entry_fee"], tournament_id))
            
            return True
            
    except Exception as e:
        logger.error(f"Error registering for tournament: {e}")
        return False

def start_tournament(tournament_id: int) -> bool:
    """Start tournament and create bracket"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            # Get participants
            cur.execute(f"""
                SELECT user_id FROM tournament_players 
                WHERE tournament_id = {param_style} AND status = 'active'
                ORDER BY registered_at
            """, (tournament_id,))
            
            participants = [row["user_id"] for row in cur.fetchall()]
            
            if len(participants) < 2:
                return False
            
            # Create first round matches
            random.shuffle(participants)  # Randomize bracket
            
            round_num = 1
            for i in range(0, len(participants), 2):
                if i + 1 < len(participants):
                    player1 = participants[i]
                    player2 = participants[i + 1]
                    
                    cur.execute(f"""
                        INSERT INTO tournament_matches 
                        (tournament_id, round, player1_id, player2_id, match_state, started_at)
                        VALUES ({param_style}, {param_style}, {param_style}, {param_style}, 'pending', {param_style})
                    """, (tournament_id, round_num, player1, player2, datetime.now(timezone.utc).isoformat()))
            
            # Update tournament status
            cur.execute(f"""
                UPDATE tournaments SET status = {param_style}, updated_at = {param_style} 
                WHERE id = {param_style}
            """, (TournamentStatus.ONGOING.value, datetime.now(timezone.utc).isoformat(), tournament_id))
            
            return True
            
    except Exception as e:
        logger.error(f"Error starting tournament: {e}")
        return False

def get_tournament_bracket(tournament_id: int) -> str:
    """Generate tournament bracket display"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            cur.execute(f"""
                SELECT tm.round, tm.player1_id, tm.player2_id, tm.winner_id, tm.match_state,
                       u1.first_name as p1_name, u2.first_name as p2_name
                FROM tournament_matches tm
                LEFT JOIN users u1 ON u1.user_id = tm.player1_id
                LEFT JOIN users u2 ON u2.user_id = tm.player2_id
                WHERE tm.tournament_id = {param_style}
                ORDER BY tm.round, tm.id
            """, (tournament_id,))
            
            matches = cur.fetchall()
            
            if not matches:
                return "No matches found"
            
            bracket_text = "🏆 <b>Tournament Bracket</b>\n\n"
            
            current_round = 0
            for match in matches:
                if match["round"] != current_round:
                    current_round = match["round"]
                    round_name = f"Round {current_round}" if current_round > 1 else "First Round"
                    bracket_text += f"\n<b>{round_name}:</b>\n"
                
                p1_name = match["p1_name"] or "Player 1"
                p2_name = match["p2_name"] or "Player 2"
                
                if match["match_state"] == "completed":
                    winner_name = p1_name if match["winner_id"] == match["player1_id"] else p2_name
                    bracket_text += f"✅ {p1_name} vs {p2_name} → <b>{winner_name}</b>\n"
                elif match["match_state"] == "ongoing":
                    bracket_text += f"🎮 {p1_name} vs {p2_name} (Playing)\n"
                else:
                    bracket_text += f"⏳ {p1_name} vs {p2_name} (Pending)\n"
            
            return bracket_text
            
    except Exception as e:
        logger.error(f"Error generating bracket: {e}")
        return "Error loading bracket"

def get_active_tournaments() -> List[Dict]:
    """Get list of active tournaments"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            cur.execute("""
                SELECT t.*, COUNT(tp.user_id) as player_count
                FROM tournaments t
                LEFT JOIN tournament_players tp ON t.id = tp.tournament_id AND tp.status = 'active'
                WHERE t.status IN ('registration', 'ongoing')
                GROUP BY t.id, t.name, t.theme, t.type, t.status, t.entry_fee, t.prize_pool, t.creator_id, t.created_at, t.updated_at
                ORDER BY t.created_at DESC
                LIMIT 10
            """)
            
            return [dict(row) for row in cur.fetchall()]
            
    except Exception as e:
        logger.error(f"Error getting active tournaments: {e}")
        return []

def create_daily_challenges():
    """Create daily challenges"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            now = datetime.now(timezone.utc)
            expires_at = (now + timedelta(days=1)).isoformat()
            
            # Clear old challenges
            cur.execute(f"DELETE FROM daily_challenges WHERE expires_at < {param_style}", (now.isoformat(),))
            
            # Create new challenges
            for challenge_type, config in CHALLENGE_TYPES.items():
                target = random.choice(config["targets"])
                description = config["description"].format(target=target)
                reward_coins = config["base_reward"] + (target * 2)
                reward_xp = config["xp_reward"] + target
                
                cur.execute(f"""
                    INSERT INTO daily_challenges 
                    (challenge_type, description, target, reward_coins, reward_xp, created_at, expires_at)
                    VALUES ({param_style}, {param_style}, {param_style}, {param_style}, {param_style}, {param_style}, {param_style})
                """, (challenge_type, description, target, reward_coins, reward_xp, now.isoformat(), expires_at))
            
    except Exception as e:
        logger.error(f"Error creating daily challenges: {e}")

def check_challenge_progress(user_id: int, challenge_type: str, progress_value: int):
    """Update challenge progress"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            now = datetime.now(timezone.utc).isoformat()
            
            # Get active challenges of this type
            cur.execute(f"""
                SELECT dc.id, dc.target, dc.reward_coins, dc.reward_xp
                FROM daily_challenges dc
                LEFT JOIN user_challenge_progress ucp ON dc.id = ucp.challenge_id AND ucp.user_id = {param_style}
                WHERE dc.challenge_type = {param_style} AND dc.expires_at > {param_style}
                AND (ucp.completed IS NULL OR ucp.completed = FALSE)
            """, (user_id, challenge_type, now))
            
            challenges = cur.fetchall()
            
            for challenge in challenges:
                # Check if progress record exists
                cur.execute(f"""
                    SELECT id, progress FROM user_challenge_progress 
                    WHERE user_id = {param_style} AND challenge_id = {param_style}
                """, (user_id, challenge["id"]))
                
                progress_record = cur.fetchone()
                
                if progress_record:
                    new_progress = max(progress_record["progress"], progress_value)
                    cur.execute(f"""
                        UPDATE user_challenge_progress 
                        SET progress = {param_style}, completed = {param_style}, completed_at = {param_style}
                        WHERE id = {param_style}
                    """, (new_progress, new_progress >= challenge["target"], 
                          now if new_progress >= challenge["target"] else None, progress_record["id"]))
                else:
                    completed = progress_value >= challenge["target"]
                    cur.execute(f"""
                        INSERT INTO user_challenge_progress 
                        (user_id, challenge_id, progress, completed, completed_at)
                        VALUES ({param_style}, {param_style}, {param_style}, {param_style}, {param_style})
                    """, (user_id, challenge["id"], progress_value, completed, 
                          now if completed else None))
                
                # Award rewards if completed
                if progress_value >= challenge["target"]:
                    cur.execute(f"""
                        UPDATE users SET coins = coins + {param_style} WHERE user_id = {param_style}
                    """, (challenge["reward_coins"], user_id))
                    
                    # Update XP
                    update_user_xp(user_id, challenge["reward_xp"])
                    
    except Exception as e:
        logger.error(f"Error checking challenge progress: {e}")

def update_user_xp(user_id: int, xp_gained: int):
    """Update user XP and level"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            now = datetime.now(timezone.utc).isoformat()
            
            # Get current XP
            cur.execute(f"SELECT xp, level FROM user_levels WHERE user_id = {param_style}", (user_id,))
            user_level = cur.fetchone()
            
            if user_level:
                new_xp = user_level["xp"] + xp_gained
                new_level = get_level_from_xp(new_xp)
                
                cur.execute(f"""
                    UPDATE user_levels SET xp = {param_style}, level = {param_style}, updated_at = {param_style}
                    WHERE user_id = {param_style}
                """, (new_xp, new_level, now, user_id))
                
                return new_level > user_level["level"]  # Level up?
            else:
                new_level = get_level_from_xp(xp_gained)
                cur.execute(f"""
                    INSERT INTO user_levels (user_id, xp, level, updated_at)
                    VALUES ({param_style}, {param_style}, {param_style}, {param_style})
                """, (user_id, xp_gained, new_level, now))
                
                return new_level > 1
                
    except Exception as e:
        logger.error(f"Error updating user XP: {e}")
        return False

# Enhanced Keyboard Functions for Tournament System
def kb_tournament_menu() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🆕 Create Tournament", callback_data="create_tournament"),
        types.InlineKeyboardButton("🔍 Join Tournament", callback_data="join_tournament")
    )
    kb.add(
        types.InlineKeyboardButton("🏆 My Tournaments", callback_data="my_tournaments"),
        types.InlineKeyboardButton("📊 Tournament Stats", callback_data="tournament_stats")
    )
    kb.add(
        types.InlineKeyboardButton("🎯 Daily Challenges", callback_data="daily_challenges"),
        types.InlineKeyboardButton("🔙 Back", callback_data="main_menu")
    )
    return kb

def kb_tournament_themes() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    for theme_key, theme_config in TOURNAMENT_THEMES.items():
        kb.add(types.InlineKeyboardButton(
            f"{theme_config['emoji']} {theme_config['name'][:20]}...",
            callback_data=f"theme_{theme_key}"
        ))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="tournament_menu"))
    return kb

def kb_tournament_formats() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    for format_key, format_config in TOURNAMENT_FORMATS.items():
        kb.add(types.InlineKeyboardButton(
            f"{format_config['name']} (💰{format_config['entry_fee']})",
            callback_data=f"tformat_{format_key}"
        ))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="create_tournament"))
    return kb

def kb_active_tournaments(tournaments: List[Dict]) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    for tournament in tournaments:
        status_emoji = "📝" if tournament["status"] == "registration" else "🎮"
        kb.add(types.InlineKeyboardButton(
            f"{status_emoji} {tournament['name'][:30]}... ({tournament['player_count']} players)",
            callback_data=f"view_tournament_{tournament['id']}"
        ))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="tournament_menu"))
    return kb

def ensure_user(message: types.Message):
    if message.from_user:
        try:
            upsert_user(message.from_user)
        except Exception as e:
            logger.error(f"Failed to upsert user {message.from_user.id}: {e}")

# Enhanced message processing with GIF animations
@bot.message_handler(func=lambda message: message.text and message.text.isdigit() and 1 <= int(message.text) <= 6)
def handle_ball_input_with_gifs(message: types.Message):
    try:
        ensure_user(message)
        user_value = int(message.text)
        result = enhanced_process_ball_v2(message.chat.id, user_value, message.from_user.id)
        
        if isinstance(result, str):
            bot.send_message(message.chat.id, result)
            return
        
        # Enhanced ball result processing with GIF animations
        commentary = result['commentary']
        is_wicket = result['is_wicket']
        runs_scored = result['runs_scored']
        game_state = result['game_state']
        
        # Determine GIF type and send with fallback
        if is_wicket:
            send_gif_with_fallback(message.chat.id, "wicket", commentary)
        elif runs_scored == 6:
            send_gif_with_fallback(message.chat.id, "six", commentary)
        elif runs_scored == 4:
            send_gif_with_fallback(message.chat.id, "four", commentary)
        else:
            bot.send_message(message.chat.id, commentary)
        
        # Check for special achievements with GIFs
        if game_state['batting'] == 'player':
            if game_state['player_score'] >= 100 and game_state['player_score'] - runs_scored < 100:
                send_gif_with_fallback(message.chat.id, "century", 
                    "🎉 CENTURY! What an incredible knock! 💯")
        
        # Over completion message
        if result['over_completed']:
            over_summary = f"📊 End of Over {game_state['overs_bowled']}\n"
            if game_state['batting'] == 'player':
                over_summary += f"Your Score: {game_state['player_score']}/{game_state['player_wkts']}"
            else:
                over_summary += f"Bot Score: {game_state['bot_score']}/{game_state['bot_wkts']}"
            bot.send_message(message.chat.id, over_summary)
        
        # Powerplay end message
        if result['powerplay_ended']:
            bot.send_message(message.chat.id, "⚡ Powerplay Over! Field restrictions lifted.")
        
        # Match end processing with tournament integration
        if result['match_ended']:
            match_result = result['result']
            if isinstance(match_result, dict):
                # Tournament match completion
                if game_state.get('is_tournament_match'):
                    handle_tournament_match_completion(message.chat.id, message.from_user.id, match_result)
                
                # Update challenge progress
                check_challenge_progress(message.from_user.id, "score_runs", game_state['player_score'])
                check_challenge_progress(message.from_user.id, "hit_sixes", game_state['player_sixes'])
                if match_result['result_type'] == 'win':
                    check_challenge_progress(message.from_user.id, "win_matches", 1)
        
    except Exception as e:
        logger.error(f"Error handling ball input with GIFs: {e}")
        bot.send_message(message.chat.id, "❌ Error processing your move. Please try again.")

def handle_tournament_match_completion(chat_id: int, user_id: int, match_result: Dict):
    """Handle completion of tournament match"""
    try:
        # Award tournament points and XP
        if match_result['result_type'] == 'win':
            update_user_xp(user_id, 100)  # Bonus XP for tournament win
            send_gif_with_fallback(chat_id, "tournament_win", 
                "🏆 TOURNAMENT VICTORY! You advance to the next round! 🎉")
        else:
            update_user_xp(user_id, 25)  # Participation XP
            
        # Update tournament progress
        # This would integrate with the tournament bracket system
        
    except Exception as e:
        logger.error(f"Error handling tournament match completion: {e}")

@bot.message_handler(commands=["tournament"])
def cmd_tournament(message: types.Message):
    try:
        ensure_user(message)
        show_tournament_menu(message.chat.id)
    except Exception as e:
        logger.error(f"Error in tournament command: {e}")
        bot.send_message(message.chat.id, "❌ Error loading tournament menu.")

def show_tournament_menu(chat_id: int):
    """Show main tournament menu"""
    tournament_text = (
        "🏆 <b>Tournament Central</b>\n\n"
        "🎮 <b>Compete against players worldwide!</b>\n\n"
        "✨ <b>Features:</b>\n"
        "• Multiple tournament themes\n"
        "• Real-time brackets\n"
        "• Prize pools & rewards\n"
        "• Tournament rankings\n\n"
        "Choose an option below:"
    )
    
    send_gif_with_fallback(chat_id, "tournament_start", tournament_text, kb_tournament_menu())

@bot.message_handler(commands=["challenges"])
def cmd_challenges(message: types.Message):
    try:
        ensure_user(message)
        show_daily_challenges(message.chat.id, message.from_user.id)
    except Exception as e:
        logger.error(f"Error in challenges command: {e}")
        bot.send_message(message.chat.id, "❌ Error loading challenges.")

def show_daily_challenges(chat_id: int, user_id: int):
    """Show daily challenges"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            now = datetime.now(timezone.utc).isoformat()
            
            # Get active challenges with user progress
            cur.execute(f"""
                SELECT dc.*, COALESCE(ucp.progress, 0) as progress, 
                       COALESCE(ucp.completed, FALSE) as completed
                FROM daily_challenges dc
                LEFT JOIN user_challenge_progress ucp ON dc.id = ucp.challenge_id AND ucp.user_id = {param_style}
                WHERE dc.expires_at > {param_style}
                ORDER BY dc.created_at
            """, (user_id, now))
            
            challenges = cur.fetchall()
            
            if not challenges:
                create_daily_challenges()  # Create new challenges if none exist
                bot.send_message(chat_id, "🎯 New daily challenges created! Use /challenges to view them.")
                return
            
            challenges_text = "🎯 <b>Daily Challenges</b>\n\n"
            
            for challenge in challenges:
                status_emoji = "✅" if challenge["completed"] else "🎯"
                progress_bar = "█" * min(10, int(challenge["progress"] / challenge["target"] * 10))
                progress_bar += "░" * (10 - len(progress_bar))
                
                challenges_text += (
                    f"{status_emoji} <b>{CHALLENGE_TYPES[challenge['challenge_type']]['name']}</b>\n"
                    f"   {challenge['description']}\n"
                    f"   Progress: {challenge['progress']}/{challenge['target']} [{progress_bar}]\n"
                    f"   Reward: 💰{challenge['reward_coins']} coins, ⭐{challenge['reward_xp']} XP\n\n"
                )
            
            bot.send_message(chat_id, challenges_text)
            
    except Exception as e:
        logger.error(f"Error showing daily challenges: {e}")
        bot.send_message(chat_id, "❌ Error loading challenges.")

# Message handlers
@bot.message_handler(commands=['start'])
def cmd_start(message: types.Message):
    try:
        ensure_user(message)
        welcome_text = (
            f"🏏 <b>Welcome to Cricket Bot, {message.from_user.first_name}!</b>\n\n"
            f"🎮 The most advanced hand-cricket experience on Telegram!\n\n"
            f"✨ <b>Features:</b>\n"
            f"• 🎯 Multiple game formats (T1 to T20)\n"
            f"• 🤖 Smart AI opponents\n" 
            f"• 📊 Detailed statistics\n"
            f"• 🎬 Live commentary & GIF animations\n"
            f"• 🏆 Tournaments & daily challenges\n\n"
            f"Ready to play some cricket?"
        )
        bot.send_message(message.chat.id, welcome_text, reply_markup=kb_main_menu())
    except Exception as e:
        logger.error(f"Error in start command: {e}")
        bot.send_message(message.chat.id, "Welcome to Cricket Bot!")

@bot.message_handler(commands=["help"])  
def cmd_help(message: types.Message):
    try:
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
            f"• Tournaments - compete with others\n\n"
            f"<b>⚡ Commands:</b>\n"
            f"/play - Start quick match\n"
            f"/tournament - Join tournaments\n"
            f"/challenges - Daily challenges\n"
            f"/stats - Your statistics\n"
            f"/leaderboard - Top players\n"
            f"/help - Show this help\n\n"
            f"<b>🎯 Tips:</b>\n"
            f"��� Use /score during match for live score\n"
            f"• Higher difficulty = smarter AI\n"
            f"• Complete challenges for rewards!"
        )
        
        bot.send_message(message.chat.id, help_text, reply_markup=kb_main_menu())
    except Exception as e:
        logger.error(f"Error in help command: {e}")
        bot.send_message(message.chat.id, "Help information unavailable. Please try again later.")

@bot.message_handler(commands=["play"])
def cmd_play(message: types.Message):
    try:
        ensure_user(message)
        safe_start_new_game(message.chat.id, user_id=message.from_user.id)
    except Exception as e:
        logger.error(f"Error in play command: {e}")
        bot.send_message(message.chat.id, "❌ Error starting match. Please try again.")

@bot.message_handler(commands=["stats"])
def cmd_stats(message: types.Message):
    try:
        ensure_user(message)
        show_user_stats(message.chat.id, message.from_user.id)
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        bot.send_message(message.chat.id, "❌ Error loading statistics.")

@bot.message_handler(commands=["leaderboard"])
def cmd_leaderboard(message: types.Message):
    try:
        ensure_user(message)
        show_leaderboard(message.chat.id)
    except Exception as e:
        logger.error(f"Error in leaderboard command: {e}")
        bot.send_message(message.chat.id, "❌ Error loading leaderboard.")

# Handle ball input (1-6) with enhanced GIF animations
@bot.message_handler(func=lambda message: message.text and message.text.isdigit() and 1 <= int(message.text) <= 6)
def handle_ball_input(message: types.Message):
    try:
        ensure_user(message)
        user_value = int(message.text)
        result = enhanced_process_ball_v2(message.chat.id, user_value, message.from_user.id)
        
        if isinstance(result, str):
            bot.send_message(message.chat.id, result)
        else:
            # Enhanced GIF animations based on result
            gif_sent = False
            
            # Try to send appropriate GIF first
            if result['runs_scored'] == 6:
                gif_sent = send_gif_with_fallback(message.chat.id, "six", result['commentary'])
            elif result['runs_scored'] == 4:
                gif_sent = send_gif_with_fallback(message.chat.id, "four", result['commentary'])
            elif result['is_wicket']:
                gif_sent = send_gif_with_fallback(message.chat.id, "wicket", result['commentary'])
            
            # If no GIF was sent, send text commentary
            if not gif_sent:
                bot.send_message(message.chat.id, result['commentary'])
            
            # Check for century celebration
            if result['game_state']['batting'] == 'player' and result['game_state']['player_score'] >= 100:
                prev_score = result['game_state']['player_score'] - result['runs_scored']
                if prev_score < 100:  # Just reached century
                    century_text = f"🎉 CENTURY! What an incredible knock! {result['game_state']['player_score']} runs!"
                    send_gif_with_fallback(message.chat.id, "century", century_text)
            
            # Send over completion message if needed
            if result['over_completed']:
                over_summary = f"📊 End of Over {result['game_state']['overs_bowled']}\n"
                if result['game_state']['batting'] == 'player':
                    over_summary += f"Your Score: {result['game_state']['player_score']}/{result['game_state']['player_wkts']}"
                else:
                    over_summary += f"Bot Score: {result['game_state']['bot_score']}/{result['game_state']['bot_wkts']}"
                bot.send_message(message.chat.id, over_summary)
            
            # Send powerplay end message if needed
            if result['powerplay_ended']:
                bot.send_message(message.chat.id, "⚡ Powerplay Over! Field restrictions lifted.")
                
    except Exception as e:
        logger.error(f"Error handling ball input: {e}")
        bot.send_message(message.chat.id, "❌ Error processing your move. Please try again.")

# Background task to create daily challenges
def create_daily_challenges_task():
    """Background task to create daily challenges"""
    while True:
        try:
            create_daily_challenges()
            logger.info("Daily challenges created/updated")
            time.sleep(86400)  # Sleep for 24 hours
        except Exception as e:
            logger.error(f"Error in daily challenges task: {e}")
            time.sleep(3600)  # Sleep for 1 hour on error

# Start background tasks
def start_background_tasks():
    """Start background tasks"""
    try:
        # Start daily challenges task in background
        challenge_thread = threading.Thread(target=create_daily_challenges_task, daemon=True)
        challenge_thread.start()
        logger.info("Background tasks started")
    except Exception as e:
        logger.error(f"Error starting background tasks: {e}")

# Initialize everything
def initialize_bot():
    """Initialize the bot and database"""
    try:
        logger.info("Initializing Cricket Bot...")
        
        # Initialize database
        db_init()
        logger.info("Database initialized")
        
        # Create initial daily challenges
        create_daily_challenges()
        logger.info("Initial daily challenges created")
        
        # Start background tasks
        start_background_tasks()
        
        logger.info("Cricket Bot initialization complete!")
        
    except Exception as e:
        logger.error(f"Error during bot initialization: {e}")
        raise

# Main execution
if __name__ == "__main__":
    try:
        # Initialize bot
        initialize_bot()
        
        if USE_WEBHOOK:
            # Webhook mode
            logger.info("Starting bot in webhook mode...")
            
            # Flask app for webhook
            app = Flask(__name__)
            
            @app.route(f"/{TOKEN}", methods=['POST'])
            def webhook():
                try:
                    json_str = request.get_data().decode('UTF-8')
                    update = telebot.types.Update.de_json(json_str)
                    bot.process_new_updates([update])
                    return '', 200
                except Exception as e:
                    logger.error(f"Webhook error: {e}")
                    return '', 500
            
            @app.route('/health', methods=['GET'])
            def health_check():
                return jsonify({"status": "healthy", "bot": "cricket_bot"}), 200
            
            # Set webhook
            if WEBHOOK_URL:
                webhook_url = f"{WEBHOOK_URL}/{TOKEN}"
                bot.remove_webhook()
                time.sleep(1)
                bot.set_webhook(url=webhook_url)
                logger.info(f"Webhook set to: {webhook_url}")
            
            # Start Flask app
            app.run(host='0.0.0.0', port=PORT, debug=False)
            
        else:
            # Polling mode
            logger.info("Starting bot in polling mode...")
            bot.remove_webhook()
            bot.infinity_polling(timeout=60, long_polling_timeout=60)
            
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Critical error: {e}")
        raise
    finally:
        logger.info("Cricket Bot shutdown complete")

@bot.message_handler(commands=["score"])
def cmd_score(message: types.Message):
    try:
        ensure_user(message)
        g = safe_load_game(message.chat.id)
        if g and g.get("state") == "play":
            show_live_score(message.chat.id, g)
        else:
            bot.send_message(message.chat.id, "No active match found.")
    except Exception as e:
        logger.error(f"Error in score command: {e}")
        bot.send_message(message.chat.id, "❌ Error loading score.")

@bot.message_handler(func=lambda message: message.text and message.text.isdigit() and 1 <= int(message.text) <= 6)
def handle_ball_input(message: types.Message):
    try:
        ensure_user(message)
        user_value = int(message.text)
        result = enhanced_process_ball_v2(message.chat.id, user_value, message.from_user.id)
        
        if isinstance(result, dict) and not result.get('match_ended', False):
            # Send commentary
            bot.send_message(message.chat.id, result['commentary'])
            
            # Check for special events
            if result['is_wicket']:
                send_cricket_animation(message.chat.id, "wicket")
            elif result['runs_scored'] == 6:
                send_cricket_animation(message.chat.id, "six")
            
            # Check for over completion
            if result['over_completed']:
                over_text = f"🔄 <b>Over Complete!</b>\n\n"
                if result['powerplay_ended']:
                    over_text += "⚡ <b>Powerplay ended!</b>\n\n"
                over_text += "Next over starting..."
                bot.send_message(message.chat.id, over_text)
            
            # Show live score after each ball
            show_live_score(message.chat.id, result['game_state'], detailed=False)
            
        elif isinstance(result, dict) and result.get('match_ended', True):
            # Match ended, result contains the match result
            bot.send_message(message.chat.id, result['commentary'])
            
        else:
            bot.send_message(message.chat.id, result)
    except Exception as e:
        logger.error(f"Error handling ball input: {e}")
        bot.send_message(message.chat.id, "❌ Error processing your move. Please try again.")

@bot.message_handler(func=lambda message: message.text and "📊" in message.text)
def handle_score_request(message: types.Message):
    try:
        ensure_user(message)
        g = safe_load_game(message.chat.id)
        if g and g.get("state") == "play":
            show_live_score(message.chat.id, g)
        else:
            bot.send_message(message.chat.id, "No active match found.")
    except Exception as e:
        logger.error(f"Error handling score request: {e}")
        bot.send_message(message.chat.id, "❌ Error loading score.")

@bot.message_handler(func=lambda message: message.text and "🏳️" in message.text)
def handle_forfeit_request(message: types.Message):
    try:
        ensure_user(message)
        g = safe_load_game(message.chat.id)
        if g and g.get("state") == "play":
            bot.send_message(
                message.chat.id, 
                "Are you sure you want to forfeit the match?",
                reply_markup=kb_forfeit_confirm()
            )
        else:
            bot.send_message(message.chat.id, "No active match found.")
    except Exception as e:
        logger.error(f"Error handling forfeit request: {e}")
        bot.send_message(message.chat.id, "❌ Error processing your request.")

# Callback handlers
@bot.callback_query_handler(func=lambda call: True)
@rate_limit_check('callback')
def handle_callback(call: types.CallbackQuery):
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Main menu callbacks
        if call.data == "main_menu":
            bot.edit_message_text(
                "🏏 <b>Cricket Bot</b>\n\nWhat would you like to do?",
                chat_id, call.message.message_id,
                reply_markup=kb_main_menu()
            )
        
        # Quick play
        elif call.data == "quick_play":
            safe_start_new_game(chat_id, user_id=user_id)
            bot.answer_callback_query(call.id, "Starting new match...")
        
        # Custom match
        elif call.data == "custom_match":
            bot.edit_message_text(
                "⚙️ <b>Custom Match Settings</b>\n\nSelect match format:",
                chat_id, call.message.message_id,
                reply_markup=kb_format_select()
            )
        
        # Format selection
        elif call.data.startswith("format_"):
            if call.data == "format_random":
                overs = random.choice([1, 2, 5, 10, 20])
                wickets = min(5, max(1, overs // 4))
            else:
                parts = call.data.split("_")
                overs = int(parts[1])
                wickets = int(parts[2])
            
            set_session_data(user_id, "custom_overs", overs)
            set_session_data(user_id, "custom_wickets", wickets)
            
            bot.edit_message_text(
                f"⚙️ <b>Custom Match Settings</b>\n\n"
                f"Format: T{overs} ({overs} overs, {wickets} wickets)\n\n"
                f"Select difficulty level:",
                chat_id, call.message.message_id,
                reply_markup=kb_difficulty_select()
            )
        
        # Difficulty selection
        elif call.data.startswith("diff_"):
            difficulty = call.data.split("_")[1]
            overs = get_session_data(user_id, "custom_overs", DEFAULT_OVERS)
            wickets = get_session_data(user_id, "custom_wickets", DEFAULT_WICKETS)
            
            safe_start_new_game(chat_id, overs, wickets, difficulty, user_id)
            bot.answer_callback_query(call.id, "Starting custom match...")
        
        # Stats and other callbacks
        elif call.data == "my_stats":
            show_user_stats(chat_id, user_id)
            bot.answer_callback_query(call.id)
        
        elif call.data == "leaderboard":
            show_leaderboard(chat_id)
            bot.answer_callback_query(call.id)
        
        elif call.data == "achievements":
            show_achievements(chat_id, user_id)
            bot.answer_callback_query(call.id)
        
        elif call.data == "help":
            help_text = (
                f"🏏 <b>Cricket Bot Help</b>\n\n"
                f"<b>📖 How to Play:</b>\n"
                f"• Choose numbers 1-6 for each ball\n"
                f"• Same numbers = OUT! ❌\n"
                f"• Different numbers = RUNS! ✅\n\n"
                f"<b>🎮 Game Modes:</b>\n"
                f"• Quick Play - instant T2 match\n"
                f"• Custom Match - choose format & difficulty\n\n"
                f"<b>⚡ Commands:</b>\n"
                f"/play - Start quick match\n"
                f"/stats - Your statistics\n"
                f"/leaderboard - Top players\n"
                f"/help - Show this help\n\n"
                f"<b>🎯 Tips:</b>\n"
                f"• Use 📊 Score button during match\n"
                f"• Higher difficulty = smarter AI\n"
                f"• Complete achievements for bragging rights!"
            )
            bot.edit_message_text(
                help_text,
                chat_id, call.message.message_id,
                reply_markup=kb_main_menu()
            )
        
        # Toss callbacks
        elif call.data in ["toss_heads", "toss_tails"]:
            user_choice = call.data.split("_")[1]
            handle_toss_result(chat_id, user_choice, user_id)
            bot.answer_callback_query(call.id, f"You chose {user_choice.title()}!")
        
        # Bat/Bowl choice
        elif call.data == "choose_bat":
            safe_set_batting_order(chat_id, "player")
            bot.answer_callback_query(call.id, "You chose to bat first!")
        
        elif call.data == "choose_bowl":
            safe_set_batting_order(chat_id, "bot")
            bot.answer_callback_query(call.id, "You chose to bowl first!")
        
        # Match actions
        elif call.data == "live_score":
            g = safe_load_game(chat_id)
            if g and g.get("state") == "play":
                show_live_score(chat_id, g)
            bot.answer_callback_query(call.id)
        
        elif call.data == "forfeit_confirm":
            bot.edit_message_text(
                "Are you sure you want to forfeit the match?",
                chat_id, call.message.message_id,
                reply_markup=kb_forfeit_confirm()
            )
        
        elif call.data == "forfeit_yes":
            g = safe_load_game(chat_id)
            if g:
                delete_game(chat_id)
                bot.edit_message_text(
                    "🏳️ Match forfeited. Better luck next time!",
                    chat_id, call.message.message_id,
                    reply_markup=kb_main_menu()
                )
            bot.answer_callback_query(call.id)
        
        elif call.data == "forfeit_no":
            g = safe_load_game(chat_id)
            if g and g.get("state") == "play":
                show_live_score(chat_id, g)
            bot.answer_callback_query(call.id)
        
        # Post match actions
        elif call.data == "play_again":
            safe_start_new_game(chat_id, user_id=user_id)
            bot.answer_callback_query(call.id, "Starting new match...")
        
        elif call.data == "match_summary":
            bot.answer_callback_query(call.id, "Match summary feature coming soon!")
        
        # Back buttons
        elif call.data == "back_main":
            bot.edit_message_text(
                "🏏 <b>Cricket Bot</b>\n\nWhat would you like to do?",
                chat_id, call.message.message_id,
                reply_markup=kb_main_menu()
            )
        
        else:
            bot.answer_callback_query(call.id, "Unknown action")
    
    except Exception as e:
        logger.error(f"Error handling callback {call.data}: {e}")
        bot.answer_callback_query(call.id, "An error occurred. Please try again.")

def handle_toss_result(chat_id: int, user_choice: str, user_id: int):
    try:
        toss_result = random.choice(["heads", "tails"])
        
        if user_choice == toss_result:
            bot.send_message(
                chat_id,
                f"🪙 <b>Toss Result: {toss_result.title()}</b>\n\n"
                f"🎉 You won the toss! What would you like to do?",
                reply_markup=kb_bat_bowl_choice()
            )
        else:
            bot_choice = random.choice(["bat", "bowl"])
            if bot_choice == "bat":
                first_batting = "bot"
                choice_text = "Bot chose to bat first"
            else:
                first_batting = "player"
                choice_text = "Bot chose to bowl first"
            
            bot.send_message(
                chat_id,
                f"🪙 <b>Toss Result: {toss_result.title()}</b>\n\n"
                f"😔 Bot won the toss and {choice_text}!"
            )
            
            safe_set_batting_order(chat_id, first_batting)
    
    except Exception as e:
        logger.error(f"Error handling toss result: {e}")
        bot.send_message(chat_id, "❌ Error with toss. Please try /play again.")

# Cricket animations
def send_cricket_animation(chat_id: int, event_type: str, caption: str = ""):
    """Send cricket-related animations with fallback to emojis"""
    try:
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
            "tie": "🤝",
            "tournament_win": "🏆"
        }
        
        if event_type in CRICKET_EMOJIS:
            emoji = CRICKET_EMOJIS[event_type]
            message = f"{emoji} {caption}" if caption else emoji
            bot.send_message(chat_id, message)
            return True
            
    except Exception as e:
        logger.error(f"Failed to send animation for {event_type}: {e}")
    
    return False

# Flask app for webhook mode
app = Flask(__name__)

@app.route('/')
def index():
    return "<h1>Cricket Bot is alive!</h1><p>Webhook is ready for Telegram updates.</p>", 200

@app.route('/' + TOKEN, methods=['POST'])
def webhook():
    try:
        if request.headers.get('content-type') == 'application/json':
            json_string = request.get_data().decode('utf-8')
            logger.info(f"Received webhook data: {json_string[:200]}...")
            
            update = telebot.types.Update.de_json(json_string)
            logger.info(f"Processing update ID: {update.update_id}")
            
            if update.message:
                logger.info(f"Message in update: '{update.message.text}' from user {update.message.from_user.id}")
            else:
                logger.info("No message in update")
            
            # Process the update
            bot.process_new_updates([update])
            
            logger.info(f"Update {update.update_id} processed successfully")
            return '', 200
        else:
            logger.warning(f"Invalid content-type: {request.headers.get('content-type')}")
            return 'Invalid request', 400
    except Exception as e:
        logger.error(f"Webhook error: {e}", exc_info=True)
        return 'Error processing update', 500

@app.route('/health', methods=['GET'])
def health_check():
    return 'OK', 200

@app.route('/test', methods=['GET'])
def test_bot():
    try:
        # Test database connection
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            db_ok = True
    except Exception as e:
        logger.error(f"Database test failed: {e}")
        db_ok = False
    
    # Test bot info
    try:
        bot_info = bot.get_me()
        bot_ok = True
    except Exception as e:
        logger.error(f"Bot test failed: {e}")
        bot_ok = False
    
    return {
        'status': 'ok' if (db_ok and bot_ok) else 'error',
        'database': 'ok' if db_ok else 'error',
        'bot': 'ok' if bot_ok else 'error',
        'webhook_url': WEBHOOK_URL if USE_WEBHOOK else 'polling'
    }, 200

# Initialize for gunicorn and direct execution
try:
    logger.info("=== STARTING DATABASE INITIALIZATION ===")
    db_init()
    logger.info("=== DATABASE INITIALIZED SUCCESSFULLY ===")
    
    # Set webhook for production
    if WEBHOOK_URL and USE_WEBHOOK:
        logger.info("=== SETTING WEBHOOK ===")
        webhook_url = WEBHOOK_URL.rstrip('/')
        if not webhook_url.endswith('/' + TOKEN):
            webhook_url += '/' + TOKEN
        bot.set_webhook(url=webhook_url)
        logger.info(f"=== WEBHOOK SET TO: {webhook_url} ===")
        
except Exception as e:
    logger.error(f"=== CRITICAL INITIALIZATION ERROR ===", exc_info=True)

# Main execution
if __name__ == "__main__":
    try:
        if USE_WEBHOOK and WEBHOOK_URL:
            logger.info("Starting Flask app for webhook mode...")
            app.run(host="0.0.0.0", port=PORT, debug=False)
        else:
            logger.info("Starting bot in polling mode...")
            # Remove webhook if it exists
            bot.remove_webhook()
            logger.info("Webhook removed, starting polling...")
            bot.infinity_polling(none_stop=True, timeout=60)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
