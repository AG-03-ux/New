import math
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
from functools import wraps
import schedule
from collections import deque
import json
import uuid
from enum import Enum
import os
from pathlib import Path
import time
import schedule
from collections import deque
import uuid
from pathlib import Path



# Load environment variables first
load_dotenv()

def check_environment():
    """Check environment variables and configuration"""
    print("=== ENVIRONMENT CHECK ===")
    
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    print(f"1. TOKEN present: {'✓ Yes' if token else '✗ MISSING'}")
    if token:
        print(f"   Token length: {len(token)} chars")
    
    use_webhook = int(os.getenv("USE_WEBHOOK", "0"))
    webhook_url = os.getenv("WEBHOOK_URL", "")
    print(f"2. Webhook mode: {'✓ Enabled' if use_webhook else '✓ Polling'}")
    if use_webhook:
        print(f"   Webhook URL: {webhook_url if webhook_url else '✗ MISSING'}")
    
    db_path = os.getenv("DB_PATH", "cricket_bot.db")
    database_url = os.getenv("DATABASE_URL")
    print(f"3. Database: {'PostgreSQL' if database_url else 'SQLite'}")
    print(f"   Path/URL: {database_url or db_path}")
    
    port = os.getenv("PORT", 5000)
    print(f"4. Port: {port}")
    
    return bool(token)


def test_bot_connection():
    """Test bot connection to Telegram"""
    print("\n=== BOT CONNECTION TEST ===")
    
    try:
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not token:
            print("✗ No token available")
            return False
            
        bot = telebot.TeleBot(token)
        me = bot.get_me()
        
        print(f"✓ Bot connected successfully")
        print(f"  Username: @{me.username}")
        print(f"  Name: {me.first_name}")
        print(f"  ID: {me.id}")
        return True
        
    except Exception as e:
        print(f"✗ Bot connection failed: {e}")
        return False
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
import logging.config

logger = logging.getLogger('cricket-bot')
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(name)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('cricket_bot.log')
    ]
)
logger = logging.getLogger("cricket-bot")

logger.info("=== MODULE LOADING STARTED ===")
logger.info(f"TOKEN present: {bool(TOKEN)}")
logger.info(f"USE_WEBHOOK: {USE_WEBHOOK}")
logger.info(f"Python version: {sys.version}")
logger.info(f"Token present: {bool(TOKEN)}")
logger.info(f"Token length: {len(TOKEN) if TOKEN else 0}")

if not TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set. Please set it in your environment variables or .env file")

# Initialize Bot directly
try:
    bot = telebot.TeleBot(TOKEN, parse_mode="HTML", threaded=False)
    bot_info = bot.get_me()
    logger.info(f"✓ Bot initialized: @{bot_info.username} (ID: {bot_info.id})")
except Exception as e:
    logger.error(f"✗ Bot initialization failed: {e}")
    raise RuntimeError(f"Cannot initialize bot - check your TOKEN: {e}")

# Log all registered handlers for debugging
logger.info(f"Total message handlers registered: {len(bot.message_handlers)}")
logger.info(f"Total callback handlers registered: {len(bot.callback_query_handlers)}")
# Store user session data temporarily
user_sessions = {}


def validate_environment():
    """Validate required environment variables"""
    required_vars = ["TELEGRAM_BOT_TOKEN"]
    missing = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)
    
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")
    
    # Optional validation
    webhook_url = os.getenv("WEBHOOK_URL", "")
    use_webhook = int(os.getenv("USE_WEBHOOK", "0"))
    
    if use_webhook and not webhook_url:
        logger.warning("USE_WEBHOOK is set but WEBHOOK_URL is empty. Bot will use polling instead.")
    
    logger.info("Environment validation completed successfully")


def get_user_session_data(user_id: int, key: str = None, default=None):
    """Get session data - FIXED VERSION"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            cur.execute(f"SELECT session_data FROM user_sessions WHERE user_id = {param_style}", (user_id,))
            
            row = cur.fetchone()
            if row and row['session_data']:
                session_data = json.loads(row['session_data'])
                if key:
                    return session_data.get(key, default)
                return session_data
            
            return default if key else {}
            
    except Exception as e:
        logger.error(f"Error getting session data: {e}")
        return default if key else {}


def set_user_session_data(user_id: int, key: str, value):
    """Set session data in database - fixed with consistent parameters"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            # Get existing data
            session_data = {}
            cur.execute(f"SELECT session_data FROM user_sessions WHERE user_id = {param_style}", (user_id,))
            
            row = cur.fetchone()
            if row and row['session_data']:
                session_data = json.loads(row['session_data'])
            
            # Update data
            session_data[key] = value
            
            # Save back
            now = datetime.now(timezone.utc).isoformat()
            if is_postgres:
                cur.execute("""
                    INSERT INTO user_sessions (user_id, session_data, updated_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET session_data = EXCLUDED.session_data, updated_at = EXCLUDED.updated_at
                """, (user_id, json.dumps(session_data), now))
            else:
                cur.execute("""
                    INSERT OR REPLACE INTO user_sessions (user_id, session_data, updated_at)
                    VALUES (?, ?, ?)
                """, (user_id, json.dumps(session_data), now))
                
    except Exception as e:
        logger.error(f"Error setting session data: {e}")

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
            
            # Extract user_id safely
            if args:
                arg = args[0]
                if hasattr(arg, 'from_user') and arg.from_user:
                    user_id = arg.from_user.id
                elif hasattr(arg, 'message') and hasattr(arg.message, 'from_user'):
                    user_id = arg.message.from_user.id
                elif isinstance(arg, int):
                    user_id = arg
            
            # Skip rate limiting if no user_id found
            if not user_id:
                return func(*args, **kwargs)
            
            if not rate_limiter.is_allowed(user_id, action_type):
                wait_time = rate_limiter.get_wait_time(user_id, action_type)
                logger.warning(f"Rate limit exceeded for user {user_id}, action {action_type}")
                
                # Try to send error message
                try:
                    if hasattr(args[0], 'answer_callback_query'):
                        args[0].answer_callback_query(
                            f"⏱️ Please wait {wait_time:.1f} seconds.",
                            show_alert=True
                        )
                    elif hasattr(args[0], 'chat'):
                        bot.send_message(
                            args[0].chat.id,
                            f"⏱️ Please wait {wait_time:.1f} seconds."
                        )
                except:
                    pass
                return None
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Database Connection - Choose one based on your environment
# Add connection pooling and better error handling
@contextmanager
def get_db_connection():
    conn = None
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            db_url = os.getenv("DATABASE_URL")
            if db_url:
                import psycopg2
                import psycopg2.extras
                conn = psycopg2.connect(db_url)
                conn.cursor_factory = psycopg2.extras.RealDictCursor
            else:
                conn = sqlite3.connect(DB_PATH, timeout=30.0)
                conn.row_factory = sqlite3.Row
            
            yield conn
            conn.commit()
            break
            
        except Exception as e:
            retry_count += 1
            if conn:
                conn.rollback()
            if retry_count >= max_retries:
                logger.error(f"Database connection failed after {max_retries} attempts: {e}")
                raise
            time.sleep(0.5 * retry_count)
        finally:
            if conn:
                conn.close()


def create_schema_version_table():
    """Create schema_version table to track migrations"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            if is_postgres:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY,
                        description TEXT,
                        applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            else:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY,
                        description TEXT,
                        applied_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            
            # Check if we have any version records
            cur.execute("SELECT COUNT(*) as count FROM schema_version")
            count = cur.fetchone()["count"]
            
            if count == 0:
                # Insert initial version
                now = datetime.now(timezone.utc).isoformat()
                if is_postgres:
                    cur.execute("""
                        INSERT INTO schema_version (version, description, applied_at)
                        VALUES (%s, %s, %s)
                    """, (0, "Initial schema", now))
                else:
                    cur.execute("""
                        INSERT INTO schema_version (version, description, applied_at)
                        VALUES (?, ?, ?)
                    """, (0, "Initial schema", now))
                    
    except Exception as e:
        logger.error(f"Error creating schema_version table: {e}")
        raise

def get_db_version():
    """Get current database schema version"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
            result = cur.fetchone()
            return result['version'] if result else 0
    except Exception:
        # If schema_version table doesn't exist, assume version 0
        return 0

def migrate_database():
    """Run all pending database migrations"""
    try:
        logger.info("Starting database migration check...")
        
        # Ensure schema_version table exists
        create_schema_version_table()
        
        current_version = get_db_version()
        # For now, just create the table - no actual migrations needed
        
        logger.info(f"Current database version: {current_version}")
        logger.info("Database migration completed successfully")
        
    except Exception as e:
        logger.error(f"Database migration failed: {e}", exc_info=True)
        logger.warning("Application will continue with current database schema")
def safe_bot_operation(func):
    """Decorator for safe bot operations with retry logic"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Bot operation failed after {max_retries} attempts: {e}")
                    raise
                time.sleep(0.5 * (attempt + 1))
        return wrapper


def db_init():
    try:
        logger.info("=== DATABASE INITIALIZATION STARTED ===")
        
        with get_db_connection() as conn:
            cur = conn.cursor()
            
            # Determine database type
            db_url = os.getenv("DATABASE_URL")
            is_postgres = bool(db_url)
            
            if is_postgres:
                bigint_type = "BIGINT"
                autoincrement = "SERIAL PRIMARY KEY"
                bool_type = "BOOLEAN"
                bool_default = "FALSE"  # <-- ADD THIS LINE
                text_type = "TEXT"
                real_type = "REAL"
                timestamp_type = "TIMESTAMPTZ"
            else: # SQLite
                bigint_type = "INTEGER"
                autoincrement = "INTEGER PRIMARY KEY AUTOINCREMENT"
                bool_type = "INTEGER"
                bool_default = "0"  # <-- ADD THIS LINE
                text_type = "TEXT"
                real_type = "REAL"
                timestamp_type = "TEXT"
            
            # Create base tables
            tables = [
                f"""CREATE TABLE IF NOT EXISTS users (
                    user_id {bigint_type} PRIMARY KEY,
                    username {text_type},
                    first_name {text_type},
                    last_name {text_type},
                    language_code {text_type},
                    is_premium {bool_type} DEFAULT {bool_default},
                    coins INTEGER DEFAULT 100,
                    created_at {timestamp_type},
                    last_active {timestamp_type},
                    total_messages INTEGER DEFAULT 0
                )""",

                
                f"""CREATE TABLE IF NOT EXISTS stats (
                    user_id {bigint_type} PRIMARY KEY,
                    games_played INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    ties INTEGER DEFAULT 0,
                    total_runs INTEGER DEFAULT 0,
                    total_balls_faced INTEGER DEFAULT 0,
                    high_score INTEGER DEFAULT 0,
                    avg_score {real_type} DEFAULT 0.0,
                    strike_rate {real_type} DEFAULT 0.0,
                    sixes_hit INTEGER DEFAULT 0,
                    fours_hit INTEGER DEFAULT 0,
                    centuries INTEGER DEFAULT 0,
                    fifties INTEGER DEFAULT 0,
                    ducks INTEGER DEFAULT 0,
                    current_winning_streak INTEGER DEFAULT 0,
                    longest_winning_streak INTEGER DEFAULT 0,
                    created_at {timestamp_type},
                    updated_at {timestamp_type}
                )""",
                
                f"""CREATE TABLE IF NOT EXISTS tournaments (
                    id {autoincrement},
                    name {text_type} NOT NULL,
                    type {text_type} NOT NULL,
                    theme {text_type} NOT NULL,
                    status {text_type} NOT NULL,
                    format {text_type} NOT NULL,
                    entry_fee INTEGER NOT NULL,
                    prize_pool INTEGER NOT NULL,
                    max_players INTEGER NOT NULL,
                    current_round INTEGER DEFAULT 1,
                    created_by {bigint_type},
                    created_at {timestamp_type},
                    starts_at {timestamp_type},
                    ends_at {timestamp_type},
                    winner_id {bigint_type},
                    runner_up_id {bigint_type},
                    brackets {text_type},
                    metadata {text_type}
                )""",

                f"""CREATE TABLE IF NOT EXISTS tournament_participants (
                    tournament_id INTEGER,
                    user_id {bigint_type},
                    position INTEGER,
                    joined_at {timestamp_type},
                    eliminated_at {timestamp_type},
                    PRIMARY KEY (tournament_id, user_id)
                )""",

                f"""CREATE TABLE IF NOT EXISTS daily_challenges (
                    id {autoincrement},
                    type {text_type} NOT NULL,
                    description {text_type} NOT NULL,
                    target INTEGER NOT NULL,
                    reward_coins INTEGER NOT NULL,
                    reward_xp INTEGER NOT NULL,
                    created_at {timestamp_type},
                    expires_at {timestamp_type}
                )""",

                f"""CREATE TABLE IF NOT EXISTS user_challenges (
                    user_id {bigint_type},
                    challenge_id INTEGER,
                    progress INTEGER DEFAULT 0,
                    completed {bool_type} DEFAULT {bool_default},
                    claimed {bool_type} DEFAULT {bool_default},
                    updated_at {timestamp_type},
                    PRIMARY KEY (user_id, challenge_id)
                )""",

                f"""CREATE TABLE IF NOT EXISTS user_levels (
                    user_id {bigint_type} PRIMARY KEY,
                    level INTEGER DEFAULT 1,
                    experience INTEGER DEFAULT 0,
                    next_level_xp INTEGER DEFAULT 100,
                    total_xp INTEGER DEFAULT 0,
                    prestige INTEGER DEFAULT 0,
                    created_at {timestamp_type},
                    updated_at {timestamp_type}
                )""",

                
                f"""CREATE TABLE IF NOT EXISTS user_sessions (
                    user_id {bigint_type} PRIMARY KEY,
                    session_data {text_type},
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                )""",

                f"""CREATE TABLE IF NOT EXISTS games (
                    chat_id {bigint_type} PRIMARY KEY,
                    state {text_type} DEFAULT 'toss',
                    innings INTEGER DEFAULT 1,
                    batting {text_type},
                    player_score INTEGER DEFAULT 0,
                    bot_score INTEGER DEFAULT 0,
                    player_wkts INTEGER DEFAULT 0,
                    bot_wkts INTEGER DEFAULT 0,
                    balls_in_over INTEGER DEFAULT 0,
                    overs_bowled INTEGER DEFAULT 0,
                    target INTEGER,
                    overs_limit INTEGER DEFAULT 2,
                    wickets_limit INTEGER DEFAULT 1,
                    match_format {text_type} DEFAULT 'T2',
                    difficulty_level {text_type} DEFAULT 'medium',
                    player_balls_faced INTEGER DEFAULT 0,
                    bot_balls_faced INTEGER DEFAULT 0,
                    player_fours INTEGER DEFAULT 0,
                    player_sixes INTEGER DEFAULT 0,
                    bot_fours INTEGER DEFAULT 0,
                    bot_sixes INTEGER DEFAULT 0,
                    extras INTEGER DEFAULT 0,
                    powerplay_overs INTEGER DEFAULT 0,
                    is_powerplay {bool_type} DEFAULT {bool_default},
                    weather_condition {text_type} DEFAULT 'clear',
                    pitch_condition {text_type} DEFAULT 'normal',
                    tournament_id INTEGER,
                    tournament_round INTEGER,
                    opponent_id {bigint_type},
                    is_tournament_match {bool_type} DEFAULT {bool_default},
                    created_at {timestamp_type},
                    updated_at {timestamp_type}
                )""",
                
                f"""CREATE TABLE IF NOT EXISTS history (
                    id {autoincrement},
                    chat_id {bigint_type},
                    event {text_type},
                    meta {text_type},
                    created_at {timestamp_type}
                )""",
                
                f"""CREATE TABLE IF NOT EXISTS match_history (
                    id {autoincrement},
                    chat_id {bigint_type},
                    user_id {bigint_type},
                    match_format {text_type},
                    player_score INTEGER,
                    bot_score INTEGER,
                    player_wickets INTEGER,
                    bot_wickets INTEGER,
                    overs_played {real_type},
                    result {text_type},
                    margin {text_type},
                    player_strike_rate {real_type},
                    match_duration_minutes INTEGER,
                    created_at {timestamp_type}
                )"""
            ]
            
            for table_sql in tables:
                cur.execute(table_sql)
                
        logger.info("=== BASE TABLES CREATED ===")
        
        # Create schema version table and run migrations
        create_schema_version_table()
        create_additional_tables()
        create_anticheat_tables()
        migrate_database()
        
        logger.info("=== MIGRATIONS COMPLETED ===")
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise


import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple

class AntiCheatSystem:
    """Comprehensive anti-cheat detection and prevention system"""
    
    # Thresholds for suspicious behavior
    THRESHOLDS = {
        'max_games_per_hour': 30,  # More than 30 games/hour is suspicious
        'max_wins_streak': 50,  # 50+ win streak needs review
        'impossible_score_rate': 300,  # 300+ runs in T2 is suspicious
        'similar_device_window': 3600,  # 1 hour window for device fingerprint check
        'max_accounts_per_device': 2,  # Max 2 accounts per device fingerprint
        'inhuman_reaction_time': 0.1,  # < 100ms response is bot-like
        'perfect_game_threshold': 5,  # 5 perfect games in a row is suspicious
        'stat_manipulation_threshold': 0.95  # 95% win rate is suspicious
    }
    
    # Ban durations in hours
    BAN_DURATIONS = {
        'warning': 0,  # Just a warning
        'temporary': 24,  # 24 hours
        'extended': 168,  # 7 days
        'permanent': 999999  # Permanent ban
    }
    
    @staticmethod
    def create_device_fingerprint(message) -> str:
        """Create a unique device fingerprint from user data"""
        # Combine various user/device attributes
        fingerprint_data = []
        
        if hasattr(message, 'from_user'):
            user = message.from_user
            # Use language code, is_bot status
            fingerprint_data.append(str(user.language_code or ''))
            fingerprint_data.append(str(user.is_bot))
        
        if hasattr(message, 'chat'):
            chat = message.chat
            fingerprint_data.append(str(chat.type))
        
        # Create hash from combined data
        fingerprint_str = '|'.join(fingerprint_data)
        return hashlib.md5(fingerprint_str.encode()).hexdigest()
    
    @staticmethod
    def check_user_behavior(user_id: int) -> Dict[str, any]:
        """
        Comprehensive behavior analysis for cheating detection
        Returns: dict with 'suspicious', 'violations', 'risk_score'
        """
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                param_style = "%s" if is_postgres else "?"
                
                violations = []
                risk_score = 0
                
                # Check 1: Games per hour
                one_hour_ago = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
                cur.execute(f"""
                    SELECT COUNT(*) as count FROM match_history 
                    WHERE user_id = {param_style} AND created_at > {param_style}
                """, (user_id, one_hour_ago))
                games_last_hour = cur.fetchone()['count']
                
                if games_last_hour > AntiCheatSystem.THRESHOLDS['max_games_per_hour']:
                    violations.append({
                        'type': 'excessive_games',
                        'severity': 'high',
                        'details': f'{games_last_hour} games in last hour'
                    })
                    risk_score += 30
                
                # Check 2: Impossible statistics
                cur.execute(f"""
                    SELECT * FROM stats WHERE user_id = {param_style}
                """, (user_id,))
                stats = cur.fetchone()
                
                if stats:
                    games_played = stats.get('games_played', 0)
                    wins = stats.get('wins', 0)
                    
                    if games_played > 10:  # Only check if sufficient games
                        win_rate = wins / games_played if games_played > 0 else 0
                        
                        # Unnaturally high win rate
                        if win_rate > AntiCheatSystem.THRESHOLDS['stat_manipulation_threshold']:
                            violations.append({
                                'type': 'impossible_win_rate',
                                'severity': 'critical',
                                'details': f'{win_rate*100:.1f}% win rate'
                            })
                            risk_score += 50
                        
                        # Check current win streak
                        current_streak = stats.get('current_winning_streak', 0)
                        if current_streak > AntiCheatSystem.THRESHOLDS['max_wins_streak']:
                            violations.append({
                                'type': 'impossible_streak',
                                'severity': 'high',
                                'details': f'{current_streak} game win streak'
                            })
                            risk_score += 40
                
                # Check 3: Multiple accounts from same device
                device_check = AntiCheatSystem._check_device_fingerprint(user_id)
                if device_check['multiple_accounts']:
                    violations.append({
                        'type': 'multiple_accounts',
                        'severity': 'critical',
                        'details': f"{device_check['account_count']} accounts on same device"
                    })
                    risk_score += 60
                
                # Check 4: Inhuman reaction times
                reaction_check = AntiCheatSystem._check_reaction_times(user_id)
                if reaction_check['bot_like_behavior']:
                    violations.append({
                        'type': 'bot_behavior',
                        'severity': 'critical',
                        'details': f"Avg reaction: {reaction_check['avg_reaction']:.3f}s"
                    })
                    risk_score += 70
                
                # Check 5: Impossible scores
                cur.execute(f"""
                    SELECT player_score, match_format, overs_played 
                    FROM match_history 
                    WHERE user_id = {param_style} 
                    ORDER BY created_at DESC LIMIT 10
                """, (user_id,))
                recent_matches = cur.fetchall()
                
                for match in recent_matches:
                    score = match.get('player_score', 0)
                    format_str = match.get('match_format', 'T2')
                    overs = int(format_str.replace('T', ''))
                    
                    # Maximum realistic score: ~18 runs per over
                    max_realistic = overs * 18
                    if score > max_realistic * 1.5:  # 50% tolerance
                        violations.append({
                            'type': 'impossible_score',
                            'severity': 'high',
                            'details': f'{score} runs in {format_str}'
                        })
                        risk_score += 35
                        break  # Only report once
                
                return {
                    'suspicious': risk_score >= 50,
                    'violations': violations,
                    'risk_score': min(risk_score, 100),  # Cap at 100
                    'user_id': user_id
                }
                
        except Exception as e:
            logger.error(f"Error checking user behavior: {e}")
            return {'suspicious': False, 'violations': [], 'risk_score': 0}
    
    @staticmethod
    def _check_device_fingerprint(user_id: int) -> Dict:
        """Check if multiple accounts using same device"""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                param_style = "%s" if is_postgres else "?"
                
                # Get user's device fingerprint
                cur.execute(f"""
                    SELECT device_fingerprint FROM user_devices 
                    WHERE user_id = {param_style}
                """, (user_id,))
                
                result = cur.fetchone()
                if not result:
                    return {'multiple_accounts': False, 'account_count': 1}
                
                fingerprint = result['device_fingerprint']
                
                # Check how many accounts use this fingerprint
                cur.execute(f"""
                    SELECT COUNT(DISTINCT user_id) as count 
                    FROM user_devices 
                    WHERE device_fingerprint = {param_style}
                """, (fingerprint,))
                
                count = cur.fetchone()['count']
                
                return {
                    'multiple_accounts': count > AntiCheatSystem.THRESHOLDS['max_accounts_per_device'],
                    'account_count': count
                }
        except Exception as e:
            logger.error(f"Error checking device fingerprint: {e}")
            return {'multiple_accounts': False, 'account_count': 1}
    
    @staticmethod
    def _check_reaction_times(user_id: int) -> Dict:
        """Check for bot-like inhuman reaction times"""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                param_style = "%s" if is_postgres else "?"
                
                # Get recent reaction times
                cur.execute(f"""
                    SELECT reaction_time FROM user_actions 
                    WHERE user_id = {param_style} 
                    ORDER BY created_at DESC LIMIT 50
                """, (user_id,))
                
                reaction_times = [row['reaction_time'] for row in cur.fetchall()]
                
                if not reaction_times:
                    return {'bot_like_behavior': False, 'avg_reaction': 1.0}
                
                avg_reaction = sum(reaction_times) / len(reaction_times)
                
                # Too fast = bot, too consistent = bot
                too_fast = avg_reaction < AntiCheatSystem.THRESHOLDS['inhuman_reaction_time']
                too_consistent = len(set(reaction_times)) < len(reaction_times) * 0.3
                
                return {
                    'bot_like_behavior': too_fast or too_consistent,
                    'avg_reaction': avg_reaction
                }
        except Exception as e:
            logger.error(f"Error checking reaction times: {e}")
            return {'bot_like_behavior': False, 'avg_reaction': 1.0}
    
    @staticmethod
    def ban_user(user_id: int, reason: str, duration_type: str = 'temporary', 
                 banned_by: int = None) -> bool:
        """Ban a user for cheating"""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                param_style = "%s" if is_postgres else "?"
                
                duration_hours = AntiCheatSystem.BAN_DURATIONS.get(duration_type, 24)
                banned_until = datetime.now(timezone.utc) + timedelta(hours=duration_hours)
                now = datetime.now(timezone.utc).isoformat()
                
                if is_postgres:
                    cur.execute("""
                        INSERT INTO user_bans 
                        (user_id, reason, banned_at, banned_until, banned_by, ban_type)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (user_id, reason, now, banned_until.isoformat(), banned_by, duration_type))
                else:
                    cur.execute("""
                        INSERT INTO user_bans 
                        (user_id, reason, banned_at, banned_until, banned_by, ban_type)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (user_id, reason, now, banned_until.isoformat(), banned_by, duration_type))
                
                # Log the ban
                logger.warning(f"User {user_id} banned: {reason} (type: {duration_type})")
                
                return True
        except Exception as e:
            logger.error(f"Error banning user: {e}")
            return False
    
    @staticmethod
    def is_user_banned(user_id: int) -> Tuple[bool, Dict]:
        """Check if user is currently banned"""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                param_style = "%s" if is_postgres else "?"
                
                now = datetime.now(timezone.utc).isoformat()
                
                cur.execute(f"""
                    SELECT * FROM user_bans 
                    WHERE user_id = {param_style} 
                    AND banned_until > {param_style}
                    ORDER BY banned_at DESC LIMIT 1
                """, (user_id, now))
                
                ban = cur.fetchone()
                
                if ban:
                    return True, {
                        'reason': ban['reason'],
                        'banned_until': ban['banned_until'],
                        'ban_type': ban['ban_type']
                    }
                
                return False, {}
        except Exception as e:
            logger.error(f"Error checking ban status: {e}")
            return False, {}
    
    @staticmethod
    def log_user_action(user_id: int, action_type: str, reaction_time: float = None):
        """Log user actions for behavior analysis"""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                now = datetime.now(timezone.utc).isoformat()
                
                if is_postgres:
                    cur.execute("""
                        INSERT INTO user_actions 
                        (user_id, action_type, reaction_time, created_at)
                        VALUES (%s, %s, %s, %s)
                    """, (user_id, action_type, reaction_time, now))
                else:
                    cur.execute("""
                        INSERT INTO user_actions 
                        (user_id, action_type, reaction_time, created_at)
                        VALUES (?, ?, ?, ?)
                    """, (user_id, action_type, reaction_time, now))
        except Exception as e:
            logger.error(f"Error logging user action: {e}")
    
    @staticmethod
    def record_device_fingerprint(user_id: int, fingerprint: str):
        """Store device fingerprint for multi-account detection"""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                now = datetime.now(timezone.utc).isoformat()
                
                if is_postgres:
                    cur.execute("""
                        INSERT INTO user_devices 
                        (user_id, device_fingerprint, first_seen, last_seen)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id, device_fingerprint) 
                        DO UPDATE SET last_seen = EXCLUDED.last_seen
                    """, (user_id, fingerprint, now, now))
                else:
                    cur.execute("""
                        INSERT OR REPLACE INTO user_devices 
                        (user_id, device_fingerprint, first_seen, last_seen)
                        VALUES (?, ?, ?, ?)
                    """, (user_id, fingerprint, now, now))
        except Exception as e:
            logger.error(f"Error recording device fingerprint: {e}")


# Database tables needed for anti-cheat (add to create_additional_tables function)
def create_anticheat_tables():
    """Create tables for anti-cheat system"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            if is_postgres:
                autoincrement = "SERIAL PRIMARY KEY"
                bigint = "BIGINT"
                text_type = "TEXT"
                real_type = "REAL"
                timestamp_type = "TIMESTAMPTZ"
            else:
                autoincrement = "INTEGER PRIMARY KEY AUTOINCREMENT"
                bigint = "INTEGER"
                text_type = "TEXT"
                real_type = "REAL"
                timestamp_type = "TEXT"
            
            # User bans table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS user_bans (
                    id {autoincrement},
                    user_id {bigint} NOT NULL,
                    reason {text_type} NOT NULL,
                    banned_at {timestamp_type} NOT NULL,
                    banned_until {timestamp_type} NOT NULL,
                    banned_by {bigint},
                    ban_type {text_type} DEFAULT 'temporary'
                )
            """)
            
            # User devices for fingerprinting
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS user_devices (
                    id {autoincrement},
                    user_id {bigint} NOT NULL,
                    device_fingerprint {text_type} NOT NULL,
                    first_seen {timestamp_type} NOT NULL,
                    last_seen {timestamp_type} NOT NULL,
                    UNIQUE(user_id, device_fingerprint)
                )
            """)
            
            # User actions log
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS user_actions (
                    id {autoincrement},
                    user_id {bigint} NOT NULL,
                    action_type {text_type} NOT NULL,
                    reaction_time {real_type},
                    created_at {timestamp_type} NOT NULL
                )
            """)
            
            # Anti-cheat reports
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS anticheat_reports (
                    id {autoincrement},
                    user_id {bigint} NOT NULL,
                    violation_type {text_type} NOT NULL,
                    severity {text_type} NOT NULL,
                    details {text_type},
                    risk_score INTEGER DEFAULT 0,
                    action_taken {text_type},
                    created_at {timestamp_type} NOT NULL
                )
            """)
            
            logger.info("Anti-cheat tables created successfully")
            
    except Exception as e:
        logger.error(f"Error creating anti-cheat tables: {e}")
        raise


# Decorator to check for bans before command execution
def check_ban(func):
    """Decorator to check if user is banned before executing command"""
    @wraps(func)
    def wrapper(message, *args, **kwargs):
        user_id = message.from_user.id
        is_banned, ban_info = AntiCheatSystem.is_user_banned(user_id)
        
        if is_banned:
            ban_msg = (
                f"🚫 <b>Account Suspended</b>\n\n"
                f"Reason: {ban_info.get('reason', 'Violation of rules')}\n"
                f"Ban Type: {ban_info.get('ban_type', 'temporary').title()}\n"
                f"Until: {ban_info.get('banned_until', 'Unknown')[:16]}\n\n"
                f"If you believe this is an error, contact support."
            )
            bot.send_message(message.chat.id, ban_msg)
            return None
        
        return func(message, *args, **kwargs)
    return wrapper


# Middleware to track actions and check for cheating
def anticheat_middleware(func):
    """Decorator to track user actions and detect cheating"""
    @wraps(func)
    def wrapper(message, *args, **kwargs):
        user_id = message.from_user.id
        action_start = time.time()
        
        # Record device fingerprint
        fingerprint = AntiCheatSystem.create_device_fingerprint(message)
        AntiCheatSystem.record_device_fingerprint(user_id, fingerprint)
        
        # Execute the function
        result = func(message, *args, **kwargs)
        
        # Calculate reaction time
        reaction_time = time.time() - action_start
        AntiCheatSystem.log_user_action(user_id, func.__name__, reaction_time)
        
        # Periodic behavior check (every 10th action)
        if random.random() < 0.1:  # 10% chance
            behavior = AntiCheatSystem.check_user_behavior(user_id)
            
            if behavior['suspicious']:
                # Log the suspicious activity
                try:
                    with get_db_connection() as conn:
                        cur = conn.cursor()
                        is_postgres = bool(os.getenv("DATABASE_URL"))
                        now = datetime.now(timezone.utc).isoformat()
                        
                        for violation in behavior['violations']:
                            if is_postgres:
                                cur.execute("""
                                    INSERT INTO anticheat_reports 
                                    (user_id, violation_type, severity, details, risk_score, created_at)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                """, (user_id, violation['type'], violation['severity'], 
                                      violation['details'], behavior['risk_score'], now))
                            else:
                                cur.execute("""
                                    INSERT INTO anticheat_reports 
                                    (user_id, violation_type, severity, details, risk_score, created_at)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                """, (user_id, violation['type'], violation['severity'], 
                                      violation['details'], behavior['risk_score'], now))
                except Exception as e:
                    logger.error(f"Error logging anticheat report: {e}")
                
                # Auto-ban for critical violations
                if behavior['risk_score'] >= 80:
                    AntiCheatSystem.ban_user(
                        user_id, 
                        f"Automated ban - Risk score: {behavior['risk_score']}", 
                        'extended'
                    )
                    bot.send_message(
                        message.chat.id,
                        "🚫 Your account has been suspended due to suspicious activity. "
                        "Contact support if you believe this is an error."
                    )
                elif behavior['risk_score'] >= 60:
                    # Warning
                    bot.send_message(
                        message.chat.id,
                        "⚠️ Warning: Suspicious activity detected on your account. "
                        "Continuing may result in a ban."
                    )
        
        return result
    return wrapper


class PowerUpType(Enum):
    BATTING_BOOST = "batting_boost"
    BOWLING_BOOST = "bowling_boost"
    LUCKY_CHARM = "lucky_charm"
    DOUBLE_COINS = "double_coins"
    XP_BOOST = "xp_boost"
    WICKET_GUARD = "wicket_guard"


class PowerUp:
    POWERUPS = {
        'batting_boost': {
            'name': '⚡ Batting Boost',
            'description': 'Increases boundary probability by 30%',
            'duration': 3,  # overs
            'cost': 50,
            'effect': {'type': 'boundary_chance', 'value': 0.3}
        },
        'bowling_boost': {
            'name': '🎯 Bowling Boost',
            'description': 'Increases wicket probability by 25%',
            'duration': 3,
            'cost': 50,
            'effect': {'type': 'wicket_chance', 'value': 0.25}
        },
        'lucky_charm': {
            'name': '🍀 Lucky Charm',
            'description': 'Reduces chance of getting out by 40%',
            'duration': 2,
            'cost': 75,
            'effect': {'type': 'wicket_protection', 'value': 0.4}
        },
        'double_coins': {
            'name': '💰 Double Coins',
            'description': 'Earn 2x coins from this match',
            'duration': 1,  # full match
            'cost': 100,
            'effect': {'type': 'coin_multiplier', 'value': 2.0}
        },
        'xp_boost': {
            'name': '📈 XP Boost',
            'description': 'Earn 1.5x XP from this match',
            'duration': 1,
            'cost': 80,
            'effect': {'type': 'xp_multiplier', 'value': 1.5}
        },
        'wicket_guard': {
            'name': '🛡️ Wicket Guard',
            'description': 'Save one wicket automatically',
            'duration': 1,
            'cost': 120,
            'effect': {'type': 'save_wicket', 'value': 1}
        }
    }
    
    @staticmethod
    def apply_powerup(game: 'GameState', powerup_id: str) -> bool:
        """Apply power-up to current game"""
        if powerup_id not in PowerUp.POWERUPS:
            return False
        
        powerup = PowerUp.POWERUPS[powerup_id]
        
        # Store active power-ups in game data
        active_powerups = game.data.get('active_powerups', {})
        active_powerups[powerup_id] = {
            'duration_remaining': powerup['duration'],
            'effect': powerup['effect']
        }
        game.data['active_powerups'] = active_powerups
        game.save()
        
        return True
    
    @staticmethod
    def check_powerup_effects(game: 'GameState', event_type: str) -> dict:
        """Check if any active power-ups affect current event"""
        active_powerups = game.data.get('active_powerups', {})
        effects = {}
        
        for powerup_id, powerup_data in active_powerups.items():
            effect = powerup_data.get('effect', {})
            effect_type = effect.get('type')
            
            if event_type == 'batting' and effect_type == 'boundary_chance':
                effects['boundary_bonus'] = effect.get('value', 0)
            elif event_type == 'bowling' and effect_type == 'wicket_chance':
                effects['wicket_bonus'] = effect.get('value', 0)
            elif event_type == 'wicket' and effect_type == 'wicket_protection':
                effects['wicket_save_chance'] = effect.get('value', 0)
            elif event_type == 'wicket' and effect_type == 'save_wicket':
                if powerup_data.get('duration_remaining', 0) > 0:
                    effects['auto_save_wicket'] = True
                    powerup_data['duration_remaining'] = 0  # Use up the power-up
        
        return effects
    
    @staticmethod
    def update_powerup_durations(game: 'GameState'):
        """Decrease power-up durations after each over"""
        active_powerups = game.data.get('active_powerups', {})
        expired = []
        
        for powerup_id, powerup_data in active_powerups.items():
            powerup_data['duration_remaining'] -= 1
            if powerup_data['duration_remaining'] <= 0:
                expired.append(powerup_id)
        
        for powerup_id in expired:
            del active_powerups[powerup_id]
        
        game.data['active_powerups'] = active_powerups
        game.save()


def kb_powerups_shop() -> types.InlineKeyboardMarkup:
    """Power-ups shop keyboard with info buttons"""
    kb = types.InlineKeyboardMarkup(row_width=2)
    
    for powerup_id, powerup in PowerUp.POWERUPS.items():
        # Purchase button
        buy_btn = types.InlineKeyboardButton(
            f"💰 {powerup['name'][:15]} - {powerup['cost']}c",
            callback_data=f"buy_powerup_{powerup_id}"
        )
        # Info button
        info_btn = types.InlineKeyboardButton(
            "ℹ️",
            callback_data=f"info_powerup_{powerup_id}"
        )
        kb.row(buy_btn, info_btn)
    
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
    return kb


def handle_powerup_purchase(user_id: int, powerup_id: str) -> dict:
    """Handle power-up purchase"""
    if powerup_id not in PowerUp.POWERUPS:
        return {'success': False, 'message': 'Invalid power-up'}
    
    powerup = PowerUp.POWERUPS[powerup_id]
    cost = powerup['cost']
    
    user_coins = _get_user_coins(user_id)
    
    if user_coins < cost:
        return {'success': False, 'message': f'Not enough coins! Need {cost}, have {user_coins}'}
    
    # Deduct coins
    _deduct_user_coins(user_id, cost)
    
    # Add to inventory
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            now = datetime.now(timezone.utc).isoformat()
            
            if is_postgres:
                cur.execute("""
                    INSERT INTO user_inventory (user_id, item_type, item_id, quantity, acquired_at)
                    VALUES (%s, 'powerup', %s, 1, %s)
                    ON CONFLICT (user_id, item_type, item_id)
                    DO UPDATE SET quantity = user_inventory.quantity + 1
                """, (user_id, powerup_id, now))
            else:
                # Check if exists
                cur.execute("""
                    SELECT quantity FROM user_inventory 
                    WHERE user_id = ? AND item_type = 'powerup' AND item_id = ?
                """, (user_id, powerup_id))
                
                row = cur.fetchone()
                if row:
                    cur.execute("""
                        UPDATE user_inventory SET quantity = quantity + 1
                        WHERE user_id = ? AND item_type = 'powerup' AND item_id = ?
                    """, (user_id, powerup_id))
                else:
                    cur.execute("""
                        INSERT INTO user_inventory (user_id, item_type, item_id, quantity, acquired_at)
                        VALUES (?, 'powerup', ?, 1, ?)
                    """, (user_id, powerup_id, now))
        
        return {
            'success': True,
            'message': f"✅ Purchased {powerup['name']}!\n{powerup['description']}"
        }
    except Exception as e:
        logger.error(f"Error purchasing power-up: {e}")
        return {'success': False, 'message': 'Purchase failed'}
    

class LeaderboardCategory(Enum):
    HIGHEST_SCORE = "highest_score"
    MOST_WINS = "most_wins"
    WIN_STREAK = "win_streak"
    MOST_SIXES = "most_sixes"
    BEST_STRIKE_RATE = "best_strike_rate"
    TOURNAMENT_WINS = "tournament_wins"
    TOTAL_XP = "total_xp"


def update_leaderboard(user_id: int, category: str, value: int):
    """Update user's leaderboard position"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            now = datetime.now(timezone.utc).isoformat()
            
            if is_postgres:
                cur.execute("""
                    INSERT INTO leaderboards (category, user_id, value, updated_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (category, user_id)
                    DO UPDATE SET value = GREATEST(leaderboards.value, EXCLUDED.value),
                                  updated_at = EXCLUDED.updated_at
                """, (category, user_id, value, now))
            else:
                cur.execute("""
                    SELECT value FROM leaderboards WHERE category = ? AND user_id = ?
                """, (category, user_id))
                
                row = cur.fetchone()
                current_value = row['value'] if row else 0
                
                if value > current_value:
                    if row:
                        cur.execute("""
                            UPDATE leaderboards SET value = ?, updated_at = ?
                            WHERE category = ? AND user_id = ?
                        """, (value, now, category, user_id))
                    else:
                        cur.execute("""
                            INSERT INTO leaderboards (category, user_id, value, updated_at)
                            VALUES (?, ?, ?, ?)
                        """, (category, user_id, value, now))
    except Exception as e:
        logger.error(f"Error updating leaderboard: {e}")


def get_leaderboard(category: str, limit: int = 10) -> list:
    """Get top players for a category"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            cur.execute(f"""
                SELECT l.user_id, l.value, u.username, u.first_name
                FROM leaderboards l
                JOIN users u ON l.user_id = u.user_id
                WHERE l.category = {param_style}
                ORDER BY l.value DESC
                LIMIT {param_style}
            """, (category, limit))
            
            return [dict(row) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        return []


def generate_leaderboard_display(category: str, top_players: list) -> str:
    """Create leaderboard display"""
    category_names = {
        'highest_score': '🏏 Highest Score',
        'most_wins': '🏆 Most Wins',
        'win_streak': '🔥 Longest Streak',
        'most_sixes': '🚀 Most Sixes',
        'best_strike_rate': '⚡ Best Strike Rate',
        'tournament_wins': '👑 Tournament Wins',
        'total_xp': '📈 Total XP'
    }
    
    display = (
        f"{'═'*50}\n"
        f" {category_names.get(category, category.upper())}\n"
        f"{'═'*50}\n\n"
    )
    
    if not top_players:
        return display + "No data available yet!"
    
    medals = ['🥇', '🥈', '🥉']
    
    for i, player in enumerate(top_players, 1):
        medal = medals[i-1] if i <= 3 else f"{i}."
        username = player.get('username') or player.get('first_name', 'Unknown')
        value = player.get('value', 0)
        
        display += f"{medal} {username}: {value}\n"
    
    return display


def kb_leaderboard_categories() -> types.InlineKeyboardMarkup:
    """Leaderboard category selector"""
    kb = types.InlineKeyboardMarkup(row_width=2)
    
    categories = [
        ("🏏 High Score", "lb_highest_score"),
        ("🏆 Most Wins", "lb_most_wins"),
        ("🔥 Win Streak", "lb_win_streak"),
        ("🚀 Most Sixes", "lb_most_sixes"),
        ("⚡ Strike Rate", "lb_best_strike_rate"),
        ("👑 Tournaments", "lb_tournament_wins"),
        ("📈 Total XP", "lb_total_xp")
    ]
    
    for text, callback in categories:
        kb.add(types.InlineKeyboardButton(text, callback_data=callback))
    
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
    return kb



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
        self.data['chat_id'] = chat_id
    
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
        game_data = default_game(self.chat_id)
        game_data['chat_id'] = self.chat_id  # Ensure chat_id is always set
        return game_data
        
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

class TournamentType(Enum):
    KNOCKOUT = "knockout"
    LEAGUE = "league"
    BRACKET = "bracket"
    DAILY = "daily"

class TournamentTheme(Enum):
    WORLD_CUP = "world_cup"
    IPL = "ipl"
    ASHES = "ashes"
    CHAMPIONS = "champions"
    CUSTOM = "custom"

class ChallengeType(Enum):
    SCORE = "score"
    SIXES = "sixes"
    WINS = "wins"
    STREAK = "streak"
    TOURNAMENT = "tournament"
    BOUNDARIES = "boundaries"
    PERFECT_OVER = "perfect_over"
    QUICK_FIRE = "quick_fire"

# ADD THESE NEW CLASSES AFTER YOUR EXISTING CLASSES:
class MatchInnings:
    """Single innings in a tournament match with proper cricket tracking"""
    def __init__(self, batting_team: str, overs_limit: int, wickets_limit: int):
        self.batting_team = batting_team
        self.overs_limit = overs_limit
        self.wickets_limit = wickets_limit
        self.runs = 0
        self.wickets = 0
        self.balls_faced = 0
        self.overs_completed = 0
        self.balls_in_over = 0
        self.fours = 0
        self.sixes = 0
        self.extras = 0
        self.powerplay_overs = max(2, overs_limit // 5)
        self.is_powerplay = True
        self.boundaries_timeline = []
        self.wicket_timeline = []
        self.momentum = 50  # 0-100 engagement metric
        
    def add_runs(self, runs: int, is_boundary: bool = False):
        self.runs += runs
        self.balls_faced += 1
        self.balls_in_over += 1
        
        if is_boundary:
            self.boundaries_timeline.append({
                'over': f"{self.overs_completed}.{self.balls_in_over}",
                'runs': runs
            })
            if runs == 4:
                self.fours += 1
                self.momentum = min(100, self.momentum + 8)
            elif runs == 6:
                self.sixes += 1
                self.momentum = min(100, self.momentum + 12)
        
        self._check_over_completion()
    
    def add_wicket(self):
        self.wickets += 1
        self.balls_faced += 1
        self.balls_in_over += 1
        self.wicket_timeline.append({
            'over': f"{self.overs_completed}.{self.balls_in_over}"
        })
        self.momentum = max(0, self.momentum - 15)
        self._check_over_completion()
    
    def _check_over_completion(self):
        if self.balls_in_over >= 6:
            self.balls_in_over = 0
            self.overs_completed += 1
            if self.overs_completed == self.powerplay_overs:
                self.is_powerplay = False
                self.momentum += 10
    
    def is_innings_complete(self) -> bool:
        if self.wickets >= self.wickets_limit:
            return True
        if self.overs_completed >= self.overs_limit:
            return True
        return False
    
    def get_strike_rate(self) -> float:
        if self.balls_faced == 0:
            return 0.0
        return (self.runs / self.balls_faced) * 100
    
    def get_run_rate(self) -> float:
        overs_faced = self.overs_completed + (self.balls_in_over / 6.0)
        if overs_faced == 0:
            return 0.0
        return self.runs / overs_faced
    
    def to_dict(self) -> Dict:
        return {
            'batting_team': self.batting_team,
            'overs_limit': self.overs_limit,
            'wickets_limit': self.wickets_limit,
            'runs': self.runs,
            'wickets': self.wickets,
            'balls_faced': self.balls_faced,
            'overs_completed': self.overs_completed,
            'balls_in_over': self.balls_in_over,
            'fours': self.fours,
            'sixes': self.sixes,
            'powerplay_overs': self.powerplay_overs,
            'is_powerplay': self.is_powerplay,
            'momentum': self.momentum
        }
    
    @classmethod
    def from_dict(cls, data: Dict):
        obj = cls(data['batting_team'], data['overs_limit'], data['wickets_limit'])
        obj.runs = data.get('runs', 0)
        obj.wickets = data.get('wickets', 0)
        obj.balls_faced = data.get('balls_faced', 0)
        obj.overs_completed = data.get('overs_completed', 0)
        obj.balls_in_over = data.get('balls_in_over', 0)
        obj.fours = data.get('fours', 0)
        obj.sixes = data.get('sixes', 0)
        obj.powerplay_overs = data.get('powerplay_overs', 0)
        obj.is_powerplay = data.get('is_powerplay', True)
        obj.momentum = data.get('momentum', 50)
        return obj


class TournamentMatch:
    """Complete match with full tournament integration"""
    def __init__(self, match_id: str, team1_id: int, team2_id: int, team1_name: str, 
                 team2_name: str, format_overs: int, format_wickets: int, tournament_stage: str):
        self.match_id = match_id
        self.team1 = {
            'id': team1_id,
            'name': team1_name,
            'emoji': random.choice(['🔴', '🔵', '🟡', '🟢', '🟣', '🟠'])
        }
        self.team2 = {
            'id': team2_id,
            'name': team2_name,
            'emoji': random.choice(['🔴', '🔵', '🟡', '🟢', '🟣', '🟠'])
        }
        
        # Ensure different emojis
        if self.team2['emoji'] == self.team1['emoji']:
            self.team2['emoji'] = random.choice([e for e in ['🔴', '🔵', '🟡', '🟢', '🟣', '🟠'] 
                                                 if e != self.team1['emoji']])
        
        self.format_overs = format_overs
        self.format_wickets = format_wickets
        self.tournament_stage = tournament_stage
        
        self.innings_1 = MatchInnings('team1', format_overs, format_wickets)
        self.innings_2 = None
        self.current_innings = 1
        self.match_state = 'not_started'  # not_started, innings_1, innings_2, completed
        
        self.toss_winner = None
        self.batting_first = None
        self.winner = None
        self.margin = None
        self.margin_type = None
        
        self.ball_count = 0
        self.key_moments = []
        self.weather = random.choice(['clear', 'overcast', 'windy', 'light_rain'])
        self.pitch = random.choice(['flat', 'bowler_friendly', 'spin_friendly'])
        
        self.created_at = datetime.now(timezone.utc).isoformat()
        self.started_at = None
        self.ended_at = None
    
    def start_match(self, toss_winner: str, batting_first: str):
        self.toss_winner = toss_winner
        self.batting_first = batting_first
        self.match_state = 'innings_1'
        self.started_at = datetime.now(timezone.utc).isoformat()
    
    def record_ball(self, outcome: str, runs: int = 0) -> Dict[str, Any]:
        """Record a delivery - outcome: 'runs', 'wicket', or 'dot'"""
        self.ball_count += 1
        current_innings = self.innings_1 if self.current_innings == 1 else self.innings_2
        
        if outcome == 'runs':
            is_boundary = runs in [4, 6]
            current_innings.add_runs(runs, is_boundary)
            if runs == 4:
                self.key_moments.append({'type': 'boundary_4', 'ball': self.ball_count})
            elif runs == 6:
                self.key_moments.append({'type': 'boundary_6', 'ball': self.ball_count})
        
        elif outcome == 'wicket':
            current_innings.add_wicket()
            self.key_moments.append({'type': 'wicket', 'ball': self.ball_count})
        
        elif outcome == 'dot':
            current_innings.balls_faced += 1
            current_innings.balls_in_over += 1
            current_innings._check_over_completion()
        
        # Check if innings complete
        if current_innings.is_innings_complete():
            return self._handle_innings_end()
        
        return {
            'success': True,
            'ball_recorded': True,
            'current_score': current_innings.runs,
            'current_wickets': current_innings.wickets,
            'momentum': current_innings.momentum
        }
    
    def _handle_innings_end(self) -> Dict[str, Any]:
        if self.current_innings == 1:
            self.innings_2 = MatchInnings('team2', self.format_overs, self.format_wickets)
            self.current_innings = 2
            self.match_state = 'innings_2'
            
            return {
                'innings_ended': True,
                'innings_number': 1,
                'runs_scored': self.innings_1.runs,
                'wickets_lost': self.innings_1.wickets,
                'next_state': 'innings_2_start',
                'target': self.innings_1.runs + 1
            }
        else:
            return self._determine_winner()
    
    def _determine_winner(self) -> Dict[str, Any]:
        self.match_state = 'completed'
        self.ended_at = datetime.now(timezone.utc).isoformat()
        
        team1_runs = self.innings_1.runs
        team2_runs = self.innings_2.runs
        
        if team2_runs > team1_runs:
            self.winner = 'team2'
            self.margin = team2_runs - team1_runs
            self.margin_type = 'runs'
        elif team1_runs > team2_runs:
            self.winner = 'team1'
            self.margin = self.format_wickets - self.innings_2.wickets
            self.margin_type = 'wickets'
        else:
            self.winner = 'tie'
            self.margin = 0
            self.margin_type = 'tie'
        
        return {
            'match_completed': True,
            'winner': self.winner,
            'margin': self.margin,
            'margin_type': self.margin_type,
            'team1_score': f"{team1_runs}/{self.innings_1.wickets}",
            'team2_score': f"{team2_runs}/{self.innings_2.wickets}"
        }
    
    def get_scorecard(self) -> str:
        scorecard = (
            f"{'='*50}\n"
            f"  {self.tournament_stage} - {self.match_id}\n"
            f"{'='*50}\n\n"
            f"{self.team1['emoji']} {self.team1['name']}\n"
            f"  {self.innings_1.runs}/{self.innings_1.wickets} "
            f"({self.innings_1.overs_completed}.{self.innings_1.balls_in_over} ov)\n"
            f"  4️⃣: {self.innings_1.fours} | 6️⃣: {self.innings_1.sixes} | "
            f"SR: {self.innings_1.get_strike_rate():.1f}%\n\n"
            f"{self.team2['emoji']} {self.team2['name']}\n"
        )
        
        if self.innings_2:
            scorecard += (
                f"  {self.innings_2.runs}/{self.innings_2.wickets} "
                f"({self.innings_2.overs_completed}.{self.innings_2.balls_in_over} ov)\n"
                f"  4️⃣: {self.innings_2.fours} | 6️⃣: {self.innings_2.sixes} | "
                f"SR: {self.innings_2.get_strike_rate():.1f}%\n"
            )
            
            if self.match_state == 'innings_2':
                target = self.innings_1.runs + 1
                need = max(0, target - self.innings_2.runs)
                scorecard += f"\n  🎯 Target: {target} | Need: {need}\n"
        
        if self.match_state == 'completed':
            scorecard += f"\n{'='*50}\n"
            if self.winner == 'team1':
                scorecard += f"🏆 {self.team1['name']} wins by {self.margin} {self.margin_type}!\n"
            elif self.winner == 'team2':
                scorecard += f"🏆 {self.team2['name']} wins by {self.margin} {self.margin_type}!\n"
            else:
                scorecard += f"🤝 Match Tied!\n"
            scorecard += f"{'='*50}"
        
        return scorecard
    
    def to_dict(self) -> Dict:
        return {
            'match_id': self.match_id,
            'team1': self.team1,
            'team2': self.team2,
            'format_overs': self.format_overs,
            'format_wickets': self.format_wickets,
            'tournament_stage': self.tournament_stage,
            'innings_1': self.innings_1.to_dict(),
            'innings_2': self.innings_2.to_dict() if self.innings_2 else None,
            'current_innings': self.current_innings,
            'match_state': self.match_state,
            'toss_winner': self.toss_winner,
            'batting_first': self.batting_first,
            'winner': self.winner,
            'margin': self.margin,
            'margin_type': self.margin_type,
            'ball_count': self.ball_count,
            'key_moments': self.key_moments,
            'weather': self.weather,
            'pitch': self.pitch,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'ended_at': self.ended_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict):
        obj = cls(
            data['match_id'],
            data['team1']['id'],
            data['team2']['id'],
            data['team1']['name'],
            data['team2']['name'],
            data['format_overs'],
            data['format_wickets'],
            data['tournament_stage']
        )
        obj.team1 = data['team1']
        obj.team2 = data['team2']
        obj.innings_1 = MatchInnings.from_dict(data['innings_1'])
        obj.innings_2 = MatchInnings.from_dict(data['innings_2']) if data.get('innings_2') else None
        obj.current_innings = data.get('current_innings', 1)
        obj.match_state = data.get('match_state', 'not_started')
        obj.toss_winner = data.get('toss_winner')
        obj.batting_first = data.get('batting_first')
        obj.winner = data.get('winner')
        obj.margin = data.get('margin')
        obj.margin_type = data.get('margin_type')
        obj.ball_count = data.get('ball_count', 0)
        obj.key_moments = data.get('key_moments', [])
        obj.weather = data.get('weather', 'clear')
        obj.pitch = data.get('pitch', 'flat')
        obj.created_at = data.get('created_at')
        obj.started_at = data.get('started_at')
        obj.ended_at = data.get('ended_at')
        return obj


class EliteTournament:
    """Main tournament orchestration"""
    def __init__(self, tournament_id: str, name: str, tournament_type: str, theme: str, 
                 format_overs: int, format_wickets: int, created_by: int):
        self.tournament_id = tournament_id
        self.name = name
        self.type = tournament_type  # 'knockout' or 'league'
        self.theme = theme
        self.format_overs = format_overs
        self.format_wickets = format_wickets
        self.created_by = created_by
        
        self.participants = []
        self.matches = []
        self.current_round = 1
        self.total_rounds = 0
        self.tournament_state = 'registration'  # registration, live, completed
        
        self.bracket = {}  # For knockout
        self.standings = {}
        
        self.created_at = datetime.now(timezone.utc).isoformat()
        self.started_at = None
        self.ended_at = None
        
        self.theme_data = self._get_theme_data()
        self.records = {
            'highest_score': 0,
            'fastest_fifty': 0,
            'most_sixes': 0,
            'most_boundaries': 0
        }
    
    def _get_theme_data(self) -> Dict:
        themes = {
            'world_cup': {
                'emoji': '🌍',
                'colors': ['🔵', '🟢', '🔴', '🟡'],
                'trophy': '🏆',
                'stage_names': {
                    1: 'Group Stage', 2: 'Quarter Finals', 3: 'Semi Finals', 4: 'Final'
                },
                'teams': ['India', 'Australia', 'England', 'Pakistan', 'South Africa', 
                         'New Zealand', 'Sri Lanka', 'Bangladesh']
            },
            'ipl': {
                'emoji': '🇮🇳',
                'colors': ['🔵', '🟡', '🔴', '🟢', '🟣', '🟠'],
                'trophy': '💎',
                'stage_names': {
                    1: 'League Stage', 2: 'Qualifiers', 3: 'Final'
                },
                'teams': ['Mumbai', 'Chennai', 'Bangalore', 'Delhi', 'Kolkata', 
                         'Hyderabad', 'Punjab', 'Rajasthan']
            },
            'champions': {
                'emoji': '⚡',
                'colors': ['🟡', '🔵', '🔴', '🟢'],
                'trophy': '👑',
                'stage_names': {
                    1: 'Round 1', 2: 'Semi Final', 3: 'Championship'
                },
                'teams': ['Thunder', 'Phoenix', 'Dragons', 'Eagles', 'Tigers', 'Warriors']
            }
        }
        return themes.get(self.theme, themes['world_cup'])
    
    def add_participant(self, user_id: int, username: str) -> Dict:
        if len(self.participants) >= 16:
            return {'success': False, 'message': 'Tournament is full'}
        
        if any(p['user_id'] == user_id for p in self.participants):
            return {'success': False, 'message': 'Already registered'}
        
        participant = {
            'user_id': user_id,
            'username': username,
            'avatar': random.choice(self.theme_data['colors']),
            'matches_played': 0,
            'wins': 0,
            'losses': 0,
            'ties': 0,
            'runs_scored': 0,
            'wickets_taken': 0,
            'highest_score': 0
        }
        
        self.participants.append(participant)
        self.standings[user_id] = {
            'points': 0,
            'matches': 0,
            'wins': 0,
            'losses': 0,
            'nrr': 0.0
        }
        
        return {
            'success': True,
            'message': f'Welcome to {self.name}!',
            'participants': f'{len(self.participants)}/16'
        }
    
    def start_tournament(self) -> Dict:
        if len(self.participants) < 2:
            return {'success': False, 'message': 'Need at least 2 participants'}
        
        self.tournament_state = 'live'
        self.started_at = datetime.now(timezone.utc).isoformat()
        
        if self.type == 'knockout':
            self._create_knockout_bracket()
        else:
            self._create_league()
        
        return {'success': True, 'message': 'Tournament started!'}
    
    def _create_knockout_bracket(self):
        num_teams = len(self.participants)
        num_rounds = int(math.ceil(math.log2(num_teams)))
        self.total_rounds = num_rounds
        
        self.bracket = {'rounds': {}}
        
        for round_num in range(1, num_rounds + 1):
            matches_in_round = 2 ** (num_rounds - round_num)
            stage_name = self.theme_data['stage_names'].get(
                round_num, f'Round {round_num}'
            )
            
            self.bracket['rounds'][round_num] = {
                'matches': [],
                'stage': stage_name
            }
            
            for match_num in range(matches_in_round):
                # In first round, pair consecutive participants
                if round_num == 1:
                    team1_idx = match_num * 2
                    team2_idx = match_num * 2 + 1
                    
                    if team1_idx < len(self.participants) and team2_idx < len(self.participants):
                        team1 = self.participants[team1_idx]
                        team2 = self.participants[team2_idx]
                        
                        match = TournamentMatch(
                            f"R{round_num}M{match_num + 1}",
                            team1['user_id'],
                            team2['user_id'],
                            team1['username'],
                            team2['username'],
                            self.format_overs,
                            self.format_wickets,
                            stage_name
                        )
                        self.bracket['rounds'][round_num]['matches'].append(match)
                        self.matches.append(match)
                else:
                    # Subsequent rounds will be filled as previous round completes
                    match = TournamentMatch(
                        f"R{round_num}M{match_num + 1}",
                        None,
                        None,
                        f"Winner {(match_num * 2) + 1}",
                        f"Winner {(match_num * 2) + 2}",
                        self.format_overs,
                        self.format_wickets,
                        stage_name
                    )
                    self.bracket['rounds'][round_num]['matches'].append(match)
                    self.matches.append(match)
    
    def _create_league(self):
        num_teams = len(self.participants)
        
        for i in range(num_teams):
            for j in range(i + 1, num_teams):
                team1 = self.participants[i]
                team2 = self.participants[j]
                
                match = TournamentMatch(
                    f"L{len(self.matches) + 1}",
                    team1['user_id'],
                    team2['user_id'],
                    team1['username'],
                    team2['username'],
                    self.format_overs,
                    self.format_wickets,
                    'League'
                )
                self.matches.append(match)
        
        self.total_rounds = 1
    
    def get_standings(self) -> str:
        sorted_standings = sorted(
            [(p, self.standings.get(p['user_id'], {})) for p in self.participants],
            key=lambda x: (x[1].get('points', 0), x[1].get('wins', 0)),
            reverse=True
        )
        
        standings_text = (
            f"{'='*50}\n"
            f"  {self.name.upper()}\n"
            f"  Status: {self.tournament_state.upper()}\n"
            f"{'='*50}\n\n"
        )
        
        for idx, (participant, stats) in enumerate(sorted_standings, 1):
            medal = '🥇' if idx == 1 else '🥈' if idx == 2 else '🥉' if idx == 3 else f'{idx}.'
            points = stats.get('points', 0)
            wins = stats.get('wins', 0)
            
            standings_text += (
                f"{medal} {participant['avatar']} {participant['username']}\n"
                f"   Points: {points} | Wins: {wins} | Matches: {stats.get('matches', 0)}\n"
            )
        
        return standings_text
    
    def update_standings_after_match(self, match: TournamentMatch):
        """Update tournament standings after match completion"""
        if match.winner == 'team1':
            self._award_points(match.team1['id'], 2)  # 2 points for win
            self._increment_wins(match.team1['id'])
        elif match.winner == 'team2':
            self._award_points(match.team2['id'], 2)
            self._increment_wins(match.team2['id'])
        else:
            self._award_points(match.team1['id'], 1)  # 1 point for tie
            self._award_points(match.team2['id'], 1)
    
    def _award_points(self, user_id: int, points: int):
        if user_id in self.standings:
            self.standings[user_id]['points'] += points
            self.standings[user_id]['matches'] += 1
    
    def _increment_wins(self, user_id: int):
        if user_id in self.standings:
            self.standings[user_id]['wins'] += 1
    
    def to_dict(self) -> Dict:
        return {
            'tournament_id': self.tournament_id,
            'name': self.name,
            'type': self.type,
            'theme': self.theme,
            'format_overs': self.format_overs,
            'format_wickets': self.format_wickets,
            'created_by': self.created_by,
            'participants': self.participants,
            'current_round': self.current_round,
            'total_rounds': self.total_rounds,
            'tournament_state': self.tournament_state,
            'matches': [m.to_dict() for m in self.matches],
            'standings': self.standings,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'ended_at': self.ended_at,
            'records': self.records
        }
    
    @classmethod
    def from_dict(cls, data: Dict):
        obj = cls(
            data['tournament_id'],
            data['name'],
            data['type'],
            data['theme'],
            data['format_overs'],
            data['format_wickets'],
            data['created_by']
        )
        obj.participants = data.get('participants', [])
        obj.current_round = data.get('current_round', 1)
        obj.total_rounds = data.get('total_rounds', 0)
        obj.tournament_state = data.get('tournament_state', 'registration')
        obj.matches = [TournamentMatch.from_dict(m) for m in data.get('matches', [])]
        obj.standings = data.get('standings', {})
        obj.created_at = data.get('created_at')
        obj.started_at = data.get('started_at')
        obj.ended_at = data.get('ended_at')
        obj.records = data.get('records', {})
        return obj


class DailyChallenge:
    def __init__(self, challenge_id: int = None):
        self.id = challenge_id
        self.type = ChallengeType.SCORE
        self.description = ""
        self.target = 0
        self.reward_coins = 0
        self.reward_xp = 0
        self.difficulty = "medium"
        self.icon = "🎯"
        self.expires_at = None
        self.created_at = datetime.now(timezone.utc)
    
    @classmethod
    def generate_daily_challenges(cls, date_str: str = None) -> list:
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        random.seed(date_str)
        challenges = []
        
        challenge_templates = [
            {
                "type": ChallengeType.SCORE,
                "easy": {"target": 30, "coins": 25, "xp": 50, "desc": "Score 30+ runs in a single match"},
                "medium": {"target": 50, "coins": 50, "xp": 100, "desc": "Score a half-century (50+ runs)"},
                "hard": {"target": 100, "coins": 100, "xp": 200, "desc": "Score a century (100+ runs)"}
            },
            {
                "type": ChallengeType.SIXES,
                "easy": {"target": 3, "coins": 20, "xp": 40, "desc": "Hit 3 sixes in a single match"},
                "medium": {"target": 5, "coins": 40, "xp": 80, "desc": "Hit 5 sixes in a single match"},
                "hard": {"target": 10, "coins": 80, "xp": 160, "desc": "Hit 10 sixes in a single match"}
            },
            {
                "type": ChallengeType.WINS,
                "easy": {"target": 1, "coins": 15, "xp": 30, "desc": "Win 1 match today"},
                "medium": {"target": 3, "coins": 35, "xp": 70, "desc": "Win 3 matches today"},
                "hard": {"target": 5, "coins": 70, "xp": 140, "desc": "Win 5 matches today"}
            },
            {
                "type": ChallengeType.STREAK,
                "easy": {"target": 2, "coins": 30, "xp": 60, "desc": "Win 2 matches in a row"},
                "medium": {"target": 3, "coins": 60, "xp": 120, "desc": "Win 3 matches in a row"},
                "hard": {"target": 5, "coins": 120, "xp": 240, "desc": "Win 5 matches in a row"}
            },
            {
                "type": ChallengeType.BOUNDARIES,
                "easy": {"target": 5, "coins": 20, "xp": 40, "desc": "Hit 5 boundaries (4s + 6s) in a match"},
                "medium": {"target": 8, "coins": 40, "xp": 80, "desc": "Hit 8 boundaries in a match"},
                "hard": {"target": 12, "coins": 80, "xp": 160, "desc": "Hit 12 boundaries in a match"}
            }
        ]
        
        selected_templates = random.sample(challenge_templates, 3)
        difficulties = ["easy", "medium", "hard"]
        random.shuffle(difficulties)
        
        for i, template in enumerate(selected_templates):
            difficulty = difficulties[i]
            config = template[difficulty]
            
            challenge = cls()
            challenge.type = template["type"]
            challenge.description = config["desc"]
            challenge.target = config["target"]
            challenge.reward_coins = config["coins"]
            challenge.reward_xp = config["xp"]
            challenge.difficulty = difficulty
            challenge.icon = cls._get_challenge_icon(template["type"])
            challenge.expires_at = datetime.now(timezone.utc) + timedelta(days=1)
            
            challenges.append(challenge)
        
        return challenges
    
    @classmethod
    def _get_challenge_icon(cls, challenge_type: ChallengeType) -> str:
        icons = {
            ChallengeType.SCORE: "📊",
            ChallengeType.SIXES: "🚀",
            ChallengeType.WINS: "🏆",
            ChallengeType.STREAK: "🔥",
            ChallengeType.TOURNAMENT: "🏅",
            ChallengeType.BOUNDARIES: "⚡",
            ChallengeType.PERFECT_OVER: "🎯",
            ChallengeType.QUICK_FIRE: "⏱️"
        }
        return icons.get(challenge_type, "🎯")

class ChallengeTracker:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.active_challenges = []
        self.progress = {}
        self.daily_stats = {}
        self._load_challenges()
    
    def _load_challenges(self):
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                
                if is_postgres:
                    cur.execute("""
                        SELECT dc.*, uc.progress, uc.completed, uc.claimed 
                        FROM daily_challenges dc
                        LEFT JOIN user_challenges uc ON dc.id = uc.challenge_id AND uc.user_id = %s
                        WHERE dc.expires_at > %s
                    """, (self.user_id, datetime.now(timezone.utc).isoformat()))
                else:
                    cur.execute("""
                        SELECT dc.*, uc.progress, uc.completed, uc.claimed 
                        FROM daily_challenges dc
                        LEFT JOIN user_challenges uc ON dc.id = uc.challenge_id AND uc.user_id = ?
                        WHERE dc.expires_at > ?
                    """, (self.user_id, datetime.now(timezone.utc).isoformat()))
                
                challenges = cur.fetchall()
                self.active_challenges = [dict(c) for c in challenges]
                
                for challenge in self.active_challenges:
                    challenge_id = challenge["id"]
                    self.progress[challenge_id] = challenge.get("progress", 0)
                
        except Exception as e:
            logger.error(f"Error loading challenges for user {self.user_id}: {e}")
    
    def update_progress(self, challenge_type: ChallengeType, value: int, match_data: dict = None):
        try:
            relevant_challenges = [c for c in self.active_challenges 
                                 if c["type"] == challenge_type.value and not c.get("completed", False)]
            
            for challenge in relevant_challenges:
                challenge_id = challenge["id"]
                current_progress = self.progress.get(challenge_id, 0)
                
                if challenge_type in [ChallengeType.SCORE, ChallengeType.SIXES, ChallengeType.BOUNDARIES]:
                    if value > current_progress:
                        self.progress[challenge_id] = value
                elif challenge_type in [ChallengeType.WINS, ChallengeType.STREAK]:
                    if challenge_type == ChallengeType.WINS:
                        self.progress[challenge_id] = current_progress + value
                    else:
                        if value > 0:
                            self.progress[challenge_id] = current_progress + 1
                        else:
                            self.progress[challenge_id] = 0
                
                self._save_progress(challenge_id, self.progress[challenge_id])
                
        except Exception as e:
            logger.error(f"Error updating challenge progress: {e}")
    
    def check_completion(self) -> list:
        completed_challenges = []
        
        for challenge in self.active_challenges:
            challenge_id = challenge["id"]
            current_progress = self.progress.get(challenge_id, 0)
            target = challenge["target"]
            
            if current_progress >= target and not challenge.get("completed", False):
                self._mark_completed(challenge_id)
                challenge["completed"] = True
                challenge["progress"] = current_progress
                completed_challenges.append(challenge)
        
        return completed_challenges
    
    def _save_progress(self, challenge_id: int, progress: int):
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                now = datetime.now(timezone.utc).isoformat()
                
                if is_postgres:
                    cur.execute("""
                        INSERT INTO user_challenges (user_id, challenge_id, progress, updated_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id, challenge_id)
                        DO UPDATE SET progress = EXCLUDED.progress, updated_at = EXCLUDED.updated_at
                    """, (self.user_id, challenge_id, progress, now))
                else:
                    cur.execute("""
                        INSERT OR REPLACE INTO user_challenges 
                        (user_id, challenge_id, progress, updated_at)
                        VALUES (?, ?, ?, ?)
                    """, (self.user_id, challenge_id, progress, now))
                    
        except Exception as e:
            logger.error(f"Error saving challenge progress: {e}")
    
    def _mark_completed(self, challenge_id: int):
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                
                if is_postgres:
                    cur.execute("""
                        UPDATE user_challenges 
                        SET completed = TRUE, updated_at = %s 
                        WHERE user_id = %s AND challenge_id = %s
                    """, (datetime.now(timezone.utc).isoformat(), self.user_id, challenge_id))
                else:
                    cur.execute("""
                        UPDATE user_challenges 
                        SET completed = 1, updated_at = ? 
                        WHERE user_id = ? AND challenge_id = ?
                    """, (datetime.now(timezone.utc).isoformat(), self.user_id, challenge_id))
                    
        except Exception as e:
            logger.error(f"Error marking challenge completed: {e}")

class AnimationManager:
    CRICKET_GIFS = {
        "six": [
            "https://media.giphy.com/media/3o7abrE7ZDWOzR3HkY/giphy.gif",
            "https://media.giphy.com/media/26gsjCZpPolPr3sBy/giphy.gif"
        ],
        "four": [
            "https://media.giphy.com/media/xT9IgG50Fb7Mi0prBC/giphy.gif",
            "https://media.giphy.com/media/3o7abrH8o4HMKOvleE/giphy.gif"
        ],
        "wicket": [
            "https://media.giphy.com/media/26gsjCZpPolPr3sBy/giphy.gif",
            "https://media.giphy.com/media/xT9IgG50Fb7Mi0prBC/giphy.gif"
        ],
        "century": "https://media.giphy.com/media/26gsjCZpPolPr3sBy/giphy.gif",
        "victory": "https://media.giphy.com/media/xT9IgG50Fb7Mi0prBC/giphy.gif",
        "tournament_win": "https://media.giphy.com/media/26gsjCZpPolPr3sBy/giphy.gif"
    }
    
    ASCII_ANIMATIONS = {
        "six": [
            "🚀💫⭐ MAXIMUM! ⭐💫🚀",
            "   🏏━━━━━━━━━━━━━▶ ⚾",
            "        SIX RUNS!",
            "   🎯🎯🎯🎯🎯🎯"
        ],
        "four": [
            "⚡⚡⚡ BOUNDARY! ⚡⚡⚡",
            "   🏏━━━━━━━━━▶ ⚾",
            "      FOUR RUNS!",
            "   🎯🎯🎯🎯"
        ],
        "wicket": [
            "💥💥💥 WICKET! 💥💥💥",
            "   🎯🔥🎯 STUMPS DOWN 🎯🔥🎯",
            "      OUT! OUT! OUT!",
            "   👨‍⚖️ ✋ DISMISSED! ✋ 👨‍⚖️"
        ],
        "century": [
            "🎉🎉🎉 CENTURY! 🎉🎉🎉",
            "   💯 100 NOT OUT! 💯",
            "  🏆 MAGNIFICENT INNINGS 🏆",
            "🎊🎊🎊🎊🎊🎊🎊🎊🎊"
        ]
    }
    
    @staticmethod
    def send_animation(chat_id: int, event_type: str, caption: str = ""):
        try:
            if AnimationManager._send_gif_animation(chat_id, event_type, caption):
                return True
            if AnimationManager._send_ascii_animation(chat_id, event_type, caption):
                return True
            return AnimationManager._send_emoji_animation(chat_id, event_type, caption)
        except Exception as e:
            logger.error(f"All animation methods failed for {event_type}: {e}")
            return False
    
    @staticmethod
    def _send_gif_animation(chat_id: int, event_type: str, caption: str = "") -> bool:
        try:
            if not bot:
                return False
            gif_urls = AnimationManager.CRICKET_GIFS.get(event_type)
            if gif_urls:
                if isinstance(gif_urls, list):
                    gif_url = random.choice(gif_urls)
                else:
                    gif_url = gif_urls
                
                bot.send_animation(
                    chat_id, 
                    gif_url, 
                    caption=caption,
                    parse_mode="HTML"
                )
                return True
        except Exception as e:
            logger.debug(f"GIF animation failed for {event_type}: {e}")
        return False
    
    @staticmethod
    def _send_ascii_animation(chat_id: int, event_type: str, caption: str = "") -> bool:
        try:
            if not bot:
                return False
            ascii_frames = AnimationManager.ASCII_ANIMATIONS.get(event_type)
            if ascii_frames:
                animation_text = "\n".join(ascii_frames)
                if caption:
                    animation_text = f"{caption}\n\n{animation_text}"
                
                bot.send_message(chat_id, f"<pre>{animation_text}</pre>", parse_mode="HTML")
                return True
        except Exception as e:
            logger.debug(f"ASCII animation failed for {event_type}: {e}")
        return False
    
    @staticmethod
    def _send_emoji_animation(chat_id: int, event_type: str, caption: str = "") -> bool:
        try:
            if not bot:
                return False
            emoji_map = {
                "six": "🚀",
                "four": "⚡",
                "wicket": "💥",
                "century": "💯",
                "victory": "🏆",
                "tournament_win": "👑"
            }
            
            emoji = emoji_map.get(event_type, "🎯")
            message = f"{emoji} {caption}" if caption else emoji
            bot.send_message(chat_id, message)
            return True
        except Exception as e:
            logger.error(f"Even emoji animation failed for {event_type}: {e}")
        return False

class UserLevelManager:
    LEVEL_XP_REQUIREMENTS = {i: int(100 * (1.5 ** (i-1))) for i in range(1, 101)}
    
    LEVEL_REWARDS = {
        5: {"coins": 100, "title": "Rising Star"},
        10: {"coins": 250, "title": "Promising Player"},
        15: {"coins": 500, "title": "Skilled Batsman"},
        20: {"coins": 1000, "title": "Cricket Veteran"},
        25: {"coins": 2000, "title": "Master Player"},
        30: {"coins": 3500, "title": "Cricket Legend"},
        40: {"coins": 5000, "title": "Hall of Famer"},
        50: {"coins": 10000, "title": "Cricket God"}
    }
    
    @staticmethod
    def calculate_match_xp(game_data: dict, result: str) -> int:
        base_xp = 15
        
        if result == "win":
            base_xp += 25
        elif result == "tie":
            base_xp += 10
        
        player_score = game_data.get("player_score", 0)
        base_xp += min(player_score // 5, 50)
        
        if player_score >= 100:
            base_xp += 100
        elif player_score >= 50:
            base_xp += 50
        
        base_xp += game_data.get("player_sixes", 0) * 8
        base_xp += game_data.get("player_fours", 0) * 4
        
        format_multipliers = {
            "T1": 0.8, "T2": 1.0, "T5": 1.3, "T10": 1.6, "T20": 2.0
        }
        
        match_format = game_data.get("match_format", "T2")
        base_xp = int(base_xp * format_multipliers.get(match_format, 1.0))
        
        difficulty_multipliers = {
            "easy": 0.8, "medium": 1.0, "hard": 1.3, "expert": 1.6
        }
        
        difficulty = game_data.get("difficulty_level", "medium")
        base_xp = int(base_xp * difficulty_multipliers.get(difficulty, 1.0))
        
        if game_data.get("is_tournament_match", False):
            base_xp = int(base_xp * 1.5)
        
        return max(base_xp, 10)
    
    @staticmethod
    def update_user_level(user_id: int, xp_gained: int) -> dict:
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                param_style = "%s" if is_postgres else "?"
                
                cur.execute(f"SELECT * FROM user_levels WHERE user_id = {param_style}", (user_id,))
                level_data = cur.fetchone()
                
                if not level_data:
                    now = datetime.now(timezone.utc).isoformat()
                    initial_data = {
                        "user_id": user_id,
                        "level": 1,
                        "experience": xp_gained,
                        "next_level_xp": UserLevelManager.LEVEL_XP_REQUIREMENTS[2],
                        "total_xp": xp_gained,
                        "prestige": 0
                    }
                    
                    if is_postgres:
                        cur.execute("""
                            INSERT INTO user_levels (user_id, level, experience, next_level_xp, total_xp, prestige)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (user_id, 1, xp_gained, UserLevelManager.LEVEL_XP_REQUIREMENTS[2], xp_gained, 0))
                    else:
                        cur.execute("""
                            INSERT INTO user_levels (user_id, level, experience, next_level_xp, total_xp, prestige)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (user_id, 1, xp_gained, UserLevelManager.LEVEL_XP_REQUIREMENTS[2], xp_gained, 0))
                    
                    return {"level_up": False, "new_level": 1, "xp_gained": xp_gained}
                
                current_level = level_data["level"]
                current_xp = level_data["experience"]
                total_xp = level_data["total_xp"] + xp_gained
                new_xp = current_xp + xp_gained
                
                level_ups = []
                new_level = current_level
                
                while new_level < 100 and new_xp >= UserLevelManager.LEVEL_XP_REQUIREMENTS.get(new_level + 1, float('inf')):
                    new_level += 1
                    new_xp -= UserLevelManager.LEVEL_XP_REQUIREMENTS[new_level]
                    level_ups.append(new_level)
                
                next_level_xp = UserLevelManager.LEVEL_XP_REQUIREMENTS.get(new_level + 1, 0)
                
                if is_postgres:
                    cur.execute("""
                        UPDATE user_levels SET 
                            level = %s, experience = %s, next_level_xp = %s, total_xp = %s
                        WHERE user_id = %s
                    """, (new_level, new_xp, next_level_xp, total_xp, user_id))
                else:
                    cur.execute("""
                        UPDATE user_levels SET 
                            level = ?, experience = ?, next_level_xp = ?, total_xp = ?
                        WHERE user_id = ?
                    """, (new_level, new_xp, next_level_xp, total_xp, user_id))
                
                rewards = []
                for level in level_ups:
                    if level in UserLevelManager.LEVEL_REWARDS:
                        reward = UserLevelManager.LEVEL_REWARDS[level]
                        UserLevelManager._award_coins(user_id, reward["coins"])
                        rewards.append(reward)
                
                return {
                    "level_up": len(level_ups) > 0,
                    "new_level": new_level,
                    "old_level": current_level,
                    "xp_gained": xp_gained,
                    "total_xp": total_xp,
                    "rewards": rewards,
                    "levels_gained": level_ups
                }
                
        except Exception as e:
            logger.error(f"Error updating user level: {e}")
            return {"level_up": False, "xp_gained": xp_gained}
    
    @staticmethod
    def _award_coins(user_id: int, coins: int):
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                
                if is_postgres:
                    cur.execute("UPDATE users SET coins = coins + %s WHERE user_id = %s", (coins, user_id))
                else:
                    cur.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (coins, user_id))
        except Exception as e:
            logger.error(f"Error awarding coins: {e}")


class AchievementSystem:
    ACHIEVEMENTS = {
        'first_win': {
            'name': 'First Victory',
            'description': 'Win your first match',
            'reward_coins': 50,
            'reward_xp': 100,
            'icon': '🏆'
        },
        'century_club': {
            'name': 'Century Maker',
            'description': 'Score 100+ runs in a match',
            'reward_coins': 200,
            'reward_xp': 300,
            'icon': '💯'
        },
        'six_machine': {
            'name': 'Six Machine',
            'description': 'Hit 10 sixes in a match',
            'reward_coins': 150,
            'reward_xp': 250,
            'icon': '🚀'
        },
        'win_streak_5': {
            'name': 'Winning Streak',
            'description': 'Win 5 matches in a row',
            'reward_coins': 300,
            'reward_xp': 400,
            'icon': '🔥'
        },
        'tournament_champion': {
            'name': 'Tournament Champion',
            'description': 'Win a tournament',
            'reward_coins': 500,
            'reward_xp': 750,
            'icon': '👑'
        },
        'perfect_chase': {
            'name': 'Perfect Chase',
            'description': 'Chase a target without losing a wicket',
            'reward_coins': 250,
            'reward_xp': 350,
            'icon': '🎯'
        },
        'speed_demon': {
            'name': 'Speed Demon',
            'description': 'Score 50 runs in under 20 balls',
            'reward_coins': 175,
            'reward_xp': 275,
            'icon': '⚡'
        }
    }
    
    @staticmethod
    def check_achievements(user_id: int, match_data: dict) -> list:
        """Check if user unlocked any achievements"""
        unlocked = []
        
        # Check various achievements
        if match_data.get('result') == 'win' and match_data.get('is_first_win'):
            unlocked.append('first_win')
        
        if match_data.get('player_score', 0) >= 100:
            unlocked.append('century_club')
        
        if match_data.get('player_sixes', 0) >= 10:
            unlocked.append('six_machine')
        
        if match_data.get('winning_streak', 0) >= 5:
            unlocked.append('win_streak_5')
        
        if match_data.get('is_tournament_win'):
            unlocked.append('tournament_champion')
        
        if (match_data.get('result') == 'win' and 
            match_data.get('innings') == 2 and 
            match_data.get('player_wkts') == 0):
            unlocked.append('perfect_chase')
        
        player_balls = match_data.get('player_balls_faced', 0)
        player_score = match_data.get('player_score', 0)
        if player_score >= 50 and player_balls < 20:
            unlocked.append('speed_demon')
        
        # Save and reward
        newly_unlocked = []
        for achievement_id in unlocked:
            if AchievementSystem._award_achievement(user_id, achievement_id):
                newly_unlocked.append(achievement_id)
        
        return newly_unlocked
    
    @staticmethod
    def _award_achievement(user_id: int, achievement_id: str) -> bool:
        """Award achievement if not already unlocked"""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                param_style = "%s" if is_postgres else "?"
                
                # Check if already unlocked
                cur.execute(
                    f"SELECT achievement_id FROM user_achievements WHERE user_id = {param_style} AND achievement_id = {param_style}",
                    (user_id, achievement_id)
                )
                
                if cur.fetchone():
                    return False  # Already unlocked
                
                # Award achievement
                achievement = AchievementSystem.ACHIEVEMENTS[achievement_id]
                now = datetime.now(timezone.utc).isoformat()
                
                if is_postgres:
                    cur.execute("""
                        INSERT INTO user_achievements (user_id, achievement_id, unlocked_at)
                        VALUES (%s, %s, %s)
                    """, (user_id, achievement_id, now))
                else:
                    cur.execute("""
                        INSERT INTO user_achievements (user_id, achievement_id, unlocked_at)
                        VALUES (?, ?, ?)
                    """, (user_id, achievement_id, now))
                
                # Award rewards
                _award_coins(user_id, achievement['reward_coins'])
                _award_xp(user_id, achievement['reward_xp'])
                
                return True
        except Exception as e:
            logger.error(f"Error awarding achievement: {e}")
            return False
    
    @staticmethod
    def get_user_achievements(user_id: int) -> list:
        """Get all unlocked achievements for user"""
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                is_postgres = bool(os.getenv("DATABASE_URL"))
                param_style = "%s" if is_postgres else "?"
                
                cur.execute(
                    f"SELECT achievement_id, unlocked_at FROM user_achievements WHERE user_id = {param_style}",
                    (user_id,)
                )
                
                unlocked = []
                for row in cur.fetchall():
                    achievement_id = row['achievement_id']
                    if achievement_id in AchievementSystem.ACHIEVEMENTS:
                        achievement = AchievementSystem.ACHIEVEMENTS[achievement_id].copy()
                        achievement['unlocked_at'] = row['unlocked_at']
                        unlocked.append(achievement)
                
                return unlocked
        except Exception as e:
            logger.error(f"Error getting achievements: {e}")
            return []

    @staticmethod
    def generate_achievement_display(achievements: list) -> str:
        """Create achievement showcase"""
        if not achievements:
            return "🏆 No achievements unlocked yet!\nPlay matches to unlock achievements!"
        
        display = (
            f"{'🏆'*15}\n"
            f"  YOUR ACHIEVEMENTS\n"
            f"{'🏆'*15}\n\n"
        )
        
        for achievement in achievements:
            display += (
                f"{achievement.get('icon', '🎖️')} <b>{achievement.get('name')}</b>\n"
                f"   {achievement.get('description')}\n"
                f"   Reward: {achievement.get('reward_coins')} coins | {achievement.get('reward_xp')} XP\n"
                f"   Unlocked: {achievement.get('unlocked_at', 'Unknown')[:10]}\n\n"
            )
        
        total = len(achievements)
        available = len(AchievementSystem.ACHIEVEMENTS)
        display += f"Progress: {total}/{available} achievements unlocked"
        
        return display
    
def create_additional_tables():
    """Create tables for new features"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            if is_postgres:
                autoincrement = "SERIAL PRIMARY KEY"
                bigint = "BIGINT"
                bool_type = "BOOLEAN"
                bool_default = "FALSE"
                text_type = "TEXT"
                timestamp_type = "TIMESTAMPTZ"
            else:
                autoincrement = "INTEGER PRIMARY KEY AUTOINCREMENT"
                bigint = "INTEGER"
                bool_type = "INTEGER"
                bool_default = "0"
                text_type = "TEXT"
                timestamp_type = "TEXT"
            
            # User achievements table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS user_achievements (
                    id {autoincrement},
                    user_id {bigint} NOT NULL,
                    achievement_id {text_type} NOT NULL,
                    unlocked_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, achievement_id)
                )
            """)
            
            # Match analytics table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS match_analytics (
                    id {autoincrement},
                    match_id {text_type} NOT NULL,
                    user_id {bigint} NOT NULL,
                    total_balls INTEGER DEFAULT 0,
                    dot_balls INTEGER DEFAULT 0,
                    singles INTEGER DEFAULT 0,
                    boundaries INTEGER DEFAULT 0,
                    sixes INTEGER DEFAULT 0,
                    wickets INTEGER DEFAULT 0,
                    run_rate {text_type},
                    strike_rate {text_type},
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Leaderboards table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS leaderboards (
                    id {autoincrement},
                    category {text_type} NOT NULL,
                    user_id {bigint} NOT NULL,
                    value INTEGER DEFAULT 0,
                    updated_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(category, user_id)
                )
            """)
            
            # User inventory/items table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS user_inventory (
                    id {autoincrement},
                    user_id {bigint} NOT NULL,
                    item_type {text_type} NOT NULL,
                    item_id {text_type} NOT NULL,
                    quantity INTEGER DEFAULT 1,
                    acquired_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, item_type, item_id)
                )
            """)
            
            # Power-ups table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS powerups (
                    id {autoincrement},
                    name {text_type} NOT NULL,
                    description {text_type},
                    effect_type {text_type},
                    effect_value {text_type},
                    duration INTEGER,
                    cost INTEGER DEFAULT 0
                )
            """)
            
            # User friends/social table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS user_friends (
                    user_id {bigint} NOT NULL,
                    friend_id {bigint} NOT NULL,
                    status {text_type} DEFAULT 'pending',
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, friend_id)
                )
            """)    
            
            # Match replays table
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS match_replays (
                    id {autoincrement},
                    match_id {text_type} NOT NULL,
                    user_id {bigint} NOT NULL,
                    ball_by_ball {text_type},
                    highlights {text_type},
                    created_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            logger.info("Additional tables created successfully")
    except Exception as e:
        logger.error(f"Error creating additional tables: {e}")



def save_tournament_to_db(tournament: EliteTournament, chat_id: int = None):
    """Save tournament state to database"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            tournament_json = json.dumps(tournament.to_dict())
            now = datetime.now(timezone.utc).isoformat()
            
            if is_postgres:
                cur.execute("""
                    INSERT INTO tournaments (name, type, theme, status, format, entry_fee, prize_pool,
                                           max_players, created_by, created_at, brackets, metadata)
                    VALUES (%s, %s, %s, %s, %s, 0, 0, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                    brackets = EXCLUDED.brackets, metadata = EXCLUDED.metadata
                """, (tournament.tournament_id, tournament.name, tournament.type, tournament.theme, 
                      tournament.tournament_state, f"T{tournament.format_overs}",
                      len(tournament.participants), tournament.created_by, now,
                      json.dumps(tournament.bracket), tournament_json))
            else:
                cur.execute("""
                    INSERT OR REPLACE INTO tournaments 
                    (name, type, theme, status, format, entry_fee, prize_pool,
                     max_players, created_by, created_at, brackets, metadata)
                    VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?)
                """, (tournament.name, tournament.type, tournament.theme,
                      tournament.tournament_state, f"T{tournament.format_overs}",
                      len(tournament.participants), tournament.created_by, now,
                      json.dumps(tournament.bracket), tournament_json))
            
            logger.info(f"Tournament {tournament.tournament_id} saved to database")
            
    except Exception as e:
        logger.error(f"Error saving tournament: {e}")

def _get_tournament_participants(tournament_id: int) -> list:
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            if is_postgres:
                cur.execute("""
                    SELECT user_id FROM tournament_participants 
                    WHERE tournament_id = %s ORDER BY joined_at
                """, (tournament_id,))
            else:
                cur.execute("""
                    SELECT user_id FROM tournament_participants 
                    WHERE tournament_id = ? ORDER BY joined_at
                """, (tournament_id,))
            
            return [row["user_id"] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error getting tournament participants: {e}")
        return []

def load_tournament_from_db(tournament_id: str) -> Optional[EliteTournament]:
    """Load tournament from database"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            if is_postgres:
                cur.execute(f"SELECT metadata FROM tournaments WHERE id = {param_style}", (tournament_id,))
            else:
                cur.execute(f"SELECT metadata FROM tournaments WHERE id = {param_style}", (tournament_id,))
            
            row = cur.fetchone()
            if row and row.get('metadata'):
                data = json.loads(row['metadata'])
                return EliteTournament.from_dict(data)
            
            return None
            
    except Exception as e:
        logger.error(f"Error loading tournament: {e}")
        return None
    
def calculate_dynamic_difficulty(game: GameState, user_stats: dict) -> float:
    """Adjust bot difficulty based on player performance"""
    difficulty_map = {"easy": 0.4, "medium": 0.6, "hard": 0.8, "expert": 0.9}
    base_difficulty = difficulty_map.get(game.data.get('difficulty_level', 'medium'), 0.6)
    
    # Adjust based on player's recent performance
    win_rate = user_stats.get('wins', 0) / max(user_stats.get('games_played', 1), 1)
    avg_score = user_stats.get('avg_score', 0)
    
    if win_rate > 0.7:
        base_difficulty += 0.1
    elif win_rate < 0.3:
        base_difficulty -= 0.1
    
    if avg_score > 50:
        base_difficulty += 0.05
    
    return max(0.3, min(0.95, base_difficulty))


def apply_weather_pitch_effects(game: GameState, outcome: str, runs: int) -> int:
    """Modify runs based on weather and pitch conditions"""
    weather = game.data.get('weather_condition', 'clear')
    pitch = game.data.get('pitch_condition', 'normal')
    
    weather_multiplier = WEATHER_CONDITIONS.get(weather, {}).get('effect', 1.0)
    pitch_data = PITCH_CONDITIONS.get(pitch, {})
    
    # Apply weather effect
    if random.random() > weather_multiplier and runs > 0:
        runs = max(0, runs - 1)  # Weather reduces runs
    
    # Apply pitch effect based on shot type
    if outcome == 'boundary' and random.random() < pitch_data.get('batting_bonus', 1.0):
        if runs == 4 and random.random() > 0.7:
            runs = 6  # Flat pitch upgrades 4 to 6
    
    return runs


def generate_ball_commentary(outcome: str, runs: int, player_name: str, is_powerplay: bool) -> str:
    """Generate dynamic commentary for each ball"""
    commentaries = {
        'six': [
            f"💥 MASSIVE HIT by {player_name}! That's gone into the stands!",
            f"🚀 BOOM! {player_name} sends it sailing over the boundary!",
            f"⚡ INCREDIBLE SHOT! {player_name} launches it for a MAXIMUM!",
            f"🎯 What a hit! {player_name} connects perfectly - SIX RUNS!"
        ],
        'four': [
            f"⚡ BOUNDARY! {player_name} finds the gap beautifully!",
            f"🎯 Perfectly placed by {player_name} - FOUR RUNS!",
            f"💫 Exquisite timing from {player_name}! Races to the boundary!",
            f"🏏 Classic shot! {player_name} beats the fielders!"
        ],
        'wicket': [
            f"💥 OUT! {player_name} is dismissed!",
            f"🎯 BREAKTHROUGH! The bowler strikes!",
            f"💔 {player_name} has to go! What a delivery!",
            f"🔥 WICKET! The bowler gets the better of {player_name}!"
        ],
        'dot': [
            f"Solid defense by {player_name}",
            f"Good bowling - dot ball",
            f"{player_name} watchfully defends",
            f"No run - pressure building"
        ],
        'single': [
            f"Quick single taken by {player_name}",
            f"Good running - one run",
            f"{player_name} rotates the strike",
            f"Smart cricket - single"
        ]
    }
    
    if runs == 6:
        commentary = random.choice(commentaries['six'])
    elif runs == 4:
        commentary = random.choice(commentaries['four'])
    elif runs == 0 and outcome == 'wicket':
        commentary = random.choice(commentaries['wicket'])
    elif runs == 0:
        commentary = random.choice(commentaries['dot'])
    elif runs == 1:
        commentary = random.choice(commentaries['single'])
    else:
        commentary = f"{runs} runs scored by {player_name}"
    
    if is_powerplay:
        commentary += " [⚡ POWERPLAY]"
    
    return commentary


def handle_milestone_achievement(chat_id: int, user_id: int, milestone_type: str, value: int):
    """Celebrate player milestones"""
    milestone_messages = {
        'fifty': f"🎉 FIFTY UP! Brilliant knock! {value} runs!",
        'century': f"💯 CENTURY! What an innings! {value}* - LEGENDARY!",
        'duck': f"🦆 Out for a duck! Better luck next time!",
        'hat_trick': f"🎩 HAT-TRICK! Three wickets in three balls!",
        'perfect_over': f"🎯 PERFECT OVER! 6 dot balls - Magnificent bowling!"
    }
    
    message = milestone_messages.get(milestone_type, f"Achievement: {milestone_type}")
    
    AnimationManager.send_animation(chat_id, milestone_type, message)
    
    # Award bonus XP for milestones
    xp_rewards = {
        'fifty': 50,
        'century': 150,
        'hat_trick': 100,
        'perfect_over': 75
    }
    
    if milestone_type in xp_rewards:
        _award_xp(user_id, xp_rewards[milestone_type])


def generate_live_scorecard(game: GameState) -> str:
    """Create detailed live scorecard"""
    batting = game.data.get('batting')
    innings = game.data.get('innings', 1)
    
    player_score = game.data.get('player_score', 0)
    player_wickets = game.data.get('player_wkts', 0)
    player_balls = game.data.get('player_balls_faced', 0)
    player_fours = game.data.get('player_fours', 0)
    player_sixes = game.data.get('player_sixes', 0)
    
    bot_score = game.data.get('bot_score', 0)
    bot_wickets = game.data.get('bot_wkts', 0)
    bot_balls = game.data.get('bot_balls_faced', 0)
    bot_fours = game.data.get('bot_fours', 0)
    bot_sixes = game.data.get('bot_sixes', 0)
    
    overs = game.data.get('overs_bowled', 0)
    balls_in_over = game.data.get('balls_in_over', 0)
    overs_limit = game.data.get('overs_limit', DEFAULT_OVERS)
    target = game.data.get('target')
    
    # Calculate strike rates
    player_sr = (player_score / player_balls * 100) if player_balls > 0 else 0
    bot_sr = (bot_score / bot_balls * 100) if bot_balls > 0 else 0
    
    # Calculate run rate
    total_balls = overs * 6 + balls_in_over
    current_rr = (player_score / (total_balls / 6)) if batting == 'player' and total_balls > 0 else 0
    current_rr = (bot_score / (total_balls / 6)) if batting == 'bot' and total_balls > 0 else 0
    
    weather = game.data.get('weather_condition', 'clear')
    pitch = game.data.get('pitch_condition', 'normal')
    is_powerplay = game.data.get('is_powerplay', False)
    
    scorecard = (
        f"╔{'═'*48}╗\n"
        f"║  {game.data.get('match_format', 'T2')} CRICKET - INNINGS {innings}           ║\n"
        f"╚{'═'*48}╝\n\n"
    )
    
    if batting == 'player':
        scorecard += (
            f"🏏 YOU (Batting)\n"
            f"   Score: {player_score}/{player_wickets}\n"
            f"   Overs: {overs}.{balls_in_over}/{overs_limit}\n"
            f"   SR: {player_sr:.1f}% | RR: {current_rr:.2f}\n"
            f"   Boundaries: 4️⃣ {player_fours} | 6️⃣ {player_sixes}\n\n"
            f"🤖 BOT\n"
            f"   Score: {bot_score}/{bot_wickets}\n"
        )
        if target:
            need = target - player_score
            req_rr = (need / ((overs_limit - overs) + (6 - balls_in_over) / 6)) if overs < overs_limit else 0
            scorecard += f"\n🎯 Target: {target} | Need: {need} | Req RR: {req_rr:.2f}\n"
    else:
        scorecard += (
            f"🤖 BOT (Batting)\n"
            f"   Score: {bot_score}/{bot_wickets}\n"
            f"   Overs: {overs}.{balls_in_over}/{overs_limit}\n"
            f"   SR: {bot_sr:.1f}% | RR: {current_rr:.2f}\n"
            f"   Boundaries: 4️⃣ {bot_fours} | 6️⃣ {bot_sixes}\n\n"
            f"🏏 YOU\n"
            f"   Score: {player_score}/{player_wickets}\n"
        )
        if target:
            need = target - bot_score
            req_rr = (need / ((overs_limit - overs) + (6 - balls_in_over) / 6)) if overs < overs_limit else 0
            scorecard += f"\n🎯 Target: {target} | Need: {need} | Req RR: {req_rr:.2f}\n"
    
    scorecard += (
        f"\n{'─'*48}\n"
        f"🌤️ Weather: {weather.title()} | 🏟️ Pitch: {pitch.title()}\n"
    )
    
    if is_powerplay:
        powerplay_overs = game.data.get('powerplay_overs', 0)
        scorecard += f"⚡ POWERPLAY ({powerplay_overs} overs)\n"
    
    return scorecard


def generate_wagon_wheel(fours: int, sixes: int) -> str:
    """Create ASCII wagon wheel visualization"""
    wagon = [
        "         N",
        "        /|\\",
        "   NW  / | \\  NE",
        "      /  |  \\",
        " W ——————⚫——————— E",
        "      \\  |  /",
        "   SW  \\ | /  SE",
        "        \\|/",
        "         S"
    ]
    
    stats = (
        f"\n📊 Shot Distribution:\n"
        f"{'─'*30}\n"
        f"   Boundaries: {fours + sixes}\n"
        f"   4️⃣ Fours: {fours}\n"
        f"   6️⃣ Sixes: {sixes}\n"
    )
    
    return "\n".join(wagon) + "\n" + stats


def generate_manhattan_chart(scores_per_over: list) -> str:
    """Create ASCII Manhattan (runs per over) chart"""
    if not scores_per_over:
        return "No data available"
    
    max_score = max(scores_per_over) if scores_per_over else 1
    chart = "📈 Runs Per Over:\n" + "─" * 40 + "\n"
    
    for i, score in enumerate(scores_per_over, 1):
        bar_length = int((score / max_score) * 20) if max_score > 0 else 0
        bar = "█" * bar_length
        chart += f"Over {i:2d} | {bar} {score}\n"
    
    return chart


def handle_tournament_bracket_progression(tournament: EliteTournament, match: TournamentMatch):
    """Progress winners through tournament bracket"""
    if match.match_state != 'completed' or not match.winner:
        return
    
    winner_id = match.team1['id'] if match.winner == 'team1' else match.team2['id']
    winner_name = match.team1['name'] if match.winner == 'team1' else match.team2['name']
    
    # Update standings
    tournament.update_standings_after_match(match)
    
    # For knockout, advance winner to next round
    if tournament.type == 'knockout':
        current_round = tournament.current_round
        next_round = current_round + 1
        
        if next_round <= tournament.total_rounds:
            # Find the next match this winner should be in
            next_round_matches = tournament.bracket['rounds'].get(next_round, {}).get('matches', [])
            
            for next_match in next_round_matches:
                if next_match.team1['id'] is None:
                    next_match.team1 = {'id': winner_id, 'name': winner_name}
                    break
                elif next_match.team2['id'] is None:
                    next_match.team2 = {'id': winner_id, 'name': winner_name}
                    break
            
            # Check if round is complete
            current_round_matches = tournament.bracket['rounds'][current_round]['matches']
            if all(m.match_state == 'completed' for m in current_round_matches):
                tournament.current_round = next_round
                _announce_round_complete(tournament, current_round)
        else:
            # Tournament complete!
            tournament.tournament_state = 'completed'
            tournament.ended_at = datetime.now(timezone.utc).isoformat()
            _announce_tournament_winner(tournament, winner_id, winner_name)
    
    save_tournament_to_db(tournament)


def _announce_round_complete(tournament: EliteTournament, round_num: int):
    """Announce round completion and next matches"""
    stage_name = tournament.theme_data['stage_names'].get(round_num + 1, f"Round {round_num + 1}")
    
    announcement = (
        f"{'🎺'*5}\n"
        f" {stage_name.upper()} BEGINS!\n"
        f"{'🎺'*5}\n\n"
        f"Upcoming Matches:\n"
    )
    
    next_matches = tournament.bracket['rounds'][round_num + 1]['matches']
    for i, match in enumerate(next_matches, 1):
        announcement += f"{i}. {match.team1['emoji']} {match.team1['name']} vs {match.team2['emoji']} {match.team2['name']}\n"
    
    logger.info(announcement)


def _announce_tournament_winner(tournament: EliteTournament, winner_id: int, winner_name: str):
    """Celebrate tournament winner"""
    announcement = (
        f"{'🏆'*10}\n"
        f" {tournament.name.upper()}\n"
        f" CHAMPION: {winner_name}!\n"
        f"{'🏆'*10}\n\n"
        f"💰 Prize: {tournament.prize_pool} coins\n"
        f"🎖️ Glory: Eternal!\n"
    )
    
    # Award prizes
    _award_coins(winner_id, tournament.prize_pool)
    _award_xp(winner_id, 500)  # Bonus XP for tournament win
    
    logger.info(announcement)


def generate_tournament_stats(tournament: EliteTournament) -> str:
    """Generate comprehensive tournament statistics"""
    stats = (
        f"📊 {tournament.name} - STATISTICS\n"
        f"{'═'*50}\n\n"
        f"🎯 Format: {tournament.type.upper()} | {tournament.format_overs} Overs\n"
        f"👥 Participants: {len(tournament.participants)}\n"
        f"🏏 Matches Played: {sum(1 for m in tournament.matches if m.match_state == 'completed')}\n"
        f"🎯 Current Round: {tournament.current_round}/{tournament.total_rounds}\n\n"
        f"🏆 RECORDS:\n"
        f"   Highest Score: {tournament.records.get('highest_score', 0)}\n"
        f"   Most Sixes: {tournament.records.get('most_sixes', 0)}\n"
        f"   Most Boundaries: {tournament.records.get('most_boundaries', 0)}\n\n"
    )
    
    # Top performers
    stats += "🌟 TOP PERFORMERS:\n"
    sorted_participants = sorted(
        tournament.participants,
        key=lambda p: p.get('runs_scored', 0),
        reverse=True
    )[:3]
    
    for i, player in enumerate(sorted_participants, 1):
        medal = ['🥇', '🥈', '🥉'][i-1]
        stats += f"   {medal} {player['username']}: {player.get('runs_scored', 0)} runs\n"
    
    return stats


def kb_match_actions(game: GameState) -> types.InlineKeyboardMarkup:
    """Enhanced match action buttons"""
    kb = types.InlineKeyboardMarkup(row_width=3)
    
    batting = game.data.get('batting')
    
    if batting == 'player':
        # Batting options
        kb.add(
            types.InlineKeyboardButton("🛡️ Defend", callback_data="shot_defend"),
            types.InlineKeyboardButton("⚡ Attack", callback_data="shot_attack"),
            types.InlineKeyboardButton("🚀 Big Hit", callback_data="shot_big")
        )
        kb.add(
            types.InlineKeyboardButton("1️⃣ Single", callback_data="shot_single"),
            types.InlineKeyboardButton("2️⃣ Two", callback_data="shot_two"),
            types.InlineKeyboardButton("💪 Boundaries", callback_data="shot_boundary")
        )
    else:
        # Bowling options
        kb.add(
            types.InlineKeyboardButton("🎯 Yorker", callback_data="bowl_yorker"),
            types.InlineKeyboardButton("⚡ Bouncer", callback_data="bowl_bouncer"),
            types.InlineKeyboardButton("🌀 Spin", callback_data="bowl_spin")
        )
        kb.add(
            types.InlineKeyboardButton("🎪 Slower", callback_data="bowl_slower"),
            types.InlineKeyboardButton("💨 Fast", callback_data="bowl_fast"),
            types.InlineKeyboardButton("📐 Wide Yorker", callback_data="bowl_wide_yorker")
        )
    
    kb.add(
        types.InlineKeyboardButton("📊 Scorecard", callback_data="view_scorecard"),
        types.InlineKeyboardButton("📈 Stats", callback_data="view_match_stats")
    )
    kb.add(types.InlineKeyboardButton("❌ Forfeit", callback_data="forfeit_match"))
    
    return kb


def kb_post_match(match_result: dict) -> types.InlineKeyboardMarkup:
    """Post-match options"""
    kb = types.InlineKeyboardMarkup(row_width=2)
    
    kb.add(
        types.InlineKeyboardButton("🔄 Play Again", callback_data="quick_play"),
        types.InlineKeyboardButton("📊 Match Stats", callback_data="detailed_stats")
    )
    kb.add(
        types.InlineKeyboardButton("🏆 View Challenges", callback_data="challenges"),
        types.InlineKeyboardButton("🎯 Tournaments", callback_data="tournaments")
    )
    kb.add(
        types.InlineKeyboardButton("📈 My Progress", callback_data="my_profile"),
        types.InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")
    )
    
    return kb


def kb_format_selector() -> types.InlineKeyboardMarkup:
    """Enhanced format selection"""
    kb = types.InlineKeyboardMarkup(row_width=2)
    
    for format_key, format_data in TOURNAMENT_FORMATS.items():
        emoji = format_data['name'].split()[0]
        text = f"{emoji} {format_data['overs']}ov | {format_data['wickets']}wk"
        kb.add(types.InlineKeyboardButton(text, callback_data=f"format_{format_key}"))
    
    kb.add(
        types.InlineKeyboardButton("🎯 Custom", callback_data="format_custom"),
        types.InlineKeyboardButton("🔙 Back", callback_data="main_menu")
    )
    
    return kb


def kb_difficulty_selector() -> types.InlineKeyboardMarkup:
    """Difficulty selection with descriptions"""
    kb = types.InlineKeyboardMarkup(row_width=2)
    
    difficulties = [
        ("🟢 Easy", "diff_easy", "Relaxed gameplay"),
        ("🟡 Medium", "diff_medium", "Balanced challenge"),
        ("🔴 Hard", "diff_hard", "Tough opponent"),
        ("⚫ Expert", "diff_expert", "Maximum challenge")
    ]
    
    for name, callback, desc in difficulties:
        kb.add(types.InlineKeyboardButton(f"{name} - {desc}", callback_data=callback))
    
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
    
    return kb


def _save_tournament_participant(tournament_id: int, user_id: int, position: int):
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            now = datetime.now(timezone.utc).isoformat()
            
            if is_postgres:
                cur.execute("""
                    INSERT INTO tournament_participants (tournament_id, user_id, position, joined_at)
                    VALUES (%s, %s, %s, %s)
                """, (tournament_id, user_id, position, now))
            else:
                cur.execute("""
                    INSERT INTO tournament_participants (tournament_id, user_id, position, joined_at)
                    VALUES (?, ?, ?, ?)
                """, (tournament_id, user_id, position, now))
    except Exception as e:
        logger.error(f"Error saving tournament participant: {e}")

# Replace all database queries to use consistent parameter style
def _get_user_coins(user_id: int) -> int:
    """Get user's coin balance"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            cur.execute(f"SELECT coins FROM users WHERE user_id = {param_style}", (user_id,))
            row = cur.fetchone()
            return row["coins"] if row else 0
    except Exception as e:
        logger.error(f"Error getting user coins: {e}")
        return 0

def _deduct_user_coins(user_id: int, amount: int):
    """Deduct coins from user"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            cur.execute(f"UPDATE users SET coins = coins - {param_style} WHERE user_id = {param_style}", (amount, user_id))
    except Exception as e:
        logger.error(f"Error deducting coins: {e}")

def _award_coins(user_id: int, amount: int):
    """Award coins to user"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            cur.execute(f"UPDATE users SET coins = coins + {param_style} WHERE user_id = {param_style}", (amount, user_id))
    except Exception as e:
        logger.error(f"Error awarding coins: {e}")

def _award_xp(user_id: int, amount: int):
    """Award XP to user"""
    UserLevelManager.update_user_level(user_id, amount)

def create_daily_challenges():
    """Create daily challenges for all users"""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            if is_postgres:
                cur.execute("SELECT COUNT(*) as count FROM daily_challenges WHERE DATE(created_at) = %s", (today,))
            else:
                cur.execute("SELECT COUNT(*) as count FROM daily_challenges WHERE DATE(created_at) = ?", (today,))
            
            count = cur.fetchone()["count"]
            
            if count > 0:
                logger.info("Daily challenges already exist for today")
                return
            
            challenges = DailyChallenge.generate_daily_challenges(today)
            
            for challenge in challenges:
                _save_daily_challenge(challenge)
            
            logger.info(f"Created {len(challenges)} daily challenges for {today}")
    except Exception as e:
        logger.error(f"Error creating daily challenges: {e}")


def _save_daily_challenge(challenge: DailyChallenge):
    """Save a daily challenge to database"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            if is_postgres:
                cur.execute("""
                    INSERT INTO daily_challenges (
                        type, description, target, reward_coins, reward_xp, created_at, expires_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    challenge.type.value, challenge.description, challenge.target,
                    challenge.reward_coins, challenge.reward_xp, challenge.created_at.isoformat(),
                    challenge.expires_at.isoformat()
                ))
            else:
                cur.execute("""
                    INSERT INTO daily_challenges (
                        type, description, target, reward_coins, reward_xp, created_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    challenge.type.value, challenge.description, challenge.target,
                    challenge.reward_coins, challenge.reward_xp, challenge.created_at.isoformat(),
                    challenge.expires_at.isoformat()
                ))
    except Exception as e:
        logger.error(f"Error saving daily challenge: {e}")

def create_scheduled_tournament():
    try:
        now = datetime.now(timezone.utc)
        
        if now.hour == 12 and now.minute == 0:
            formats = ["T5", "T10"]
            selected_format = random.choice(formats)
            
            tournament_data = {
                "name": f"Daily {selected_format} Tournament",
                "type": "knockout",
                "theme": "world_cup",
                "format": selected_format,
                "entry_fee": 30,
                "max_players": 8
            }
            
            tournament_id = str(int(time.time() * 1000))
            tournament = EliteTournament(
                tournament_id,
                tournament_data.get("name", "Weekly Championship"),
                tournament_data.get("type", "knockout"),
                tournament_data.get("theme", "champions"),
                tournament_data.get("overs", 20),
                10,
                0
            )
            save_tournament_to_db(tournament)
            logger.info(f"Created weekly championship: {tournament.id}")
            announce_new_tournament(tournament.id)
        
        if now.weekday() == 6 and now.hour == 15 and now.minute == 0:
            tournament_data = {
                "name": "Weekly Championship T20",
                "type": "knockout", 
                "theme": "champions",
                "format": "T20",
                "entry_fee": 100,
                "max_players": 16
            }
            
            tournament_id = str(int(time.time() * 1000))
            tournament = EliteTournament(
                tournament_id,
                tournament_data.get("name", "Weekly Championship"),
                tournament_data.get("type", "knockout"),
                tournament_data.get("theme", "champions"),
                tournament_data.get("overs", 20),
                10,
                0
            )
            save_tournament_to_db(tournament)
            logger.info(f"Created weekly championship: {tournament.id}")
            announce_new_tournament(tournament.id)
            
    except Exception as e:
        logger.error(f"Error creating scheduled tournament: {e}")

def announce_new_tournament(tournament_id: int):
    try:
        tournament = load_tournament_from_db(tournament_id)
        if not tournament:
            return
        
        announcement = (
            f"🎺 <b>NEW TOURNAMENT ALERT!</b> 🎺\n\n"
            f"🏆 <b>{tournament.name}</b>\n"
            f"📋 Format: {tournament.format}\n"
            f"💰 Entry: {tournament.entry_fee} coins\n"
            f"👥 Max Players: {tournament.max_players}\n"
            f"🏆 Prize Pool: {tournament.prize_pool} coins\n\n"
            f"Join now with /tournaments"
        )
        
        logger.info(f"Tournament announcement: {announcement}")
        
    except Exception as e:
        logger.error(f"Error announcing tournament: {e}")

# ADD THESE NEW KEYBOARD FUNCTIONS:
def kb_tournament_menu() -> types.InlineKeyboardMarkup:
    """Tournament menu keyboard"""
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🏆 Join Tournament", callback_data="tournament_list"),
        types.InlineKeyboardButton("➕ Create Tournament", callback_data="tournament_create")
    )
    kb.add(
        types.InlineKeyboardButton("📊 My Tournaments", callback_data="tournament_history"),
        types.InlineKeyboardButton("🥇 Rankings", callback_data="tournament_rankings")
    )
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
    return kb

def kb_tournament_formats() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    for format_key, format_data in TOURNAMENT_FORMATS.items():
        kb.add(types.InlineKeyboardButton(
            f"{format_data['name']} - {format_data['entry_fee']} coins",
            callback_data=f"tformat_{format_key}"
        ))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="tournaments"))
    return kb

def kb_tournament_themes() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    themes = [
        ("🌍 World Cup", "theme_world_cup"),
        ("🇮🇳 IPL Style", "theme_ipl"),
        ("🏏 The Ashes", "theme_ashes"),
        ("⚡ Champions", "theme_champions"),
        ("🎯 Custom", "theme_custom")
    ]
    for text, callback in themes:
        kb.add(types.InlineKeyboardButton(text, callback_data=callback))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="tournament_create"))
    return kb

def kb_challenges() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("📋 View Challenges", callback_data="challenges_view"),
        types.InlineKeyboardButton("🎁 Claim Rewards", callback_data="challenges_claim"),
        types.InlineKeyboardButton("📊 Challenge History", callback_data="challenges_history")
    )
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
    return kb

def kb_challenge_claim(challenges: list) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    for challenge in challenges:
        if challenge.get("completed") and not challenge.get("claimed"):
            kb.add(types.InlineKeyboardButton(
                f"🎁 {challenge['description']} - {challenge['reward_coins']} coins",
                callback_data=f"claim_{challenge['id']}"
            ))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="challenges"))
    return kb

def kb_level_up() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🎮 Play Again", callback_data="quick_play"),
        types.InlineKeyboardButton("📊 View Stats", callback_data="my_stats")
    )
    kb.add(types.InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu"))
    return kb

def generate_live_match_display(match: TournamentMatch) -> str:
    """Create engaging live match display"""
    current_innings = match.innings_1 if match.current_innings == 1 else match.innings_2
    
    momentum_bar = '█' * (current_innings.momentum // 5) + '░' * ((100 - current_innings.momentum) // 5)
    
    display = (
        f"╔{'═'*48}╗\n"
        f"║  {match.tournament_stage:<44}  ║\n"
        f"║  Match: {match.match_id:<38}  ║\n"
        f"╚{'═'*48}╝\n\n"
        f"{match.team1['emoji']} {match.team1['name']}\n"
        f"   Runs: {match.innings_1.runs:3d} | Wickets: {match.innings_1.wickets}\n"
        f"   Overs: {match.innings_1.overs_completed}.{match.innings_1.balls_in_over}/{match.format_overs}\n"
        f"   Boundaries: 4️⃣ {match.innings_1.fours} | 6️⃣ {match.innings_1.sixes}\n"
        f"   Strike Rate: {match.innings_1.get_strike_rate():.1f}%\n\n"
        f"{match.team2['emoji']} {match.team2['name']}\n"
    )
    
    if match.current_innings == 2 and match.innings_2:
        target = match.innings_1.runs + 1
        need = max(0, target - match.innings_2.runs)
        
        display += (
            f"   Runs: {match.innings_2.runs:3d} | Wickets: {match.innings_2.wickets}\n"
            f"   Overs: {match.innings_2.overs_completed}.{match.innings_2.balls_in_over}/{match.format_overs}\n"
            f"   Boundaries: 4️⃣ {match.innings_2.fours} | 6️⃣ {match.innings_2.sixes}\n"
            f"   Strike Rate: {match.innings_2.get_strike_rate():.1f}%\n\n"
            f"   🎯 Target: {target} | Need: {need}\n"
        )
    
    display += (
        f"\n⚡ MOMENTUM: [{momentum_bar}] {current_innings.momentum}%\n"
        f"🌤️ Weather: {match.weather.title()} | 🏟️ Pitch: {match.pitch.title()}\n"
        f"⚡ Powerplay: {'✅ ACTIVE' if current_innings.is_powerplay else '❌ Ended'}\n"
    )
    
    return display


def generate_match_summary(match: TournamentMatch) -> str:
    """Create post-match summary"""
    summary = (
        f"╔{'═'*48}╗\n"
        f"║  MATCH SUMMARY - {match.match_id:<31}  ║\n"
        f"╚{'═'*48}╝\n\n"
        f"{match.team1['emoji']} {match.team1['name']}\n"
        f"   {match.innings_1.runs}/{match.innings_1.wickets} "
        f"({match.innings_1.overs_completed}.{match.innings_1.balls_in_over} overs)\n"
        f"   4️⃣: {match.innings_1.fours} | 6️⃣: {match.innings_1.sixes}\n"
        f"   SR: {match.innings_1.get_strike_rate():.1f}%\n\n"
        f"{match.team2['emoji']} {match.team2['name']}\n"
        f"   {match.innings_2.runs}/{match.innings_2.wickets} "
        f"({match.innings_2.overs_completed}.{match.innings_2.balls_in_over} overs)\n"
        f"   4️⃣: {match.innings_2.fours} | 6️⃣: {match.innings_2.sixes}\n"
        f"   SR: {match.innings_2.get_strike_rate():.1f}%\n\n"
    )
    
    if match.winner == 'team1':
        summary += f"🏆 {match.team1['name']} wins by {match.margin} {match.margin_type}!\n"
    elif match.winner == 'team2':
        summary += f"🏆 {match.team2['name']} wins by {match.margin} {match.margin_type}!\n"
    else:
        summary += "🤝 Match Tied!\n"
    
    if match.key_moments:
        summary += "\n📊 Key Moments:\n"
        for moment in match.key_moments[:5]:
            if moment['type'] == 'boundary_4':
                summary += f"   ⚡ Ball {moment['ball']}: FOUR!\n"
            elif moment['type'] == 'boundary_6':
                summary += f"   💣 Ball {moment['ball']}: SIX!\n"
            elif moment['type'] == 'wicket':
                summary += f"   💥 Ball {moment['ball']}: WICKET!\n"
    
    return summary


def generate_tournament_bracket(tournament: EliteTournament) -> str:
    """Display tournament bracket"""
    bracket_text = (
        f"{tournament.theme_data['emoji']} {tournament.name.upper()}\n"
        f"Type: {tournament.type.upper()} | Status: {tournament.tournament_state.upper()}\n"
        f"{'='*50}\n\n"
    )
    
    if tournament.type == 'knockout' and tournament.bracket:
        for round_num in sorted(tournament.bracket['rounds'].keys()):
            round_data = tournament.bracket['rounds'][round_num]
            bracket_text += f"🎯 {round_data['stage']}\n"
            
            for match in round_data['matches']:
                if match.match_state == 'completed':
                    status = '✅' if match.winner else '⚽'
                    winner = match.team1['name'] if match.winner == 'team1' else match.team2['name']
                    bracket_text += f"   {status} {match.team1['name']} vs {match.team2['name']}\n"
                    bracket_text += f"      Winner: {winner}\n"
                else:
                    bracket_text += f"   ⏳ {match.team1['name']} vs {match.team2['name']}\n"
            
            bracket_text += "\n"
    else:
        bracket_text += f"Participants: {len(tournament.participants)}\n"
        bracket_text += f"Matches: {len(tournament.matches)}\n"
        bracket_text += f"Current Round: {tournament.current_round}\n"
    
    return bracket_text

# ADD THESE DISPLAY FUNCTIONS:
def handle_tournament_menu(chat_id: int, user_id: int):
    """Show tournament main menu - FIXED"""
    try:
        menu_text = (
            "🏆 TOURNAMENT HUB 🏆\n\n"
            "Choose an option:\n"
        )
        
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("🎯 Join", callback_data="tournament_join"),
            types.InlineKeyboardButton("➕ Create", callback_data="tournament_create")
        )
        kb.add(
            types.InlineKeyboardButton("📊 View", callback_data="tournament_list"),
            types.InlineKeyboardButton("🥇 Rankings", callback_data="tournament_rankings")
        )
        kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
        
        bot.send_message(chat_id, menu_text, reply_markup=kb)
        
    except Exception as e:
        logger.error(f"Error showing tournament menu: {e}")
        bot.send_message(chat_id, "Error loading tournament menu")


def show_all_tournaments(chat_id: int):
    """List all active tournaments"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            if is_postgres:
                cur.execute("SELECT id, name, type, status FROM tournaments WHERE status IN ('registration', 'live') LIMIT 10")
            else:
                cur.execute("SELECT id, name, type, status FROM tournaments WHERE status IN ('registration', 'live') LIMIT 10")
            
            tournaments = cur.fetchall()
        
        if not tournaments:
            bot.send_message(chat_id, "No tournaments available right now.")
            return
        
        text = "📊 AVAILABLE TOURNAMENTS\n\n"
        kb = types.InlineKeyboardMarkup(row_width=1)
        
        for tournament in tournaments:
            text += f"🏆 {tournament['name']} ({tournament['type'].upper()})\n"
            kb.add(types.InlineKeyboardButton(
                f"View {tournament['name']}", 
                callback_data=f"tourn_view_{tournament['id']}"
            ))
        
        kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="tournament_menu"))
        bot.send_message(chat_id, text, reply_markup=kb)
        
    except Exception as e:
        logger.error(f"Error showing tournaments: {e}")
        bot.send_message(chat_id, "Error loading tournaments.")


def show_tournament_participants(chat_id: int, tournament_id: str):
    """Display tournament participants"""
    try:
        tournament = load_tournament_from_db(tournament_id)
        if not tournament:
            bot.send_message(chat_id, "❌ Tournament not found.")
            return
        
        text = (
            f"👥 PARTICIPANTS - {tournament.name}\n\n"
            f"Total: {len(tournament.participants)}/16\n\n"
        )
        
        for idx, participant in enumerate(tournament.participants, 1):
            text += f"{idx}. {participant['avatar']} {participant['username']}\n"
        
        kb = types.InlineKeyboardMarkup(row_width=1)
        kb.add(
            types.InlineKeyboardButton("🎯 Join Tournament", callback_data=f"join_tourn_{tournament_id}"),
            types.InlineKeyboardButton("🔙 Back", callback_data="tournament_menu")
        )
        
        bot.send_message(chat_id, text, reply_markup=kb)
        
    except Exception as e:
        logger.error(f"Error showing participants: {e}")
        bot.send_message(chat_id, "❌ Error loading tournament.")


def start_tournament_match(chat_id: int, user_id: int, tournament_id: str, match_id: str):
    """Start a specific tournament match"""
    try:
        tournament = load_tournament_from_db(tournament_id)
        if not tournament:
            bot.send_message(chat_id, "❌ Tournament not found.")
            return
        
        match = next((m for m in tournament.matches if m.match_id == match_id), None)
        if not match:
            bot.send_message(chat_id, "❌ Match not found.")
            return
        
        # Store match context in session
        set_user_session_data(user_id, "current_tournament", tournament_id)
        set_user_session_data(user_id, "current_match", match_id)
        
        # Start toss
        toss_text = (
            f"🏆 {match.tournament_stage}\n\n"
            f"{match.team1['emoji']} {match.team1['name']} vs {match.team2['emoji']} {match.team2['name']}\n\n"
            f"🪙 TOSS TIME!\n\n"
            f"Predict the toss:"
        )
        
        kb = types.InlineKeyboardMarkup()
        kb.add(
            types.InlineKeyboardButton("🪙 Heads", callback_data=f"tourn_toss_heads_{tournament_id}_{match_id}"),
            types.InlineKeyboardButton("🪙 Tails", callback_data=f"tourn_toss_tails_{tournament_id}_{match_id}")
        )
        
        bot.send_message(chat_id, toss_text, reply_markup=kb)
        
    except Exception as e:
        logger.error(f"Error starting tournament match: {e}")
        bot.send_message(chat_id, "❌ Error starting match.")



def show_challenges_menu(chat_id: int, user_id: int):
    try:
        level_info = _get_user_level_info(user_id)
        
        menu_text = (
            f"🎯 <b>Daily Challenges</b>\n\n"
            f"👤 Level: {level_info['level']} "
            f"({level_info['experience']}/{level_info['next_level_xp']} XP)\n\n"
            f"Complete daily challenges to earn coins and XP!\n"
            f"New challenges reset every day at midnight UTC.\n\n"
            f"What would you like to do?"
        )
        
        bot.send_message(chat_id, menu_text, reply_markup=kb_challenges())
        
    except Exception as e:
        logger.error(f"Error showing challenges menu: {e}")
        bot.send_message(chat_id, "❌ Error loading challenges menu.")

def show_daily_challenges(chat_id: int, user_id: int):
    try:
        tracker = ChallengeTracker(user_id)
        
        if not tracker.active_challenges:
            bot.send_message(
                chat_id,
                "🎯 No active challenges found.\n\nNew challenges will be available soon!",
                reply_markup=kb_challenges()
            )
            return
        
        challenges_text = "🎯 <b>Today's Challenges</b>\n\n"
        
        for challenge in tracker.active_challenges:
            challenge_id = challenge["id"]
            progress = tracker.progress.get(challenge_id, 0)
            target = challenge["target"]
            completed = challenge.get("completed", False)
            
            if completed:
                status = "✅ COMPLETED"
                progress_bar = "████████████"
            else:
                status = f"{progress}/{target}"
                progress_pct = min(progress / target, 1.0)
                filled_blocks = int(progress_pct * 12)
                progress_bar = "█" * filled_blocks + "░" * (12 - filled_blocks)
            
            diff_emoji = {"easy": "🟢", "medium": "🟡", "hard": "🔴"}.get(challenge.get("difficulty", "medium"), "🟡")
            
            challenges_text += (
                f"{challenge.get('icon', '🎯')} <b>{challenge['description']}</b>\n"
                f"   {diff_emoji} Difficulty • "
                f"💰 {challenge['reward_coins']} coins • "
                f"⭐ {challenge['reward_xp']} XP\n"
                f"   Progress: {status}\n"
                f"   [{progress_bar}]\n\n"
            )
        
        bot.send_message(chat_id, challenges_text, reply_markup=kb_challenges())
        
    except Exception as e:
        logger.error(f"Error showing daily challenges: {e}")
        bot.send_message(chat_id, "❌ Error loading challenges.")

def claim_challenge_rewards(chat_id: int, user_id: int):
    try:
        tracker = ChallengeTracker(user_id)
        
        claimable = [c for c in tracker.active_challenges 
                    if c.get("completed") and not c.get("claimed")]
        
        if not claimable:
            bot.send_message(
                chat_id,
                "🎁 No rewards to claim right now.\n\nComplete challenges to earn rewards!",
                reply_markup=kb_challenges()
            )
            return
        
        rewards_text = "🎁 <b>Claimable Rewards</b>\n\n"
        total_coins = sum(c["reward_coins"] for c in claimable)
        total_xp = sum(c["reward_xp"] for c in claimable)
        
        for challenge in claimable:
            rewards_text += (
                f"✅ {challenge['description']}\n"
                f"   💰 {challenge['reward_coins']} coins + "
                f"⭐ {challenge['reward_xp']} XP\n\n"
            )
        
        rewards_text += (
            f"<b>Total Rewards:</b>\n"
            f"💰 {total_coins} coins\n"
            f"⭐ {total_xp} XP"
        )
        
        bot.send_message(chat_id, rewards_text, reply_markup=kb_challenge_claim(claimable))
        
    except Exception as e:
        logger.error(f"Error showing claimable rewards: {e}")
        bot.send_message(chat_id, "❌ Error loading rewards.")

def _get_user_level_info(user_id: int) -> dict:
    """Fixed version with consistent parameters"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            cur.execute(f"SELECT * FROM user_levels WHERE user_id = {param_style}", (user_id,))
            
            level_data = cur.fetchone()
            
            if level_data:
                return {
                    "level": level_data["level"],
                    "experience": level_data["experience"],
                    "next_level_xp": level_data["next_level_xp"],
                    "total_xp": level_data["total_xp"],
                    "prestige": level_data.get("prestige", 0)
                }
            else:
                return {"level": 1, "experience": 0, "next_level_xp": 100, "total_xp": 0, "prestige": 0}
    except Exception as e:
        logger.error(f"Error getting user level info: {e}")
        return {"level": 1, "experience": 0, "next_level_xp": 100, "total_xp": 0, "prestige": 0}

def initialize_daily_systems():
    try:
        create_daily_challenges()
        create_scheduled_tournament()
        logger.info("Daily systems initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing daily systems: {e}")

def schedule_daily_tasks():
    schedule.every().day.at("00:01").do(create_daily_challenges)
    schedule.every().day.at("12:00").do(create_scheduled_tournament)
    schedule.every().sunday.at("15:00").do(create_scheduled_tournament)
    
    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("Daily task scheduler started")

# ADD THIS INITIALIZATION FUNCTION - CALL THIS IN YOUR MAIN SECTION:
def start_background_systems():
    try:
        logger.info("Background systems initialized")
    except Exception as e:
        logger.error(f"Error starting background systems: {e}")
# Game Logic Functions
def safe_load_game(chat_id: int) -> Optional[Dict[str, Any]]:
    try:
        game_state = GameState(chat_id)
        return game_state.data
    except Exception as e:
        logger.error(f"Failed to load game: {e}")
        # Return a default game state instead of None
        return default_game(chat_id)

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
        g['chat_id'] = chat_id
        safe_save_game(chat_id, g)
        game_state = GameState(chat_id)
        game_state.data = g
        game_state.save()
        
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

def check_innings_end(g: Dict[str, Any]) -> dict:
    """Check if innings should end - FIXED VERSION"""
    current_batting = g["batting"]
    
    # Check wickets first
    if current_batting == "player":
        if g["player_wkts"] >= g["wickets_limit"]:
            return {'innings_end': True, 'reason': 'all_out'}
    else:
        if g["bot_wkts"] >= g["wickets_limit"]:
            return {'innings_end': True, 'reason': 'all_out'}
    
    # Check overs - FIXED: Must complete full over
    if g["overs_bowled"] >= g["overs_limit"]:
        return {'innings_end': True, 'reason': 'overs_complete'}
    
    # Check target in second innings
    if g["innings"] == 2 and g.get("target"):
        current_score = g["player_score"] if current_batting == "player" else g["bot_score"]
        if current_score >= g["target"]:
            return {'innings_end': True, 'reason': 'target_achieved'}
    
    return {'innings_end': False, 'reason': None}

def enhanced_process_ball_v2(chat_id: int, user_value: int, user_id: int):
    """Enhanced version with tournament and challenge integration - FIXED"""
    if not (1 <= user_value <= 6):
        return "Please send a number between 1 and 6"
    
    try:
        log_event(chat_id, "ball_input", f"user={user_value} from={user_id}")
        
        game_state = GameState(chat_id)
        logger.info(f"Game state loaded for chat {chat_id}: state={game_state.data.get('state', 'unknown')}")
        
        if game_state.data['state'] != 'play':
            logger.warning(f"Game state is '{game_state.data['state']}' instead of 'play' for chat {chat_id}")
            return "No active match found. Use /play to start a new match."
        
        bot_value = calculate_bot_move(game_state.data, user_value)
        logger.debug(f"User: {user_value}, Bot: {bot_value}")
        
        is_wicket = (user_value == bot_value)
        runs_scored = 0
        current_batting = game_state.data['batting']
        
        if current_batting == "player":
            game_state.update(player_balls_faced=game_state.data['player_balls_faced'] + 1)
            
            if is_wicket:
                game_state.update(player_wkts=game_state.data['player_wkts'] + 1)
            else:
                runs_scored = user_value
                new_score = game_state.data['player_score'] + runs_scored
                updates = {'player_score': new_score}
                
                if runs_scored == 4:
                    updates['player_fours'] = game_state.data['player_fours'] + 1
                    AnimationManager.send_animation(chat_id, "four", "⚡ BOUNDARY!")
                elif runs_scored == 6:
                    updates['player_sixes'] = game_state.data['player_sixes'] + 1
                    AnimationManager.send_animation(chat_id, "six", "🚀 MAXIMUM!")
                
                game_state.update(**updates)
        else:
            game_state.update(bot_balls_faced=game_state.data['bot_balls_faced'] + 1)
            
            if is_wicket:
                game_state.update(bot_wkts=game_state.data['bot_wkts'] + 1)
                AnimationManager.send_animation(chat_id, "wicket", "💥 WICKET!")
            else:
                runs_scored = bot_value
                new_score = game_state.data['bot_score'] + runs_scored
                updates = {'bot_score': new_score}
                
                if runs_scored == 4:
                    updates['bot_fours'] = game_state.data['bot_fours'] + 1
                elif runs_scored == 6:
                    updates['bot_sixes'] = game_state.data['bot_sixes'] + 1
                
                game_state.update(**updates)
        
        commentary = get_commentary(game_state.data, user_value, bot_value, runs_scored, is_wicket)
        
        # Check for century milestone
        if current_batting == "player" and game_state.data['player_score'] >= 100:
            previous_score = game_state.data['player_score'] - runs_scored
            if previous_score < 100:
                AnimationManager.send_animation(chat_id, "century", "💯 CENTURY!")
        
        # CRITICAL FIX: Update balls_in_over AFTER processing the ball
        game_state.data['balls_in_over'] += 1
        
        over_completed = False
        powerplay_ended = False
        
        # Check for over completion
        if game_state.data['balls_in_over'] >= 6:
            game_state.data['balls_in_over'] = 0
            game_state.data['overs_bowled'] += 1
            over_completed = True
        
        # Check powerplay status
        if game_state.data.get('is_powerplay') and game_state.data['overs_bowled'] >= game_state.data.get('powerplay_overs', 0):
            game_state.data['is_powerplay'] = False
            powerplay_ended = True
        
        if not game_state.save():
            logger.error("Failed to save game state")
            return "Error saving game state. Please try again."
        
        # Update challenges after each ball
        tracker = ChallengeTracker(user_id)
        
        if current_batting == "player":
            tracker.update_progress(ChallengeType.SCORE, game_state.data['player_score'], game_state.data)
            tracker.update_progress(ChallengeType.SIXES, game_state.data['player_sixes'])
            tracker.update_progress(ChallengeType.BOUNDARIES, 
                                  game_state.data['player_fours'] + game_state.data['player_sixes'])
        
        # CRITICAL FIX: Check innings end PROPERLY
        innings_check = check_innings_end(game_state.data)
        
        if innings_check['innings_end']:
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
        complete_match_enhanced(game_state.chat_id, game_state.data, user_id)
        game_state.delete()
        return result


def handle_level_up_notification(chat_id: int, level_data: dict):
    """Handle level up notification with rewards"""
    if not level_data.get("level_up"):
        return
    
    try:
        new_level = level_data["new_level"]
        xp_gained = level_data["xp_gained"]
        rewards = level_data.get("rewards", [])
        
        level_up_text = (
            f"🎊 <b>LEVEL UP!</b> 🎊\n\n"
            f"🆙 Level: {level_data['old_level']} → {new_level}\n"
            f"⭐ XP Gained: +{xp_gained}\n"
            f"💫 Total XP: {level_data['total_xp']}\n\n"
        )
        
        if rewards:
            level_up_text += "🎁 <b>Level Rewards:</b>\n"
            for reward in rewards:
                level_up_text += f"💰 +{reward['coins']} coins\n"
                level_up_text += f"🏆 Title: {reward['title']}\n"
        
        level_up_text += "\nKeep playing to unlock more rewards!"
        
        # Send with special animation
        AnimationManager.send_animation(chat_id, "level_up", level_up_text)
        bot.send_message(chat_id, level_up_text, reply_markup=kb_level_up())
        
    except Exception as e:
        logger.error(f"Error handling level up notification: {e}")

def notify_challenge_completion(chat_id: int, user_id: int, completed_challenges: list):
    """Notify user of completed challenges"""
    if not completed_challenges:
        return
    
    try:
        notification_text = "🎯 <b>Challenge Completed!</b>\n\n"
        
        for challenge in completed_challenges:
            notification_text += (
                f"✅ {challenge['description']}\n"
                f"🎁 Reward: {challenge['reward_coins']} coins + {challenge['reward_xp']} XP\n\n"
            )
        
        notification_text += "Visit the challenges menu to claim your rewards!"
        
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("🎁 Claim Rewards", callback_data="challenges_claim"))
        
        bot.send_message(chat_id, notification_text, reply_markup=kb)
        
    except Exception as e:
        logger.error(f"Error notifying challenge completion: {e}")


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

def complete_match_enhanced(chat_id: int, g: Dict[str, Any], user_id: int):
    """Enhanced match completion with proper error handling"""
    try:
        player_score, bot_score = g["player_score"], g["bot_score"]
        
        # Determine result
        if player_score > bot_score:
            result = "win"
            margin = player_score - bot_score
            margin_text = f"by {margin} runs"
            result_emoji = "🏆"
            result_text = "YOU WIN!"
        elif bot_score > player_score:
            wickets_left = g["wickets_limit"] - g["bot_wkts"]
            result = "loss"
            margin_text = f"by {wickets_left} wickets" if wickets_left > 0 else "on last ball"
            result_emoji = "😔"
            result_text = "BOT WINS!"
        else:
            result = "tie"
            margin_text = "Match Tied!"
            result_emoji = "🤝"
            result_text = "IT'S A TIE!"
        
        # Update stats and get rewards
        update_result = update_user_stats_v2(user_id, g, result)
        
        match_summary = generate_match_summary(g, result, margin_text)
        
        final_message = (
            f"🏁 <b>MATCH OVER</b>\n\n"
            f"{result_emoji} <b>{result_text}</b>\n"
            f"Margin: <b>{margin_text}</b>\n\n"
            f"{match_summary}\n\n"
            f"Well played! 🏏"
        )
        
        # Send match result
        bot.send_message(chat_id, final_message, reply_markup=kb_post_match())
        
        # Handle level up notification
        if update_result.get("level_data", {}).get("level_up"):
            handle_level_up_notification(chat_id, update_result["level_data"])
        
        # Handle challenge completion notification
        completed_challenges = update_result.get("completed_challenges", [])
        if completed_challenges:
            notify_challenge_completion(chat_id, user_id, completed_challenges)
        
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
    """Enhanced version with XP and challenge updates - REPLACE EXISTING"""
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
            
            # Calculate and award XP
            xp_gained = UserLevelManager.calculate_match_xp(g, result)
            level_data = UserLevelManager.update_user_level(user_id, xp_gained)
            
            # Update challenges
            tracker = ChallengeTracker(user_id)
            
            if result == "win":
                tracker.update_progress(ChallengeType.WINS, 1)
                # Update streak
                current_streak = get_user_session_data(user_id, "current_streak", 0) + 1
                set_user_session_data(user_id, "current_streak", current_streak)
                tracker.update_progress(ChallengeType.STREAK, current_streak)
            else:
                # Reset streak on loss
                set_user_session_data(user_id, "current_streak", 0)
                tracker.update_progress(ChallengeType.STREAK, 0)
            
            # Update score-based challenges with final score
            tracker.update_progress(ChallengeType.SCORE, g["player_score"], g)
            tracker.update_progress(ChallengeType.SIXES, g["player_sixes"])
            tracker.update_progress(ChallengeType.BOUNDARIES, g["player_fours"] + g["player_sixes"])
            
            # Check for completed challenges
            completed = tracker.check_completion()
            
            return {
                "level_data": level_data,
                "completed_challenges": completed,
                "xp_gained": xp_gained
            }
            
    except Exception as e:
        logger.error(f"Error updating user stats: {e}")
        return {"level_data": {"level_up": False}, "completed_challenges": [], "xp_gained": 0}


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
        types.InlineKeyboardButton("🏆 Tournaments", callback_data="tournament_menu"),
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

def _update_tournament_in_db(tournament):
    """Save tournament updates to database"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            now = datetime.now(timezone.utc).isoformat()
            
            tournament_json = json.dumps(tournament.to_dict())
            
            if is_postgres:
                cur.execute("""
                    UPDATE tournaments SET 
                        name = %s, status = %s, current_round = %s, 
                        brackets = %s, metadata = %s, updated_at = %s
                    WHERE id = %s
                """, (
                    tournament.name, tournament.tournament_state, tournament.current_round,
                    json.dumps(tournament.bracket), tournament_json, now, tournament.tournament_id
                ))
            else:
                cur.execute("""
                    UPDATE tournaments SET 
                        name = ?, status = ?, current_round = ?, 
                        brackets = ?, metadata = ?, updated_at = ?
                    WHERE id = ?
                """, (
                    tournament.name, tournament.tournament_state, tournament.current_round,
                    json.dumps(tournament.bracket), tournament_json, now, tournament.tournament_id
                ))
                
    except Exception as e:
        logger.error(f"Error updating tournament: {e}")

def _send_registration_confirmation(chat_id: int, user_id: int, tournament):
    """Send registration confirmation message"""
    try:
        confirmation_text = (
            f"✅ <b>Registration Confirmed!</b>\n\n"
            f"🏆 Tournament: {tournament.name}\n"
            f"🎮 You're player #{len(tournament.participants)}\n"
            f"👥 Total players: {len(tournament.participants)}/{tournament.max_players}\n\n"
            f"Good luck! 🍀"
        )
        bot.send_message(chat_id, confirmation_text)
        
    except Exception as e:
        logger.error(f"Error sending registration confirmation: {e}")

def _start_tournament(tournament_id: int):
    """Start tournament when full"""
    try:
        tournament = load_tournament_from_db(tournament_id)
        if tournament and len(tournament.participants) >= 16:
            tournament.status = TournamentStatus.ONGOING
            logger.info(f"Tournament {tournament_id} started with {len(tournament.participants)} players")
            # Additional tournament start logic here
            
    except Exception as e:
        logger.error(f"Error starting tournament {tournament_id}: {e}")



def get_param_style():
    """Get correct parameter placeholder for current database"""
    return "%s" if bool(os.getenv("DATABASE_URL")) else "?"


def execute_query(cursor, query, params):
    """Execute query with proper parameter style"""
    if os.getenv("DATABASE_URL"):  # PostgreSQL
        cursor.execute(query.replace("?", "%s"), params)
    else:  # SQLite
        cursor.execute(query, params)


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


def cleanup_old_sessions():
    """Clean up expired sessions"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            
            if is_postgres:
                cur.execute("DELETE FROM user_sessions WHERE updated_at < %s", (cutoff,))
            else:
                cur.execute("DELETE FROM user_sessions WHERE updated_at < ?", (cutoff,))
    except Exception as e:
        logger.error(f"Error cleaning up sessions: {e}")

def recover_game_state(chat_id: int):
    """Attempt to recover an interrupted game"""
    try:
        game_state = GameState(chat_id)
        if game_state.data.get('state') == 'play':
            # Game was in progress
            return game_state.data
        return None
    except Exception as e:
        logger.error(f"Error recovering game state: {e}")
        return None


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

def ensure_user(message: types.Message):
    if message.from_user:
        try:
            logger.info(f"Upserting user {message.from_user.id}")
            upsert_user(message.from_user)
            logger.info(f"✓ User {message.from_user.id} upserted")
        except Exception as e:
            logger.error(f"✗ Failed to upsert user {message.from_user.id}: {e}", exc_info=True)

def setup_webhook():
    """Set up webhook with proper error handling"""
    try:
        if not USE_WEBHOOK or not WEBHOOK_URL:
            logger.info("Webhook not configured, will use polling")
            return False
        
        # Remove existing webhook
        bot.remove_webhook()
        time.sleep(1)
        
        # Construct proper webhook URL
        webhook_url = WEBHOOK_URL.rstrip('/')
        if not webhook_url.endswith(f'/webhook/{TOKEN}'):
            webhook_url = f"{webhook_url}/webhook/{TOKEN}"
        
        logger.info(f"Setting webhook to: {webhook_url}")
        
        # Set webhook with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = bot.set_webhook(
                    url=webhook_url,
                    max_connections=40,
                    drop_pending_updates=True
                )
                
                if result:
                    # Verify
                    info = bot.get_webhook_info()
                    if info.url == webhook_url:
                        logger.info("✓ Webhook set successfully")
                        return True
                    else:
                        logger.error(f"Webhook verification failed. Expected: {webhook_url}, Got: {info.url}")
                        
            except Exception as e:
                logger.error(f"Webhook setup attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    raise
        
        return False
                
    except Exception as e:
        logger.error(f"Fatal webhook setup error: {e}", exc_info=True)
        return False
# Message handlers
@bot.message_handler(commands=['start'])
@check_ban
@anticheat_middleware
@rate_limit_check('command')
def cmd_start(message):
    """Handle /start command"""
    try:
        user_id = message.from_user.id
        ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
        
        welcome_text = (
            f"🏏 Welcome to Cricket Bot, {message.from_user.first_name}!\n\n"
            "Play realistic cricket matches against the bot!\n\n"
            "Commands:\n"
            "/quickmatch - Start a quick T2 match\n"
            "/profile - View your profile\n"
            "/leaderboard - View rankings\n"
            "/daily - Daily challenges\n"
            "/powerups - Power-ups shop\n"
            "/help - Show all commands"
        )
        
        bot.send_message(message.chat.id, welcome_text)
    except Exception as e:
        logger.error(f"Error in start command: {e}")


@bot.message_handler(commands=['play'])
@check_ban
@anticheat_middleware
def cmd_play(message):
    try:
        logger.info(f"Received /play from user {message.from_user.id}")
        
        bot.send_message(
            message.chat.id,
            "🏏 Starting a new cricket match!\n\n"
            "🪙 Time for the toss! Choose heads or tails:"
        )
        
        # Add toss buttons
        keyboard = types.InlineKeyboardMarkup()
        keyboard.add(
            types.InlineKeyboardButton("🪙 Heads", callback_data="toss_heads"),
            types.InlineKeyboardButton("🪙 Tails", callback_data="toss_tails")
        )
        bot.send_message(message.chat.id, "Make your choice:", reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Error in /play handler: {e}", exc_info=True)
        bot.reply_to(message, "Sorry, couldn't start the game. Please try again.")

@bot.message_handler(commands=['help'])
def cmd_help(message):
    try:
        logger.info(f"Received /help from user {message.from_user.id}")
        
        help_text = (
            "🏏 <b>CRICKET BOT HELP</b>\n\n"
            
            "🎮 <b>GAMEPLAY COMMANDS</b>\n"
            "/play - Start a new match\n"
            "/quickmatch or /qm - Quick T2 match\n"
            "/score - View current score\n\n"
            
            "📊 <b>STATS & PROGRESS</b>\n"
            "/stats - Your statistics\n"
            "/profile - Your profile\n"
            "/achievements - View achievements\n"
            "/inventory - Your items\n"
            "/replay - Last match summary\n\n"
            
            "🏆 <b>COMPETITIVE</b>\n"
            "/leaderboard or /rankings - View rankings\n"
            "/daily - Daily challenges\n"
            "/tournaments - Tournament menu\n\n"
            
            "🛒 <b>SHOP & ITEMS</b>\n"
            "/powerups - Power-ups shop\n\n"
            
            "ℹ️ <b>INFORMATION</b>\n"
            "/commands - Full command list\n"
            "/help - This help message\n\n"
            
            "<b>📖 HOW TO PLAY:</b>\n"
            "• Send numbers 1-6 for each ball\n"
            "• Same number = OUT! ❌\n"
            "• Different numbers = RUNS! ✅\n"
            "• Win the toss to choose bat/bowl\n\n"
            
            "Need more help? Use /commands for detailed info!"
        )
        
        bot.send_message(message.chat.id, help_text)
        
    except Exception as e:
        logger.error(f"Error in /help handler: {e}", exc_info=True)



@bot.message_handler(commands=['commands'])
def cmd_commands(message):
    """Comprehensive command list"""
    try:
        commands_text = (
            "📋 <b>COMPLETE COMMAND LIST</b>\n"
            f"{'═'*50}\n\n"
            
            "🎮 <b>GAME COMMANDS</b>\n"
            "/play - Start new match (choose format)\n"
            "/quickmatch, /qm - Quick T2 match\n"
            "/score - View live score\n"
            "/forfeit - Forfeit current match\n\n"
            
            "👤 <b>PROFILE & STATS</b>\n"
            "/profile - View your profile\n"
            "/stats - Detailed statistics\n"
            "/achievements - Achievement showcase\n"
            "/inventory - Your items & power-ups\n"
            "/replay - Last match replay\n\n"
            
            "🏆 <b>COMPETITIVE</b>\n"
            "/leaderboard - Global rankings\n"
            "/rankings - Same as leaderboard\n"
            "/daily - Daily challenges\n"
            "/tournaments - Tournament system\n\n"
            
            "🛒 <b>SHOP & ECONOMY</b>\n"
            "/powerups - Buy power-ups\n"
            "/coins - Check coin balance\n\n"
            
            "ℹ️ <b>INFORMATION</b>\n"
            "/help - Quick help guide\n"
            "/commands - This list\n"
            "/start - Welcome message\n\n"
            
            "🎯 <b>MATCH FORMATS</b>\n"
            "• T1 - 1 over, 1 wicket (Blitz)\n"
            "• T2 - 2 overs, 1 wicket (Quick)\n"
            "• T5 - 5 overs, 2 wickets (Classic)\n"
            "• T10 - 10 overs, 3 wickets (Power)\n"
            "• T20 - 20 overs, 5 wickets (Premier)\n\n"
            
            "💡 <b>PRO TIPS</b>\n"
            "• Complete daily challenges for rewards\n"
            "• Use power-ups strategically\n"
            "• Check leaderboards for competition\n"
            "• Join tournaments for prizes\n\n"
            
            f"{'═'*50}\n"
            "Need help? Contact support or use /help"
        )
        
        bot.send_message(message.chat.id, commands_text)
        
    except Exception as e:
        logger.error(f"Error in /commands handler: {e}")


@bot.message_handler(commands=['stats'])
def cmd_stats(message):
    try:
        logger.info(f"Received /stats from user {message.from_user.id}")
        bot.send_message(message.chat.id, "📊 Your stats will be shown here (feature in development)")
    except Exception as e:
        logger.error(f"Error in /stats handler: {e}", exc_info=True)

@bot.message_handler(commands=["leaderboard"])
def cmd_leaderboard(message: types.Message):
    try:
        ensure_user(message)
        show_leaderboard(message.chat.id)
    except Exception as e:
        logger.error(f"Error in leaderboard command: {e}")
        bot.send_message(message.chat.id, "❌ Error loading leaderboard.")

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
def handle_game_input(message):
    try:
        ensure_user(message)
        number = int(message.text)
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        logger.info(f"Game input {number} from user {user_id}")
        
        # Check if there's an active game
        g = safe_load_game(chat_id)
        if not g or g.get("state") != "play":
            bot.reply_to(message, "No active match found. Use /play to start a new match.")
            return
            
        # Process the ball
        result = enhanced_process_ball_v2(chat_id, number, user_id)
        
        if isinstance(result, dict):
            # Handle the result properly
            commentary_msg = result['commentary']
            
            if result.get('match_ended'):
                bot.send_message(chat_id, commentary_msg)
                # Match end logic handled in enhanced_process_ball_v2
            else:
                # Show ball result and current score
                bot.send_message(chat_id, commentary_msg)
                
                if result.get('over_completed'):
                    bot.send_message(chat_id, "🎯 Over completed!")
                    
                if result.get('powerplay_ended'):
                    bot.send_message(chat_id, "⚡ Powerplay ended!")
                    
                # Show updated score
                show_live_score(chat_id, result['game_state'], detailed=False)
        else:
            # Handle string responses (errors)
            bot.reply_to(message, str(result))
            
    except Exception as e:
        logger.error(f"Error in game input handler: {e}", exc_info=True)
        bot.reply_to(message, "Error processing your move. Please try again.")

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
def handle_callback(call):
    try:
        logger.info(f"Received callback: {call.data} from user {call.from_user.id}")
        
        # ADD ALL THESE MISSING HANDLERS:
        data = call.data
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Toss handlers
        if data in ["toss_heads", "toss_tails"]:
            choice = data.split("_")[1]
            bot.answer_callback_query(call.id, f"You chose {choice}!")
            handle_toss_result(chat_id, choice, user_id)
            
        # Bat/Bowl choice
        elif data in ["choose_bat", "choose_bowl"]:
            choice = "player" if data == "choose_bat" else "bot"
            bot.answer_callback_query(call.id, "Starting match...")
            safe_set_batting_order(chat_id, choice)
            
        # Main menu handlers
        elif data == "quick_play":
            bot.answer_callback_query(call.id, "Starting quick play...")
            safe_start_new_game(chat_id, user_id=user_id)
            
        elif data == "custom_match":
            bot.answer_callback_query(call.id, "Custom match...")
            bot.send_message(chat_id, "Choose difficulty:", reply_markup=kb_difficulty_select())
            
        elif data == "my_stats":
            bot.answer_callback_query(call.id, "Loading stats...")
            show_user_stats(chat_id, user_id)
            
        elif data == "leaderboard":
            bot.answer_callback_query(call.id, "Loading leaderboard...")
            show_leaderboard(chat_id)
            
        # Difficulty selection
        elif data.startswith("diff_"):
            difficulty = data.split("_")[1]
            bot.answer_callback_query(call.id, f"Selected {difficulty} difficulty")
            bot.send_message(chat_id, "Choose format:", reply_markup=kb_format_select())
            set_user_session_data(user_id, "selected_difficulty", difficulty)
            
        # Format selection  
        elif data.startswith("format_"):
            parts = data.split("_")
            if len(parts) >= 3:
                overs = int(parts[1])
                wickets = int(parts[2])
                difficulty = get_user_session_data(user_id, "selected_difficulty", "medium")
                bot.answer_callback_query(call.id, f"Starting T{overs} match...")
                safe_start_new_game(chat_id, overs, wickets, difficulty, user_id)
            
        # Post-match actions
        elif data == "play_again":
            bot.answer_callback_query(call.id, "Starting new match...")
            safe_start_new_game(chat_id, user_id=user_id)
            
        elif data == "match_summary":
            bot.answer_callback_query(call.id, "Loading summary...")
            # Show match summary logic here
            
        elif data == "forfeit_yes":
            bot.answer_callback_query(call.id, "Match forfeited")
            delete_game(chat_id)
            bot.send_message(chat_id, "Match forfeited. Use /play for a new match.")
            
        elif data == "forfeit_no":
            bot.answer_callback_query(call.id, "Continuing match...")
            g = safe_load_game(chat_id)
            if g:
                show_live_score(chat_id, g)
                
        # Live score
        elif data == "live_score":
            bot.answer_callback_query(call.id, "Current score...")
            g = safe_load_game(chat_id)
            if g:
                show_live_score(chat_id, g)
            else:
                bot.send_message(chat_id, "No active match found.")
                
        # Tournament handlers (simplified)
        elif data == "tournaments":
            bot.answer_callback_query(call.id, "Loading tournaments...")
            handle_tournament_menu(chat_id)
            
        elif data == "challenges":
            bot.answer_callback_query(call.id, "Loading challenges...")
            show_challenges_menu(chat_id, user_id)
            
        # Back to main menu
        elif data in ["main_menu", "back_main"]:
            bot.answer_callback_query(call.id, "Main menu")
            welcome_text = f"🏏 Welcome back! What would you like to do?"
            bot.send_message(chat_id, welcome_text, reply_markup=kb_main_menu())
        
        elif data == "tournament_menu":
            handle_tournament_menu(chat_id, user_id)

        elif data == "tournament_create":
            handle_create_tournament(chat_id, user_id)

        elif data.startswith("fmt_"):
            format_key = data
            handle_tournament_type_selection(chat_id, user_id, format_key)

        elif data.startswith("type_"):
            tournament_type = data.split("_")[1]
            handle_tournament_theme_selection(chat_id, user_id, tournament_type)

        elif data.startswith("theme_"):
            theme = data.split("_")[1]
            finalize_tournament_creation(chat_id, user_id, theme)

        elif data.startswith("tourn_view_"):
            tournament_id = data.split("_")[2]
            show_tournament_participants(chat_id, tournament_id)

        elif data.startswith("join_tourn_"):
            tournament_id = data.split("_")[2]
            user_id = call.from_user.id
            username = call.from_user.first_name or call.from_user.username
            
            try:
                tournament = load_tournament_from_db(tournament_id)
                
                if not tournament:
                    bot.answer_callback_query(call.id, "Tournament not found", show_alert=True)
                    return
                
                # Check if tournament is full
                if len(tournament.participants) >= 16:
                    bot.answer_callback_query(call.id, "Tournament is full", show_alert=True)
                    return
                
                # Check if already joined
                if any(p['user_id'] == user_id for p in tournament.participants):
                    bot.answer_callback_query(call.id, "Already registered", show_alert=True)
                    return
                
                # Add participant
                result = tournament.add_participant(user_id, username)
                
                if result['success']:
                    # Save updated tournament
                    save_tournament_to_db(tournament, call.message.chat.id)
                    
                    success_msg = (
                        f"✅ {result['message']}\n\n"
                        f"Participants: {result['participants']}\n\n"
                        f"Waiting for tournament to start..."
                    )
                    
                    kb = types.InlineKeyboardMarkup(row_width=1)
                    kb.add(
                        types.InlineKeyboardButton("👥 View Participants", callback_data=f"tourn_view_{tournament_id}"),
                        types.InlineKeyboardButton("🔙 Back", callback_data="tournament_menu")
                    )
                    
                    bot.send_message(call.message.chat.id, success_msg, reply_markup=kb)
                    bot.answer_callback_query(call.id, "Joined successfully!", show_alert=False)
                else:
                    bot.answer_callback_query(call.id, f"Join failed: {result['message']}", show_alert=True)
            
            except Exception as e:
                logger.error(f"Error joining tournament: {e}")
                bot.answer_callback_query(call.id, "Error joining tournament", show_alert=True)
            
        else:
            # Log unhandled callbacks for debugging
            logger.warning(f"Unhandled callback: {data}")
            bot.answer_callback_query(call.id, "Feature coming soon!")
            
    except Exception as e:
        logger.error(f"Error in callback handler: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "An error occurred")


@bot.callback_query_handler(func=lambda call: call.data.startswith('lb_'))
@rate_limit_check('callback')
def handle_leaderboard_callback(call):
    """Handle leaderboard selection - FIXED"""
    try:
        category = call.data.replace('lb_', '')
        
        # Map callback data to actual database categories
        category_map = {
            'highest_score': 'highest_score',
            'most_wins': 'most_wins', 
            'win_streak': 'win_streak',
            'most_sixes': 'most_sixes',
            'best_strike_rate': 'best_strike_rate',
            'tournament_wins': 'tournament_wins',
            'total_xp': 'total_xp'
        }
        
        actual_category = category_map.get(category, category)
        
        # Get leaderboard data
        top_players = get_leaderboard(actual_category, limit=10)
        
        if not top_players:
            display = f"📊 No data available for {category.replace('_', ' ').title()} yet!"
        else:
            display = generate_leaderboard_display(actual_category, top_players)
        
        bot.edit_message_text(
            display,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=kb_leaderboard_categories()
        )
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Error in leaderboard callback: {e}")
        bot.answer_callback_query(call.id, "Error loading leaderboard")


@bot.callback_query_handler(func=lambda call: call.data == 'tournament_list')
def handle_tournament_list(call):
    show_all_tournaments(call.message.chat.id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == 'tournament_rankings')
def handle_tournament_rankings(call):
    show_tournament_rankings(call.message.chat.id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == 'tournament_history')
def handle_tournament_history(call):
    show_user_tournament_history(call.message.chat.id, call.from_user.id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == 'challenges_view')
def handle_challenges_view_callback(call):
    show_daily_challenges(call.message.chat.id, call.from_user.id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == 'challenges_claim')
def handle_challenges_claim_callback(call):
    claim_challenge_rewards(call.message.chat.id, call.from_user.id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == 'challenges_history')
def handle_challenges_history_callback(call):
    show_challenge_history(call.message.chat.id, call.from_user.id)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data in ["toss_heads", "toss_tails"])
@rate_limit_check('callback')
def handle_toss_callback(call):
    """Handle toss selection"""
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        choice = call.data.split("_")[1]
        
        bot.answer_callback_query(call.id, f"You chose {choice}!")
        handle_toss_result(chat_id, choice, user_id)
        
    except Exception as e:
        logger.error(f"Error in toss callback: {e}")
        bot.answer_callback_query(call.id, "Error processing toss")


@bot.callback_query_handler(func=lambda call: call.data.startswith('info_powerup_'))
@rate_limit_check('callback')
def handle_powerup_info(call):
    """Show power-up information"""
    try:
        powerup_id = call.data.replace('info_powerup_', '')
        
        if powerup_id not in PowerUp.POWERUPS:
            bot.answer_callback_query(call.id, "Power-up not found")
            return
        
        powerup = PowerUp.POWERUPS[powerup_id]
        
        info_text = (
            f"{powerup['name']}\n\n"
            f"📝 {powerup['description']}\n\n"
            f"⏱️ Duration: {powerup['duration']} over(s)\n"
            f"💰 Cost: {powerup['cost']} coins\n\n"
            f"Effect: {powerup['effect']['type'].replace('_', ' ').title()}\n"
            f"Value: +{int(powerup['effect']['value'] * 100)}%"
        )
        
        bot.answer_callback_query(call.id, info_text, show_alert=True)
        
    except Exception as e:
        logger.error(f"Error showing powerup info: {e}")
        bot.answer_callback_query(call.id, "Error loading info")


@bot.message_handler(commands=['coins'])
@rate_limit_check('command')
def cmd_coins(message):
    """Check coin balance"""
    try:
        user_id = message.from_user.id
        coins = _get_user_coins(user_id)
        
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            # Get level for bonus info
            cur.execute(f"SELECT level FROM user_levels WHERE user_id = {param_style}", (user_id,))
            level_row = cur.fetchone()
            level = level_row['level'] if level_row else 1
        
        coins_text = (
            f"💰 <b>YOUR COINS</b>\n\n"
            f"Balance: <b>{coins}</b> coins\n"
            f"Level: {level}\n\n"
            f"<b>Earn Coins:</b>\n"
            f"• Win matches: 50 coins\n"
            f"• Complete daily challenges\n"
            f"• Level up rewards\n"
            f"• Tournament prizes\n\n"
            f"<b>Spend Coins:</b>\n"
            f"Use /powerups to buy power-ups!"
        )
        
        bot.send_message(message.chat.id, coins_text)
        
    except Exception as e:
        logger.error(f"Error in coins command: {e}")


@bot.callback_query_handler(func=lambda call: call.data in ["choose_bat", "choose_bowl"])
@rate_limit_check('callback')
def handle_bat_bowl_callback(call):
    """Handle bat/bowl choice after winning toss"""
    try:
        chat_id = call.message.chat.id
        choice = "player" if call.data == "choose_bat" else "bot"
        
        bot.answer_callback_query(call.id, "Starting match...")
        safe_set_batting_order(chat_id, choice)
        
    except Exception as e:
        logger.error(f"Error in bat/bowl callback: {e}")
        bot.answer_callback_query(call.id, "Error starting match")



@bot.callback_query_handler(func=lambda call: call.data.startswith('buy_powerup_'))
@rate_limit_check('callback')
def handle_powerup_purchase_callback(call):
    """Handle power-up purchase"""
    try:
        powerup_id = call.data.replace('buy_powerup_', '')
        result = handle_powerup_purchase(call.from_user.id, powerup_id)
        
        bot.answer_callback_query(call.id, result['message'], show_alert=True)
        
        if result['success']:
            # Refresh shop display
            shop_text = (
                "🛒 POWER-UPS SHOP\n\n"
                "Purchase power-ups to gain advantages in matches!\n"
                "Use them before starting a match.\n\n"
            )
            
            for pid, powerup in PowerUp.POWERUPS.items():
                shop_text += (
                    f"{powerup['name']}\n"
                    f"  {powerup['description']}\n"
                    f"  Duration: {powerup['duration']} over(s) | Cost: {powerup['cost']} coins\n\n"
                )
            
            user_coins = _get_user_coins(call.from_user.id)
            shop_text += f"💰 Your coins: {user_coins}"
            
            bot.edit_message_text(
                shop_text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=kb_powerups_shop()
            )
    except Exception as e:
        logger.error(f"Error in powerup purchase callback: {e}")
        bot.answer_callback_query(call.id, "Purchase failed")


@bot.callback_query_handler(func=lambda call: call.data == 'view_scorecard')
@rate_limit_check('callback')
def handle_scorecard_view(call):
    """Show detailed scorecard"""
    try:
        game = GameState(call.message.chat.id)
        scorecard = generate_live_scorecard(game)
        
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, f"<pre>{scorecard}</pre>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error showing scorecard: {e}")
        bot.answer_callback_query(call.id, "Error loading scorecard")


@bot.callback_query_handler(func=lambda call: call.data == 'challenges_view')
@rate_limit_check('callback')
def handle_challenges_view(call):
    """Show active challenges"""
    try:
        tracker = ChallengeTracker(call.from_user.id)
        
        if not tracker.active_challenges:
            message = "📋 No active challenges!\nChallenges reset daily at midnight UTC."
        else:
            message = "📋 DAILY CHALLENGES\n" + "═" * 40 + "\n\n"
            
            for challenge in tracker.active_challenges:
                progress = tracker.progress.get(challenge['id'], 0)
                target = challenge['target']
                completed = challenge.get('completed', False)
                
                status = "✅" if completed else f"{progress}/{target}"
                
                message += (
                    f"{challenge.get('icon', '🎯')} {challenge['description']}\n"
                    f"   Progress: {status}\n"
                    f"   Reward: {challenge['reward_coins']} coins | {challenge['reward_xp']} XP\n\n"
                )
        
        bot.edit_message_text(
            message,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=kb_challenges()
        )
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Error viewing challenges: {e}")
        bot.answer_callback_query(call.id, "Error loading challenges")


@bot.callback_query_handler(func=lambda call: call.data.startswith('shot_'))
@rate_limit_check('callback')
def handle_shot_selection(call):
    """Handle batting shot selection"""
    try:
        shot_type = call.data.replace('shot_', '')
        game = GameState(call.message.chat.id)
        
        if game.data.get('batting') != 'player':
            bot.answer_callback_query(call.id, "It's not your turn to bat!")
            return
        
        # Map shot types to aggressive levels
        shot_aggression = {
            'defend': 0.2,
            'single': 0.4,
            'two': 0.5,
            'attack': 0.7,
            'boundary': 0.85,
            'big': 0.95
        }
        
        aggression = shot_aggression.get(shot_type, 0.5)
        
        # Simulate ball outcome based on shot
        outcome = simulate_player_ball(game, aggression)
        
        # Update game state and show result
        commentary = generate_ball_commentary(
            outcome['type'],
            outcome['runs'],
            call.from_user.first_name,
            game.data.get('is_powerplay', False)
        )
        
        bot.answer_callback_query(call.id, commentary[:200])  # Show in popup
        
        # Send full update
        scorecard = generate_live_scorecard(game)
        bot.send_message(
            call.message.chat.id,
            f"{commentary}\n\n<pre>{scorecard}</pre>",
            parse_mode="HTML",
            reply_markup=kb_match_actions(game)
        )
        
    except Exception as e:
        logger.error(f"Error handling shot selection: {e}")
        bot.answer_callback_query(call.id, "Error processing shot")


def simulate_player_ball(game: GameState, aggression: float) -> dict:
    """Simulate a ball based on player's shot selection"""
    difficulty = calculate_dynamic_difficulty(game, {})  # Pass user stats if available
    
    # Check for active power-ups
    powerup_effects = PowerUp.check_powerup_effects(game, 'batting')
    boundary_bonus = powerup_effects.get('boundary_bonus', 0)
    
    # Wicket probability (inverse to aggression)
    wicket_chance = difficulty * (aggression * 0.3)
    
    # Check for wicket protection power-up
    wicket_save_chance = powerup_effects.get('wicket_save_chance', 0)
    if random.random() < wicket_save_chance:
        wicket_chance = 0  # Power-up saves from wicket
    
    # Auto-save wicket power-up
    if powerup_effects.get('auto_save_wicket') and random.random() < wicket_chance:
        wicket_chance = 0
        bot.send_message(game.chat_id, "🛡️ WICKET GUARD activated! Wicket saved!")
    
    if random.random() < wicket_chance:
        game.data['player_wkts'] += 1
        return {'type': 'wicket', 'runs': 0}
    
    # Runs distribution based on aggression
    rand = random.random()
    
    if aggression > 0.85:  # Big shot
        if rand < 0.25 + boundary_bonus:
            runs = 6
        elif rand < 0.45 + boundary_bonus:
            runs = 4
        elif rand < 0.65:
            runs = random.choice([1, 2, 3])
        else:
            runs = 0  # Dot ball
    elif aggression > 0.65:  # Aggressive
        if rand < 0.15 + boundary_bonus:
            runs = 6
        elif rand < 0.35 + boundary_bonus:
            runs = 4
        elif rand < 0.70:
            runs = random.choice([1, 2, 3])
        else:
            runs = 0
    elif aggression > 0.45:  # Moderate
        if rand < 0.05 + boundary_bonus:
            runs = 6
        elif rand < 0.20 + boundary_bonus:
            runs = 4
        elif rand < 0.75:
            runs = random.choice([1, 2])
        else:
            runs = 0
    else:  # Defensive
        if rand < 0.02:
            runs = 4
        elif rand < 0.60:
            runs = random.choice([0, 1])
        else:
            runs = 0
    
    # Apply weather/pitch effects
    runs = apply_weather_pitch_effects(game, 'batting', runs)
    
    # Update game stats
    game.data['player_score'] += runs
    game.data['player_balls_faced'] += 1
    game.data['balls_in_over'] += 1
    
    if runs == 4:
        game.data['player_fours'] += 1
    elif runs == 6:
        game.data['player_sixes'] += 1
    
    # Check over completion
    if game.data['balls_in_over'] >= 6:
        game.data['balls_in_over'] = 0
        game.data['overs_bowled'] += 1
        PowerUp.update_powerup_durations(game)
    
    game.save()
    
    return {'type': 'runs' if runs > 0 else 'dot', 'runs': runs}


@bot.message_handler(commands=['powerups'])
@rate_limit_check('command')
def cmd_powerups(message):
    """Show power-ups shop"""
    try:
        user_coins = _get_user_coins(message.from_user.id)
        
        shop_text = (
            "🛒 POWER-UPS SHOP\n\n"
            "Purchase power-ups to gain advantages in matches!\n\n"
        )
        
        for powerup_id, powerup in PowerUp.POWERUPS.items():
            shop_text += (
                f"{powerup['name']}\n"
                f"  {powerup['description']}\n"
                f"  Duration: {powerup['duration']} over(s) | Cost: {powerup['cost']} coins\n\n"
            )
        
        shop_text += f"💰 Your coins: {user_coins}"
        
        bot.send_message(
            message.chat.id,
            shop_text,
            reply_markup=kb_powerups_shop()
        )
    except Exception as e:
        logger.error(f"Error in powerups command: {e}")



@bot.message_handler(commands=['achievements'])
@rate_limit_check('command')
def cmd_achievements(message):
    """Show user achievements"""
    try:
        achievements = AchievementSystem.get_user_achievements(message.from_user.id)
        display = AchievementSystem.generate_achievement_display(achievements)
        
        bot.send_message(message.chat.id, display, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error in achievements command: {e}")
        bot.reply_to(message, "❌ Error loading achievements")


@bot.message_handler(commands=['leaderboard', 'rankings'])
@rate_limit_check('command')
def cmd_leaderboard(message):
    """Show leaderboards"""
    try:
        text = (
            "🏆 LEADERBOARDS 🏆\n\n"
            "Choose a category to view rankings:\n"
        )
        
        bot.send_message(
            message.chat.id,
            text,
            reply_markup=kb_leaderboard_categories()
        )
    except Exception as e:
        logger.error(f"Error in leaderboard command: {e}")



@bot.message_handler(commands=['inventory'])
@rate_limit_check('command')
def cmd_inventory(message):
    """Show user inventory"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            cur.execute(f"""
                SELECT item_type, item_id, quantity
                FROM user_inventory
                WHERE user_id = {param_style}
            """, (message.from_user.id,))
            
            items = cur.fetchall()
            
            if not items:
                text = "🎒 Your inventory is empty!\nPurchase items from /powerups"
            else:
                text = "🎒 YOUR INVENTORY\n" + "═" * 40 + "\n\n"
                
                for item in items:
                    item_type = item['item_type']
                    item_id = item['item_id']
                    quantity = item['quantity']
                    
                    if item_type == 'powerup' and item_id in PowerUp.POWERUPS:
                        powerup = PowerUp.POWERUPS[item_id]
                        text += f"{powerup['name']} x{quantity}\n"
                
                text += f"\n💡 Use /powerups to purchase more items"
            
            bot.send_message(message.chat.id, text)
    except Exception as e:
        logger.error(f"Error in inventory command: {e}")
        bot.reply_to(message, "❌ Error loading inventory")


@bot.message_handler(commands=['profile'])
@rate_limit_check('command')
def cmd_profile(message):
    """Show user profile"""
    try:
        user_id = message.from_user.id
        
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            # Get user data
            cur.execute(f"SELECT * FROM users WHERE user_id = {param_style}", (user_id,))
            user = cur.fetchone()
            
            if not user:
                bot.reply_to(message, "Profile not found. Use /start to create your profile!")
                return
            
            # Get stats
            cur.execute(f"SELECT * FROM stats WHERE user_id = {param_style}", (user_id,))
            stats = cur.fetchone()
            
            # Get level
            cur.execute(f"SELECT * FROM user_levels WHERE user_id = {param_style}", (user_id,))
            level = cur.fetchone()
            
            games_played = stats['games_played'] if stats else 0
            wins = stats['wins'] if stats else 0
            win_rate = (wins / games_played * 100) if games_played > 0 else 0
            
            profile_text = (
                f"{'═'*50}\n"
                f"👤 PLAYER PROFILE\n"
                f"{'═'*50}\n\n"
                f"🆔 Name: {user['first_name']}\n"
                f"💎 Level: {level['level'] if level else 1}\n"
                f"💰 Coins: {user['coins']}\n\n"
                f"📈 STATISTICS\n"
                f"{'─'*50}\n"
                f"🎮 Matches: {games_played}\n"
                f"✅ Wins: {wins}\n"
                f"📊 Win Rate: {win_rate:.1f}%\n"
            )
            
            if stats:
                profile_text += f"🏏 High Score: {stats['high_score']}\n"
            
            bot.send_message(message.chat.id, profile_text)
            
    except Exception as e:
        logger.error(f"Error in profile command: {e}")


@bot.message_handler(commands=['replay'])
@rate_limit_check('command')
def cmd_replay(message):
    """Show last match replay/summary"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            cur.execute(f"""
                SELECT * FROM match_history
                WHERE user_id = {param_style}
                ORDER BY created_at DESC
                LIMIT 1
            """, (message.from_user.id,))
            
            last_match = cur.fetchone()
            
            if not last_match:
                bot.reply_to(message, "No match history found!")
                return
            
            # Build replay display
            replay = (
                f"{'🎬'*15}\n"
                f"  MATCH REPLAY\n"
                f"{'🎬'*15}\n\n"
                f"Format: {last_match['match_format']}\n"
                f"Date: {last_match['created_at'][:19]}\n\n"
                f"{'─'*40}\n"
                f"🏏 YOUR INNINGS\n"
                f"{'─'*40}\n"
                f"Score: {last_match['player_score']}/{last_match['player_wickets']}\n"
                f"Overs: {last_match['overs_played']:.1f}\n"
                f"Strike Rate: {last_match['player_strike_rate']:.1f}\n\n"
                f"{'─'*40}\n"
                f"🤖 BOT INNINGS\n"
                f"{'─'*40}\n"
                f"Score: {last_match['bot_score']}/{last_match['bot_wickets']}\n\n"
                f"{'═'*40}\n"
            )
            
            result = last_match['result']
            margin = last_match['margin']
            
            if result == 'win':
                replay += f"🏆 YOU WON by {margin}!\n"
            elif result == 'loss':
                replay += f"💔 YOU LOST by {margin}\n"
            else:
                replay += f"🤝 MATCH TIED!\n"
            
            replay += f"{'═'*40}\n"
            replay += f"Duration: {last_match['match_duration_minutes']} minutes"
            
            bot.send_message(message.chat.id, replay)
            
    except Exception as e:
        logger.error(f"Error in replay command: {e}")
        bot.reply_to(message, "❌ Error loading replay")


@bot.message_handler(commands=['daily'])
@rate_limit_check('command')
def cmd_daily(message):
    """Show daily challenges"""
    try:
        tracker = ChallengeTracker(message.from_user.id)
        
        if not tracker.active_challenges:
            text = (
                "📋 DAILY CHALLENGES\n\n"
                "No active challenges at the moment!\n"
                "Challenges refresh daily at midnight UTC.\n\n"
                "Come back later! 🎯"
            )
        else:
            text = (
                "📋 DAILY CHALLENGES\n"
                "Complete challenges to earn rewards!\n"
                f"{'═'*40}\n\n"
            )
            
            for challenge in tracker.active_challenges:
                progress = tracker.progress.get(challenge['id'], 0)
                target = challenge['target']
                completed = challenge.get('completed', False)
                claimed = challenge.get('claimed', False)
                
                if completed and claimed:
                    status = "✅ CLAIMED"
                elif completed:
                    status = "🎁 READY TO CLAIM"
                else:
                    status = f"📊 {progress}/{target}"
                
                text += (
                    f"{challenge.get('icon', '🎯')} {challenge['description']}\n"
                    f"   Status: {status}\n"
                    f"   Reward: {challenge['reward_coins']} coins | {challenge['reward_xp']} XP\n\n"
                )
        
        bot.send_message(message.chat.id, text)
        
    except Exception as e:
        logger.error(f"Error in daily command: {e}")


@bot.message_handler(commands=['quickmatch', 'qm'])
@rate_limit_check('command')
def cmd_quick_match(message):
    """Start a quick match"""
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
        
        game = GameState(chat_id)
        game.data = default_game(chat_id)
        game.save()
        
        text = (
            "⚡ QUICK MATCH STARTED! ⚡\n\n"
            f"Format: T2 (2 overs, 1 wicket)\n"
            f"Difficulty: Medium\n\n"
            "Time to toss the coin! 🪙"
        )
        
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("🪙 Heads", callback_data="toss_heads"),
            types.InlineKeyboardButton("🪙 Tails", callback_data="toss_tails")
        )
        
        bot.send_message(chat_id, text, reply_markup=kb)
        
    except Exception as e:
        logger.error(f"Error in quick match: {e}")



@bot.callback_query_handler(func=lambda call: call.data == 'view_achievements')
@rate_limit_check('callback')
def handle_view_achievements_callback(call):
    """Show achievements from profile"""
    try:
        achievements = AchievementSystem.get_user_achievements(call.from_user.id)
        display = AchievementSystem.generate_achievement_display(achievements)
        
        bot.send_message(call.message.chat.id, display, parse_mode="HTML")
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Error viewing achievements: {e}")
        bot.answer_callback_query(call.id, "Error loading achievements")


@bot.callback_query_handler(func=lambda call: call.data == 'shop')
@rate_limit_check('callback')
def handle_shop_callback(call):
    """Show shop from profile"""
    try:
        user_coins = _get_user_coins(call.from_user.id)
        
        shop_text = (
            "🛒 CRICKET BOT SHOP\n"
            f"{'═'*40}\n\n"
            "💪 POWER-UPS\n"
            "Boost your performance in matches!\n\n"
        )
        
        for powerup_id, powerup in PowerUp.POWERUPS.items():
            shop_text += f"{powerup['name']} - {powerup['cost']} coins\n"
        
        shop_text += (
            f"\n{'─'*40}\n"
            f"💰 Your Balance: {user_coins} coins\n\n"
            "Use /powerups to purchase items!"
        )
        
        bot.send_message(call.message.chat.id, shop_text)
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Error showing shop: {e}")
        bot.answer_callback_query(call.id, "Error loading shop")


@bot.callback_query_handler(func=lambda call: call.data == 'inventory')
@rate_limit_check('callback')
def handle_inventory_callback(call):
    """Show inventory from profile"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            cur.execute(f"""
                SELECT item_type, item_id, quantity
                FROM user_inventory
                WHERE user_id = {param_style}
            """, (call.from_user.id,))
            
            items = cur.fetchall()
            
            if not items:
                text = "🎒 Your inventory is empty!\n\nPurchase items from /powerups"
            else:
                text = "🎒 YOUR INVENTORY\n" + "═" * 40 + "\n\n"
                
                for item in items:
                    item_type = item['item_type']
                    item_id = item['item_id']
                    quantity = item['quantity']
                    
                    if item_type == 'powerup' and item_id in PowerUp.POWERUPS:
                        powerup = PowerUp.POWERUPS[item_id]
                        text += f"{powerup['name']} x{quantity}\n"
            
            bot.send_message(call.message.chat.id, text)
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Error showing inventory: {e}")
        bot.answer_callback_query(call.id, "Error loading inventory")


@bot.callback_query_handler(func=lambda call: call.data.startswith('claim_'))
@rate_limit_check('callback')
def handle_challenge_claim_callback(call):
    """Claim challenge rewards"""
    try:
        challenge_id = int(call.data.replace('claim_', ''))
        
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            # Check if challenge is completed and not claimed
            cur.execute(f"""
                SELECT uc.completed, uc.claimed, dc.reward_coins, dc.reward_xp, dc.description
                FROM user_challenges uc
                JOIN daily_challenges dc ON uc.challenge_id = dc.id
                WHERE uc.user_id = {param_style} AND uc.challenge_id = {param_style}
            """, (call.from_user.id, challenge_id))
            
            challenge = cur.fetchone()
            
            if not challenge:
                bot.answer_callback_query(call.id, "Challenge not found!", show_alert=True)
                return
            
            if not challenge['completed']:
                bot.answer_callback_query(call.id, "Challenge not completed yet!", show_alert=True)
                return
            
            if challenge['claimed']:
                bot.answer_callback_query(call.id, "Already claimed!", show_alert=True)
                return
            
            # Award rewards
            _award_coins(call.from_user.id, challenge['reward_coins'])
            _award_xp(call.from_user.id, challenge['reward_xp'])
            
            # Mark as claimed
            if is_postgres:
                cur.execute("""
                    UPDATE user_challenges SET claimed = TRUE
                    WHERE user_id = %s AND challenge_id = %s
                """, (call.from_user.id, challenge_id))
            else:
                cur.execute("""
                    UPDATE user_challenges SET claimed = 1
                    WHERE user_id = ? AND challenge_id = ?
                """, (call.from_user.id, challenge_id))
            
            reward_text = (
                f"🎁 REWARD CLAIMED!\n\n"
                f"Challenge: {challenge['description']}\n"
                f"💰 Coins: +{challenge['reward_coins']}\n"
                f"📈 XP: +{challenge['reward_xp']}"
            )
            
            bot.answer_callback_query(call.id, reward_text, show_alert=True)
            
            # Refresh challenges view
            tracker = ChallengeTracker(call.from_user.id)
            message = "📋 DAILY CHALLENGES\n" + "═" * 40 + "\n\n"
            
            for ch in tracker.active_challenges:
                progress = tracker.progress.get(ch['id'], 0)
                target = ch['target']
                completed = ch.get('completed', False)
                claimed = ch.get('claimed', False) or (ch['id'] == challenge_id)
                
                if completed and claimed:
                    status = "✅ CLAIMED"
                elif completed:
                    status = "🎁 READY TO CLAIM"
                else:
                    status = f"📊 {progress}/{target}"
                
                message += (
                    f"{ch.get('icon', '🎯')} {ch['description']}\n"
                    f"   Status: {status}\n"
                    f"   Reward: {ch['reward_coins']} coins | {ch['reward_xp']} XP\n\n"
                )
            
            bot.edit_message_text(
                message,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=kb_challenges()
            )
            
    except Exception as e:
        logger.error(f"Error claiming challenge: {e}")
        bot.answer_callback_query(call.id, "Error claiming reward")


@bot.callback_query_handler(func=lambda call: call.data == 'detailed_stats')
@rate_limit_check('callback')
def handle_detailed_stats_callback(call):
    """Show detailed match statistics"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            # Get recent matches
            cur.execute(f"""
                SELECT * FROM match_history
                WHERE user_id = {param_style}
                ORDER BY created_at DESC
                LIMIT 5
            """, (call.from_user.id,))
            
            recent_matches = cur.fetchall()
            
            if not recent_matches:
                bot.answer_callback_query(call.id, "No match history found!")
                return
            
            stats_text = (
                "📊 DETAILED STATISTICS\n"
                f"{'═'*40}\n\n"
                "📈 RECENT MATCHES (Last 5)\n"
                f"{'─'*40}\n"
            )
            
            for i, match in enumerate(recent_matches, 1):
                result_emoji = "✅" if match['result'] == 'win' else "❌" if match['result'] == 'loss' else "🤝"
                stats_text += (
                    f"{i}. {result_emoji} {match['match_format']} - "
                    f"{match['player_score']}/{match['player_wickets']} "
                    f"vs {match['bot_score']}/{match['bot_wickets']}\n"
                )
            
            # Calculate recent performance
            wins = sum(1 for m in recent_matches if m['result'] == 'win')
            avg_score = sum(m['player_score'] for m in recent_matches) / len(recent_matches)
            
            stats_text += (
                f"\n{'─'*40}\n"
                f"Recent Form: {wins}/5 wins\n"
                f"Avg Score: {avg_score:.1f}\n"
            )
            
            bot.send_message(call.message.chat.id, stats_text)
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"Error showing detailed stats: {e}")
        bot.answer_callback_query(call.id, "Error loading stats")


@bot.callback_query_handler(func=lambda call: call.data.startswith('bowl_'))
@rate_limit_check('callback')
def handle_bowling_selection(call):
    """Handle bowling delivery selection"""
    try:
        bowl_type = call.data.replace('bowl_', '')
        game = GameState(call.message.chat.id)
        
        if game.data.get('batting') != 'bot':
            bot.answer_callback_query(call.id, "Bot is not batting!")
            return
        
        # Simulate bot's batting response to your bowling
        difficulty = calculate_dynamic_difficulty(game, {})
        
        # Different bowling types have different effectiveness
        bowling_effectiveness = {
            'yorker': {'wicket': 0.25, 'dot': 0.45},
            'bouncer': {'wicket': 0.15, 'boundary': 0.25},
            'spin': {'wicket': 0.20, 'dot': 0.40},
            'slower': {'wicket': 0.18, 'dot': 0.50},
            'fast': {'wicket': 0.12, 'boundary': 0.30},
            'wide_yorker': {'wicket': 0.22, 'dot': 0.38}
        }
        
        effectiveness = bowling_effectiveness.get(bowl_type, {'wicket': 0.15, 'dot': 0.40})
        
        # Check for power-ups
        powerup_effects = PowerUp.check_powerup_effects(game, 'bowling')
        wicket_bonus = powerup_effects.get('wicket_bonus', 0)
        
        wicket_chance = effectiveness.get('wicket', 0.15) + wicket_bonus
        
        rand = random.random()
        
        if rand < wicket_chance:
            # Wicket!
            game.data['bot_wkts'] += 1
            game.data['bot_balls_faced'] += 1
            game.data['balls_in_over'] += 1
            
            commentary = f"🎯 WICKET! Excellent {bowl_type}!"
            runs = 0
            
        elif rand < wicket_chance + effectiveness.get('dot', 0.40):
            # Dot ball
            game.data['bot_balls_faced'] += 1
            game.data['balls_in_over'] += 1
            
            commentary = f"Good {bowl_type} - dot ball"
            runs = 0
            
        else:
            # Runs scored by bot
            boundary_chance = effectiveness.get('boundary', 0.20)
            
            if rand < wicket_chance + effectiveness.get('dot', 0.40) + boundary_chance:
                # Boundary
                runs = 6 if random.random() < 0.4 else 4
                if runs == 6:
                    game.data['bot_sixes'] += 1
                else:
                    game.data['bot_fours'] += 1
                commentary = f"💥 The bot hits a {runs}! Punished!"
            else:
                # Singles/twos
                runs = random.choice([1, 1, 2])
                commentary = f"Bot takes {runs} run{'s' if runs > 1 else ''}"
            
            game.data['bot_score'] += runs
            game.data['bot_balls_faced'] += 1
            game.data['balls_in_over'] += 1
        
        # Check over completion
        if game.data['balls_in_over'] >= 6:
            game.data['balls_in_over'] = 0
            game.data['overs_bowled'] += 1
            PowerUp.update_powerup_durations(game)
            commentary += "\n⚪ Over complete!"
        
        game.save()
        
        # Check if innings should end
        innings_check = check_innings_end(game)
        if innings_check['innings_end']:
            if game.data.get('innings') == 1:
                handle_innings_break(call.message.chat.id, game, innings_check['reason'])
            else:
                handle_match_completion(call.message.chat.id, call.from_user.id, game)
            bot.answer_callback_query(call.id)
            return
        
        # Show update
        scorecard = generate_live_scorecard(game)
        bot.answer_callback_query(call.id, commentary[:200])
        
        bot.send_message(
            call.message.chat.id,
            f"{commentary}\n\n<pre>{scorecard}</pre>",
            parse_mode="HTML",
            reply_markup=kb_match_actions(game)
        )
        
    except Exception as e:
        logger.error(f"Error handling bowling: {e}")
        bot.answer_callback_query(call.id, "Error processing delivery")


def handle_innings_break(chat_id: int, game: GameState, reason: str):
    """Handle the break between innings"""
    try:
        first_innings_score = game.data.get('player_score' if game.data.get('batting') == 'player' else 'bot_score', 0)
        target = first_innings_score + 1
        
        game.data['innings'] = 2
        game.data['target'] = target
        game.data['overs_bowled'] = 0
        game.data['balls_in_over'] = 0
        
        # Switch batting
        game.data['batting'] = 'bot' if game.data.get('batting') == 'player' else 'player'
        
        game.save()
        
        innings_break_msg = (
            f"{'═'*50}\n"
            f"  INNINGS BREAK\n"
            f"{'═'*50}\n\n"
            f"First Innings Complete!\n"
            f"Score: {first_innings_score}\n\n"
            f"{'─'*50}\n"
            f"🎯 TARGET: {target} runs\n"
            f"{'─'*50}\n\n"
            f"Second innings about to begin...\n"
        )
        
        if game.data.get('batting') == 'player':
            innings_break_msg += "\n🏏 YOUR TURN TO BAT!\nChase down the target!"
        else:
            innings_break_msg += "\n🎳 YOUR TURN TO BOWL!\nDefend the target!"
        
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("▶️ Start 2nd Innings", callback_data="start_second_innings"))
        
        bot.send_message(chat_id, innings_break_msg, reply_markup=kb)
        
    except Exception as e:
        logger.error(f"Error handling innings break: {e}")


@bot.callback_query_handler(func=lambda call: call.data == 'start_second_innings')
@rate_limit_check('callback')
def handle_start_second_innings(call):
    """Start the second innings"""
    try:
        game = GameState(call.message.chat.id)
        
        scorecard = generate_live_scorecard(game)
        
        msg = (
            "🏏 SECOND INNINGS UNDERWAY!\n\n"
            f"<pre>{scorecard}</pre>"
        )
        
        bot.edit_message_text(
            msg,
            call.message.chat.id,
            call.message.message_id,
            parse_mode="HTML",
            reply_markup=kb_match_actions(game)
        )
        
        bot.answer_callback_query(call.id, "Second innings started!")
        
    except Exception as e:
        logger.error(f"Error starting second innings: {e}")
        bot.answer_callback_query(call.id, "Error starting innings")



@bot.callback_query_handler(func=lambda call: call.data == 'tournament_join')
def handle_tournament_join(call):
    """Show available tournaments to join"""
    show_all_tournaments(call.message.chat.id)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('tourn_start_'))
def handle_tournament_start(call):
    """Start tournament if creator"""
    try:
        tournament_id = call.data.split('_')[2]
        tournament = load_tournament_from_db(tournament_id)
        
        if tournament and tournament.created_by == call.from_user.id:
            if len(tournament.participants) < 2:
                bot.answer_callback_query(call.id, "Need at least 2 players!", show_alert=True)
                return
            
            result = tournament.start_tournament()
            if result['success']:
                save_tournament_to_db(tournament)
                bot.send_message(
                    call.message.chat.id,
                    f"🏆 Tournament Started!\n\n{result['message']}"
                )
            else:
                bot.answer_callback_query(call.id, result['message'], show_alert=True)
        else:
            bot.answer_callback_query(call.id, "Only creator can start!", show_alert=True)
    except Exception as e:
        logger.error(f"Error starting tournament: {e}")
        bot.answer_callback_query(call.id, "Error starting tournament")



@bot.callback_query_handler(func=lambda call: call.data == 'help')
def handle_help_callback(call):
    help_text = (
        "🏏 <b>HOW TO PLAY</b>\n\n"
        "1. Start a match with /play\n"
        "2. Win the toss to choose bat/bowl\n"
        "3. Send numbers 1-6 for each ball\n"
        "4. Same number = OUT! Different = RUNS!\n\n"
        "<b>Commands:</b>\n"
        "/play - Start match\n"
        "/stats - View stats\n"
        "/leaderboard - Rankings\n"
        "/daily - Daily challenges\n"
        "/achievements - View achievements"
    )
    bot.send_message(call.message.chat.id, help_text)
    bot.answer_callback_query(call.id)


@bot.message_handler(func=lambda message: True)
def handle_other_messages(message):
    try:
        logger.info(f"Received unhandled message: '{message.text}' from user {message.from_user.id}")
        bot.reply_to(message, "I didn't understand that. Try /help for available commands.")
    except Exception as e:
        logger.error(f"Error in default handler: {e}", exc_info=True)


def ensure_user_exists(user_id: int, username: str = None, first_name: str = None):
    """Ensure user exists in database"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            cur.execute(f"SELECT user_id FROM users WHERE user_id = {param_style}", (user_id,))
            
            if not cur.fetchone():
                now = datetime.now(timezone.utc).isoformat()
                
                if is_postgres:
                    cur.execute("""
                        INSERT INTO users (user_id, username, first_name, coins, created_at, last_active)
                        VALUES (%s, %s, %s, 100, %s, %s)
                    """, (user_id, username, first_name, now, now))
                else:
                    cur.execute("""
                        INSERT INTO users (user_id, username, first_name, coins, created_at, last_active)
                        VALUES (?, ?, ?, 100, ?, ?)
                    """, (user_id, username, first_name, now, now))
                
                # Create stats entry
                if is_postgres:
                    cur.execute("""
                        INSERT INTO stats (user_id, created_at, updated_at)
                        VALUES (%s, %s, %s)
                    """, (user_id, now, now))
                else:
                    cur.execute("""
                        INSERT INTO stats (user_id, created_at, updated_at)
                        VALUES (?, ?, ?)
                    """, (user_id, now, now))
                
                # Create level entry
                if is_postgres:
                    cur.execute("""
                        INSERT INTO user_levels (user_id, level, experience, next_level_xp, total_xp)
                        VALUES (%s, 1, 0, 100, 0)
                    """, (user_id,))
                else:
                    cur.execute("""
                        INSERT INTO user_levels (user_id, level, experience, next_level_xp, total_xp)
                        VALUES (?, 1, 0, 100, 0)
                    """, (user_id,))
                
                logger.info(f"Created new user: {user_id}")
    except Exception as e:
        logger.error(f"Error ensuring user exists: {e}")


def save_match_to_history(chat_id: int, user_id: int, game: GameState, result: str, margin: str):
    """Save completed match to history"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            now = datetime.now(timezone.utc).isoformat()
            
            overs_played = game.data.get('overs_bowled', 0) + (game.data.get('balls_in_over', 0) / 6.0)
            player_sr = 0
            if game.data.get('player_balls_faced', 0) > 0:
                player_sr = (game.data['player_score'] / game.data['player_balls_faced']) * 100
            
            duration = 5  # Estimate - you can track actual time if needed
            
            if is_postgres:
                cur.execute("""
                    INSERT INTO match_history (
                        chat_id, user_id, match_format, player_score, bot_score,
                        player_wickets, bot_wickets, overs_played, result, margin,
                        player_strike_rate, match_duration_minutes, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    chat_id, user_id, game.data.get('match_format', 'T2'),
                    game.data.get('player_score', 0), game.data.get('bot_score', 0),
                    game.data.get('player_wkts', 0), game.data.get('bot_wkts', 0),
                    overs_played, result, margin, player_sr, duration, now
                ))
            else:
                cur.execute("""
                    INSERT INTO match_history (
                        chat_id, user_id, match_format, player_score, bot_score,
                        player_wickets, bot_wickets, overs_played, result, margin,
                        player_strike_rate, match_duration_minutes, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    chat_id, user_id, game.data.get('match_format', 'T2'),
                    game.data.get('player_score', 0), game.data.get('bot_score', 0),
                    game.data.get('player_wkts', 0), game.data.get('bot_wkts', 0),
                    overs_played, result, margin, player_sr, duration, now
                ))
    except Exception as e:
        logger.error(f"Error saving match to history: {e}")


def update_user_stats_after_match(user_id: int, game: GameState, result: str):
    """Update user statistics after match completion"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            
            # Get current stats
            cur.execute(f"SELECT * FROM stats WHERE user_id = {param_style}", (user_id,))
            stats = cur.fetchone()
            
            if not stats:
                return
            
            # Calculate new stats
            games_played = stats['games_played'] + 1
            wins = stats['wins'] + (1 if result == 'win' else 0)
            losses = stats['losses'] + (1 if result == 'loss' else 0)
            ties = stats['ties'] + (1 if result == 'tie' else 0)
            
            player_score = game.data.get('player_score', 0)
            total_runs = stats['total_runs'] + player_score
            total_balls = stats['total_balls_faced'] + game.data.get('player_balls_faced', 0)
            
            high_score = max(stats['high_score'], player_score)
            avg_score = total_runs / games_played if games_played > 0 else 0
            strike_rate = (total_runs / total_balls * 100) if total_balls > 0 else 0
            
            sixes = stats['sixes_hit'] + game.data.get('player_sixes', 0)
            fours = stats['fours_hit'] + game.data.get('player_fours', 0)
            
            centuries = stats['centuries'] + (1 if player_score >= 100 else 0)
            fifties = stats['fifties'] + (1 if 50 <= player_score < 100 else 0)
            ducks = stats['ducks'] + (1 if player_score == 0 and game.data.get('player_wkts', 0) > 0 else 0)
            
            # Update streak
            if result == 'win':
                current_streak = stats['current_winning_streak'] + 1
                longest_streak = max(stats['longest_winning_streak'], current_streak)
            else:
                current_streak = 0
                longest_streak = stats['longest_winning_streak']
            
            now = datetime.now(timezone.utc).isoformat()
            
            # Update database
            if is_postgres:
                cur.execute("""
                    UPDATE stats SET
                        games_played = %s, wins = %s, losses = %s, ties = %s,
                        total_runs = %s, total_balls_faced = %s, high_score = %s,
                        avg_score = %s, strike_rate = %s, sixes_hit = %s, fours_hit = %s,
                        centuries = %s, fifties = %s, ducks = %s,
                        current_winning_streak = %s, longest_winning_streak = %s,
                        updated_at = %s
                    WHERE user_id = %s
                """, (
                    games_played, wins, losses, ties, total_runs, total_balls,
                    high_score, avg_score, strike_rate, sixes, fours,
                    centuries, fifties, ducks, current_streak, longest_streak,
                    now, user_id
                ))
            else:
                cur.execute("""
                    UPDATE stats SET
                        games_played = ?, wins = ?, losses = ?, ties = ?,
                        total_runs = ?, total_balls_faced = ?, high_score = ?,
                        avg_score = ?, strike_rate = ?, sixes_hit = ?, fours_hit = ?,
                        centuries = ?, fifties = ?, ducks = ?,
                        current_winning_streak = ?, longest_winning_streak = ?,
                        updated_at = ?
                    WHERE user_id = ?
                """, (
                    games_played, wins, losses, ties, total_runs, total_balls,
                    high_score, avg_score, strike_rate, sixes, fours,
                    centuries, fifties, ducks, current_streak, longest_streak,
                    now, user_id
                ))
            
            # Update leaderboards
            update_leaderboard(user_id, 'highest_score', high_score)
            update_leaderboard(user_id, 'most_wins', wins)
            update_leaderboard(user_id, 'win_streak', longest_streak)
            update_leaderboard(user_id, 'most_sixes', sixes)
            
    except Exception as e:
        logger.error(f"Error updating user stats: {e}")


def handle_match_completion(chat_id: int, user_id: int, game: GameState):
    """Handle all post-match operations"""
    try:
        # Determine result
        player_score = game.data.get('player_score', 0)
        bot_score = game.data.get('bot_score', 0)
        
        if player_score > bot_score:
            result = 'win'
            margin = f"{player_score - bot_score} runs"
            coins_reward = 50
            result_text = f"🏆 YOU WON by {margin}!"
        elif bot_score > player_score:
            result = 'loss'
            wickets_remaining = game.data.get('wickets_limit', 1) - game.data.get('bot_wkts', 0)
            margin = f"{wickets_remaining} wickets"
            coins_reward = 10
            result_text = f"💔 YOU LOST by {margin}"
        else:
            result = 'tie'
            margin = "Match Tied"
            coins_reward = 25
            result_text = "🤝 MATCH TIED!"
        
        # Award coins
        _award_coins(user_id, coins_reward)
        
        # Calculate and award XP
        xp_earned = UserLevelManager.calculate_match_xp(game.data, result)
        level_update = UserLevelManager.update_user_level(user_id, xp_earned)
        
        # Update stats
        update_user_stats_after_match(user_id, game, result)
        
        # Save to history
        save_match_to_history(chat_id, user_id, game, result, margin)
        
        # Check for achievements
        match_data = {
            'result': result,
            'player_score': player_score,
            'player_sixes': game.data.get('player_sixes', 0),
            'player_fours': game.data.get('player_fours', 0),
            'player_balls_faced': game.data.get('player_balls_faced', 0),
            'player_wkts': game.data.get('player_wkts', 0),
            'innings': game.data.get('innings', 1),
            'is_tournament_win': game.data.get('is_tournament_match', False) and result == 'win'
        }
        
        # Get winning streak for achievement check
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            param_style = "%s" if is_postgres else "?"
            cur.execute(f"SELECT current_winning_streak FROM stats WHERE user_id = {param_style}", (user_id,))
            stats_row = cur.fetchone()
            if stats_row:
                match_data['winning_streak'] = stats_row['current_winning_streak']
        
        unlocked_achievements = AchievementSystem.check_achievements(user_id, match_data)
        
        # Update challenges
        tracker = ChallengeTracker(user_id)
        if result == 'win':
            tracker.update_progress(ChallengeType.WINS, 1)
            tracker.update_progress(ChallengeType.STREAK, 1)
        else:
            tracker.update_progress(ChallengeType.STREAK, 0)  # Reset streak
        
        tracker.update_progress(ChallengeType.SCORE, player_score)
        tracker.update_progress(ChallengeType.SIXES, game.data.get('player_sixes', 0))
        tracker.update_progress(
            ChallengeType.BOUNDARIES,
            game.data.get('player_fours', 0) + game.data.get('player_sixes', 0)
        )
        
        completed_challenges = tracker.check_completion()
        
        # Build match summary
        summary = (
            f"{'═'*50}\n"
            f"  MATCH SUMMARY\n"
            f"{'═'*50}\n\n"
            f"{result_text}\n\n"
            f"🏏 YOUR SCORE: {player_score}/{game.data.get('player_wkts', 0)}\n"
            f"🤖 BOT SCORE: {bot_score}/{game.data.get('bot_wkts', 0)}\n\n"
            f"{'─'*50}\n"
            f"💰 Coins Earned: +{coins_reward}\n"
            f"📈 XP Earned: +{xp_earned}\n"
        )
        
        if level_update.get('level_up'):
            summary += f"\n🎉 LEVEL UP! You're now Level {level_update['new_level']}!\n"
            
            for reward in level_update.get('rewards', []):
                summary += f"🎁 Bonus: {reward['coins']} coins + Title: {reward['title']}\n"
        
        if unlocked_achievements:
            summary += f"\n{'🏆'*15}\n"
            for achievement_id in unlocked_achievements:
                achievement = AchievementSystem.ACHIEVEMENTS[achievement_id]
                summary += f"🏆 ACHIEVEMENT UNLOCKED: {achievement['name']}!\n"
        
        if completed_challenges:
            summary += f"\n{'📋'*15}\n"
            summary += "✅ Challenges Completed:\n"
            for challenge in completed_challenges:
                summary += f"  • {challenge['description']}\n"
        
        summary += f"\n{'═'*50}"
        
        # Send summary with post-match options
        bot.send_message(
            chat_id,
            summary,
            reply_markup=kb_post_match({'result': result})
        )
        
        # Clean up game
        game.delete()
        
    except Exception as e:
        logger.error(f"Error handling match completion: {e}")


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
    """Enhanced animation with GIF priority - REPLACE EXISTING FUNCTION"""
    return AnimationManager.send_animation(chat_id, event_type, caption)


def handle_create_tournament(chat_id: int, user_id: int):
    """Start tournament creation"""
    set_user_session_data(user_id, "creating_tournament", True)
    set_user_session_data(user_id, "tournament_step", "format")
    
    text = (
        "⚙️ CREATE TOURNAMENT\n\n"
        "Step 1: Choose Format\n\n"
        "Select match format:"
    )
    
    kb = types.InlineKeyboardMarkup(row_width=1)
    formats = [
        ("🏃 T10 (10 overs)", "fmt_10"),
        ("🏏 T20 (20 overs)", "fmt_20"),
        ("⚡ T5 (5 overs)", "fmt_5"),
    ]
    
    for text_btn, cb in formats:
        kb.add(types.InlineKeyboardButton(text_btn, callback_data=cb))
    
    kb.add(types.InlineKeyboardButton("🔙 Cancel", callback_data="tournament_menu"))
    
    bot.send_message(chat_id, text, reply_markup=kb)


def handle_tournament_type_selection(chat_id: int, user_id: int, format_key: str):
    """Ask for tournament type after format"""
    set_user_session_data(user_id, "tournament_format", format_key)
    set_user_session_data(user_id, "tournament_step", "type")
    
    text = (
        "⚙️ CREATE TOURNAMENT\n\n"
        "Step 2: Choose Type\n\n"
        "Select tournament type:"
    )
    
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("🏆 Knockout", callback_data="type_knockout"),
        types.InlineKeyboardButton("📊 League", callback_data="type_league")
    )
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="tournament_create"))
    
    bot.send_message(chat_id, text, reply_markup=kb)


def finalize_tournament_creation(chat_id: int, user_id: int, theme: str):
    """Create and save tournament"""
    try:
        format_key = get_user_session_data(user_id, "tournament_format")
        tournament_type = get_user_session_data(user_id, "tournament_type")
        
        format_map = {"fmt_5": 5, "fmt_10": 10, "fmt_20": 20}
        overs = format_map.get(format_key, 10)
        
        tournament_id = str(int(time.time() * 1000))
        tournament = EliteTournament(
            tournament_id,
            f"{theme.upper()} Tournament",
            tournament_type,
            theme,
            overs,
            10,
            user_id
        )
        
        save_tournament_to_db(tournament, chat_id)
        
        # Clear session
        set_user_session_data(user_id, "creating_tournament", None)
        set_user_session_data(user_id, "tournament_format", None)
        set_user_session_data(user_id, "tournament_type", None)
        set_user_session_data(user_id, "tournament_step", None)
        
        success_text = (
            f"✅ TOURNAMENT CREATED!\n\n"
            f"🏆 {tournament.name}\n"
            f"📋 Type: {tournament_type.upper()}\n"
            f"📊 Format: T{overs}\n"
            f"🎨 Theme: {theme.upper()}\n\n"
            f"Tournament ID: {tournament_id}\n\n"
            f"Share with friends to invite them!"
        )
        
        kb = types.InlineKeyboardMarkup(row_width=1)
        kb.add(
            types.InlineKeyboardButton("👥 View Participants", callback_data=f"tourn_view_{tournament_id}"),
            types.InlineKeyboardButton("🎮 Start Tournament", callback_data=f"tourn_start_{tournament_id}"),
            types.InlineKeyboardButton("🏆 Tournaments", callback_data="tournament_menu")
        )
        
        bot.send_message(chat_id, success_text, reply_markup=kb)
        
    except Exception as e:
        logger.error(f"Error creating tournament: {e}")
        bot.send_message(chat_id, "❌ Error creating tournament. Please try again.")


def handle_tournament_theme_selection(chat_id: int, user_id: int, tournament_type: str):
    """Ask for tournament theme"""
    set_user_session_data(user_id, "tournament_type", tournament_type)
    set_user_session_data(user_id, "tournament_step", "theme")
    
    text = (
        "⚙️ CREATE TOURNAMENT\n\n"
        "Step 3: Choose Theme\n\n"
        "Select theme:"
    )
    
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("🌍 World Cup", callback_data="theme_world_cup"),
        types.InlineKeyboardButton("🇮🇳 IPL", callback_data="theme_ipl"),
        types.InlineKeyboardButton("⚡ Champions", callback_data="theme_champions")
    )
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="tournament_type"))
    
    bot.send_message(chat_id, text, reply_markup=kb)


def check_webhook_status():
    """Check webhook status"""
    print("\n=== WEBHOOK STATUS ===")
    
    try:
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not token:
            print("✗ No token for webhook check")
            return False
            
        bot = telebot.TeleBot(token)
        info = bot.get_webhook_info()
        
        print(f"Current webhook URL: {info.url or 'None'}")
        print(f"Pending updates: {info.pending_update_count}")
        if info.last_error_message:
            print(f"Last error: {info.last_error_message}")
            print(f"Error date: {info.last_error_date}")
        
        use_webhook = int(os.getenv("USE_WEBHOOK", "0"))
        if use_webhook and not info.url:
            print("⚠ Webhook mode enabled but no webhook set")
        elif not use_webhook and info.url:
            print("⚠ Polling mode but webhook is still set")
            
        return True
        
    except Exception as e:
        print(f"✗ Webhook check failed: {e}")
        return False

def fix_common_issues():
    """Provide fixes for common issues"""
    print("\n=== COMMON FIXES ===")
    
    # Check for rate limiting
    print("1. Rate Limiting:")
    print("   - Your bot has aggressive rate limiting (10 actions per 10 seconds)")
    print("   - Consider increasing limits or removing for testing")
    
    # Check logging configuration
    print("\n2. Logging Issues:")
    print("   - You have duplicate logging configuration")
    print("   - This can cause log duplication or missing logs")
    
    # Check webhook URL format
    use_webhook = int(os.getenv("USE_WEBHOOK", "0"))
    if use_webhook:
        webhook_url = os.getenv("WEBHOOK_URL", "")
        if webhook_url and not webhook_url.endswith(f"/webhook/{os.getenv('TELEGRAM_BOT_TOKEN')}"):
            print("\n3. Webhook URL Format:")
            print("   ✗ Webhook URL should end with '/webhook/YOUR_TOKEN'")
            print(f"   Current: {webhook_url}")
            print(f"   Should be: {webhook_url}/webhook/{os.getenv('TELEGRAM_BOT_TOKEN')}")\


def handle_challenge_claim(chat_id: int, user_id: int, challenge_id: int):
    """Fixed version with proper database queries"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            # Get challenge details - FIXED QUERY
            if is_postgres:
                cur.execute("""
                    SELECT dc.*, uc.completed, uc.claimed
                    FROM daily_challenges dc
                    JOIN user_challenges uc ON dc.id = uc.challenge_id
                    WHERE dc.id = %s AND uc.user_id = %s
                """, (challenge_id, user_id))
            else:
                cur.execute("""
                    SELECT dc.*, uc.completed, uc.claimed
                    FROM daily_challenges dc
                    JOIN user_challenges uc ON dc.id = uc.challenge_id
                    WHERE dc.id = ? AND uc.user_id = ?
                """, (challenge_id, user_id))
            
            challenge = cur.fetchone()
            
            if not challenge or not challenge["completed"] or challenge["claimed"]:
                bot.send_message(chat_id, "Cannot claim this reward.")
                return
            
            # Award rewards
            _award_coins(user_id, challenge["reward_coins"])
            _award_xp(user_id, challenge["reward_xp"])
            
            # Mark as claimed
            now = datetime.now(timezone.utc).isoformat()
            if is_postgres:
                cur.execute("""
                    UPDATE user_challenges 
                    SET claimed = TRUE, updated_at = %s
                    WHERE user_id = %s AND challenge_id = %s
                """, (now, user_id, challenge_id))
            else:
                cur.execute("""
                    UPDATE user_challenges 
                    SET claimed = 1, updated_at = ?
                    WHERE user_id = ? AND challenge_id = ?
                """, (now, user_id, challenge_id))
            
            success_text = (
                f"🎁 <b>Reward Claimed!</b>\n\n"
                f"✅ {challenge['description']}\n\n"
                f"💰 +{challenge['reward_coins']} coins\n"
                f"⭐ +{challenge['reward_xp']} XP\n\n"
                f"Great job! Keep completing challenges!"
            )
            
            bot.send_message(chat_id, success_text, reply_markup=kb_challenges())
            
    except Exception as e:
        logger.error(f"Error claiming challenge reward: {e}")
        bot.send_message(chat_id, "Error claiming reward. Please try again.")

def show_user_tournament_history(chat_id: int, user_id: int):
    """Show user's tournament history - placeholder for now"""
    bot.send_message(chat_id, "📊 Tournament history feature coming soon!", reply_markup=kb_tournament_menu())

def show_tournament_rankings(chat_id: int):
    """Show tournament rankings - placeholder for now"""  
    bot.send_message(chat_id, "🥇 Tournament rankings feature coming soon!", reply_markup=kb_tournament_menu())

def show_challenge_history(chat_id: int, user_id: int):
    """Show challenge history - placeholder for now"""
    bot.send_message(chat_id, "📊 Challenge history feature coming soon!", reply_markup=kb_challenges())


@bot.message_handler(commands=['dbversion'])
def cmd_db_version(message: types.Message):
    """Check database version - Admin only"""
    try:
        if message.from_user.id not in ADMIN_IDS:
            bot.send_message(message.chat.id, "❌ Admin access required.")
            return
        
        version = get_db_version()
        bot.send_message(message.chat.id, f"🗄️ Database version: {version}")
        
    except Exception as e:
        logger.error(f"Error checking DB version: {e}")
        bot.send_message(message.chat.id, "❌ Error checking database version.")

@bot.message_handler(commands=['migrate'])
def cmd_migrate(message: types.Message):
    """Run database migrations - Admin only"""
    try:
        if message.from_user.id not in ADMIN_IDS:
            bot.send_message(message.chat.id, "❌ Admin access required.")
            return
        
        old_version = get_db_version()
        migrate_database()
        new_version = get_db_version()
        
        if old_version == new_version:
            bot.send_message(message.chat.id, f"✅ Database already up to date (version {new_version})")
        else:
            bot.send_message(message.chat.id, f"✅ Migration completed: {old_version} → {new_version}")
        
    except Exception as e:
        logger.error(f"Error running migrations: {e}")
        bot.send_message(message.chat.id, "❌ Migration failed. Check logs.")

# Flask app for webhook mode
app = Flask(__name__)

@app.route('/')
def index():
    return "<h1>Cricket Bot is alive!</h1><p>Webhook is ready for Telegram updates.</p>", 200

@app.route('/health', methods=['GET'])
def health_check():
    return 'OK', 200


@app.route('/status', methods=['GET'])
def status_check():
    """Comprehensive status check for monitoring"""
    try:
        status = {
            'bot': 'unknown',
            'database': 'unknown',
            'db_version': 0,
            'webhook': 'unknown',
            'uptime': time.time() - start_time if 'start_time' in globals() else 0,
            'version': '1.0.0'
        }
        
        # Test bot
        try:
            bot.get_me()
            status['bot'] = 'ok'
        except Exception as e:
            status['bot'] = f'error: {str(e)}'
        
        # Test database and get version
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                status['database'] = 'ok'
                status['db_version'] = get_db_version()
        except Exception as e:
            status['database'] = f'error: {str(e)}'
        
        # Check webhook if enabled
        if USE_WEBHOOK:
            try:
                webhook_info = bot.get_webhook_info()
                status['webhook'] = 'active' if webhook_info.url else 'not_set'
            except Exception as e:
                status['webhook'] = f'error: {str(e)}'
        else:
            status['webhook'] = 'polling_mode'
        
        return jsonify(status), 200
        
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return jsonify({'error': 'Status check failed'}), 500

@app.route('/webhook-info', methods=['GET'])
def get_webhook_info():
    try:
        info = bot.get_webhook_info()
        return {
            'webhook_url': info.url,
            'has_custom_certificate': info.has_custom_certificate,
            'pending_update_count': info.pending_update_count,
            'last_error_date': info.last_error_date,
            'last_error_message': info.last_error_message,
            'max_connections': info.max_connections,
            'allowed_updates': info.allowed_updates
        }
    except Exception as e:
        return {'error': str(e)}

@app.route('/webhook/' + TOKEN, methods=['POST'])
def webhook():
    """Handle incoming webhook updates with better error handling"""
    try:
        if request.headers.get('content-type') != 'application/json':
            logger.warning(f"Invalid content-type: {request.headers.get('content-type')}")
            return '', 403
        
        json_string = request.get_data().decode('utf-8')
        
        if not json_string:
            logger.warning("Empty webhook request")
            return '', 400
        
        logger.debug(f"Received webhook update: {json_string[:100]}...")
        
        update = telebot.types.Update.de_json(json_string)
        
        # Log update info
        if update.message:
            logger.info(f"Message from {update.message.from_user.id}: {update.message.text}")
        elif update.callback_query:
            logger.info(f"Callback from {update.callback_query.from_user.id}: {update.callback_query.data}")
        
        # Process update
        bot.process_new_updates([update])
        
        return '', 200
        
    except Exception as e:
        logger.error(f"Webhook error: {e}", exc_info=True)
        return '', 500

@app.route('/test-db', methods=['GET'])
def test_database():
    """Enhanced database test with better error details"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            result = cur.fetchone()
            
            # Also test if main tables exist
            if os.getenv("DATABASE_URL"):  # PostgreSQL
                cur.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='users'")
            else:  # SQLite
                cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='users'")
            
            table_exists = cur.fetchone()[0] > 0
            
            return {
                'database': 'ok', 
                'connection_test': str(result),
                'users_table_exists': table_exists,
                'db_type': 'postgresql' if os.getenv("DATABASE_URL") else 'sqlite'
            }, 200
            
    except Exception as e:
        return {'database': 'failed', 'error': str(e)}, 500


@app.route('/test-token', methods=['GET'])
def test_token():
    """Test bot token"""
    try:
        me = bot.get_me()
        return {
            'bot': 'ok',
            'username': me.username,
            'id': me.id,
            'first_name': me.first_name
        }, 200
    except Exception as e:
        return {'bot': 'failed', 'error': str(e)}, 500

@app.route('/verify-bot', methods=['GET'])
def verify_bot():
    """Verify bot is working"""
    try:
        # Test bot
        me = bot.get_me()
        
        # Test webhook
        webhook_info = bot.get_webhook_info()
        
        # Test database
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                db_status = "OK"
        except Exception as e:
            db_status = f"ERROR: {e}"
        
        return jsonify({
            'bot_username': f"@{me.username}",
            'bot_id': me.id,
            'webhook_url': webhook_info.url,
            'webhook_set': bool(webhook_info.url),
            'pending_update_count': webhook_info.pending_update_count,
            'last_error': webhook_info.last_error_message or "None",
            'database': db_status,
            'handlers_registered': {
                'message': len(bot.message_handlers),
                'callback': len(bot.callback_query_handlers)
            }
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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



@bot.message_handler(commands=['checkuser'])
def check_user_admin(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    try:
        user_id = int(message.text.split()[1])
        behavior = AntiCheatSystem.check_user_behavior(user_id)
        
        response = f"🔍 <b>User Analysis: {user_id}</b>\n\n"
        response += f"Risk Score: {behavior['risk_score']}/100\n"
        response += f"Suspicious: {'⚠️ YES' if behavior['suspicious'] else '✅ NO'}\n\n"
        
        if behavior['violations']:
            response += "<b>Violations:</b>\n"
            for v in behavior['violations']:
                response += f"• {v['type']}: {v['details']} ({v['severity']})\n"
        
        bot.send_message(message.chat.id, response)
    except:
        bot.send_message(message.chat.id, "Usage: /checkuser <user_id>")

@bot.message_handler(commands=['ban'])
def ban_user_admin(message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    try:
        parts = message.text.split()
        user_id = int(parts[1])
        duration = parts[2] if len(parts) > 2 else 'temporary'
        reason = ' '.join(parts[3:]) if len(parts) > 3 else 'Admin ban'
        
        AntiCheatSystem.ban_user(user_id, reason, duration, message.from_user.id)
        bot.send_message(message.chat.id, f"✅ User {user_id} banned ({duration})")
    except:
        bot.send_message(message.chat.id, "Usage: /ban <user_id> <temporary|extended|permanent> [reason]")


import time
start_time = time.time()


if __name__ == "__main__":
    try:
        logger.info("=== CRICKET BOT STARTING ===")
        logger.info(f"Python version: {sys.version}")
        logger.info(f"Telebot version: {telebot.__version__}")
        
        # Validate environment
        logger.info("Validating environment...")
        validate_environment()
        
        # Initialize database
        logger.info("Initializing database...")
        db_init()
        
        # Create scheduled tasks
        logger.info("Setting up scheduled tasks...")
        create_daily_challenges()
        
        # Start scheduler in background
        def run_scheduled_tasks():
            schedule.every().day.at("00:00").do(create_daily_challenges)
            schedule.every().day.at("12:00").do(create_scheduled_tournament)
            
            while True:
                try:
                    schedule.run_pending()
                    time.sleep(60)
                except Exception as e:
                    logger.error(f"Scheduler error: {e}")
        
        scheduler_thread = threading.Thread(target=run_scheduled_tasks, daemon=True)
        scheduler_thread.start()
        logger.info("✓ Scheduled tasks started")
        
        if USE_WEBHOOK:
            logger.info("=== WEBHOOK MODE ===")
            
            # Set webhook
            if setup_webhook():
                logger.info("✓ Webhook configured successfully")
                logger.info(f"Starting Flask on port {PORT}...")
                
                # Run Flask
                app.run(
                    host='0.0.0.0',
                    port=PORT,
                    debug=False,
                    use_reloader=False  # Important for production
                )
            else:
                logger.error("Webhook setup failed, falling back to polling")
                USE_WEBHOOK = 0
        
        if not USE_WEBHOOK:
            logger.info("=== POLLING MODE ===")
            logger.info("Removing any existing webhook...")
            bot.remove_webhook()
            time.sleep(1)
            
            logger.info("✓ Bot is ready to receive messages!")
            logger.info("Press Ctrl+C to stop")
            
            bot.infinity_polling(
                timeout=60,
                long_polling_timeout=60,
                skip_pending=True
            )
            
    except KeyboardInterrupt:
        logger.info("\n=== Bot stopped by user ===")
    except Exception as e:
        logger.error(f"=== FATAL ERROR ===", exc_info=True)
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        logger.info("=== Cleanup complete ===")


# === FINAL INITIALIZATION (MUST BE AT END) ===
# This runs when the module is loaded by gunicorn

# Log handler registration
logger.info(f"=== HANDLERS REGISTERED ===")
logger.info(f"Message handlers: {len(bot.message_handlers)}")
logger.info(f"Callback handlers: {len(bot.callback_query_handlers)}")

# List all registered command handlers for debugging
for handler in bot.message_handlers:
    if hasattr(handler, 'commands') and handler.commands:
        logger.info(f"  - Command handler: {handler.commands}")

try:
    if USE_WEBHOOK and WEBHOOK_URL:
        logger.info("=== FINAL WEBHOOK SETUP ===")
        
        # Small delay to ensure everything is loaded
        time.sleep(1)
        
        setup_webhook()
        logger.info("=== BOT READY TO RECEIVE UPDATES ===")
        logger.info(f"Webhook URL: {WEBHOOK_URL}/webhook/{TOKEN[:10]}...")
except Exception as e:
    logger.error(f"Final webhook setup failed: {e}")