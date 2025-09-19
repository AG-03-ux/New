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
from enum import Enum
from collections import defaultdict
import uuid


load_dotenv()


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

ACHIEVEMENTS_LIST = [
    {
        "id": 1, "name": "First Victory", "description": "Win your first match",
        "icon": "🏆", "points": 10, "requirement_type": "wins", "requirement_value": 1
    },
    {
        "id": 2, "name": "Century Maker", "description": "Score 100 runs in a match",
        "icon": "💯", "points": 50, "requirement_type": "high_score", "requirement_value": 100
    },
    {
        "id": 3, "name": "Hat-trick Hero", "description": "Take 3 wickets in consecutive balls",
        "icon": "🎩", "points": 75, "requirement_type": "hat_tricks", "requirement_value": 1
    },
    {
        "id": 4, "name": "Consistent Player", "description": "Win 5 matches in a row",
        "icon": "🔥", "points": 100, "requirement_type": "winning_streak", "requirement_value": 5
    },
    {
        "id": 5, "name": "Big Hitter", "description": "Hit 50 sixes",
        "icon": "🚀", "points": 25, "requirement_type": "sixes_hit", "requirement_value": 50
    },
    {
        "id": 6, "name": "Boundary King", "description": "Hit 100 fours",
        "icon": "⚡", "points": 30, "requirement_type": "fours_hit", "requirement_value": 100
    },
    {
        "id": 7, "name": "Marathon Player", "description": "Play 100 matches",
        "icon": "🏃", "points": 150, "requirement_type": "games_played", "requirement_value": 100
    },
    {
        "id": 8, "name": "Perfect Game", "description": "Win without losing a wicket",
        "icon": "👑", "points": 200, "requirement_type": "perfect_game", "requirement_value": 1
    },
    {
        "id": 9, "name": "Speed Demon", "description": "Score 50 runs in under 20 balls",
        "icon": "💨", "points": 60, "requirement_type": "fastest_50", "requirement_value": 20
    },
    {
        "id": 10, "name": "Double Century", "description": "Score 200 runs in a match",
        "icon": "🌟", "points": 100, "requirement_type": "high_score", "requirement_value": 200
    },
    {
        "id": 11, "name": "Veteran Player", "description": "Play for 30 days",
        "icon": "🎖️", "points": 75, "requirement_type": "days_played", "requirement_value": 30
    },
    {
        "id": 12, "name": "Tournament Winner", "description": "Win your first tournament",
        "icon": "🏅", "points": 300, "requirement_type": "tournaments_won", "requirement_value": 1
    },
    {
        "id": 13, "name": "Serial Winner", "description": "Win 50 matches",
        "icon": "👑", "points": 250, "requirement_type": "wins", "requirement_value": 50
    },
    {
        "id": 14, "name": "Challenger", "description": "Participate in 10 tournaments",
        "icon": "🎯", "points": 150, "requirement_type": "tournaments_played", "requirement_value": 10
    },
    {
        "id": 15, "name": "Lucky Ducky", "description": "Get out for a duck 10 times",
        "icon": "🦆", "points": 25, "requirement_type": "ducks", "requirement_value": 10
    }
]
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


# Add this constant after your existing TOURNAMENT_FORMATS (you already have this but make sure it's there)
TOURNAMENT_REWARDS = {
    "winner": {"coins": 1000, "trophy_points": 100, "title": "🏆 Champion"},
    "runner_up": {"coins": 500, "trophy_points": 50, "title": "🥈 Runner-up"},
    "semi_finalist": {"coins": 200, "trophy_points": 25, "title": "🥉 Semi-finalist"},
    "quarter_finalist": {"coins": 100, "trophy_points": 10, "title": "🎖️ Quarter-finalist"}
}

# Add these missing functions (place them after your tournament management functions)

def notify_tournament_round_start(tournament_id: int, round_number: int):
    """Notify participants that a new round has started"""
    with db_conn() as db:
        try:
            # Get tournament info
            tournament = db.execute("SELECT name FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
            if not tournament:
                return
            
            # Get active participants
            participants = db.execute("""
                SELECT user_id FROM tournament_participants 
                WHERE tournament_id = ? AND is_eliminated = FALSE
            """, (tournament_id,)).fetchall()
            
            tournament_name = tournament["name"]
            message = f"🏆 {tournament_name} - Round {round_number} has started! Check your matches and get ready to play."
            
            for participant in participants:
                try:
                    bot.send_message(participant["user_id"], message)
                except Exception as e:
                    logger.warning(f"Failed to notify participant {participant['user_id']}: {e}")
                    
        except Exception as e:
            logger.error(f"Error notifying tournament round start: {e}")

def update_tournament_stats(tournament_id: int):
    """Update tournament statistics for all participants"""
    with db_conn() as db:
        try:
            # Update tournaments_played for all participants
            db.execute("""
                UPDATE stats SET tournaments_played = tournaments_played + 1
                WHERE user_id IN (
                    SELECT user_id FROM tournament_participants WHERE tournament_id = ?
                )
            """, (tournament_id,))
            
            # Update tournaments_won for the winner
            tournament = db.execute("SELECT winner_id FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
            if tournament and tournament["winner_id"]:
                db.execute("""
                    UPDATE stats SET tournaments_won = tournaments_won + 1
                    WHERE user_id = ?
                """, (tournament["winner_id"],))
                
            logger.info(f"Updated tournament stats for tournament {tournament_id}")
            
        except Exception as e:
            logger.error(f"Error updating tournament stats: {e}")

def handle_tournament_match_completion(chat_id: int, g: dict, winner_id: int, loser_id: int):
    """Handle completion of a tournament match"""
    try:
        tournament_id = g.get("tournament_id")
        if not tournament_id:
            logger.warning("Tournament match completion called but no tournament_id found")
            return
        
        round_number = g.get("tournament_round", 1)
        
        with db_conn() as db:
            # Update match result in tournament_matches table
            db.execute("""
                UPDATE tournament_matches SET
                    winner_id = ?,
                    match_status = 'completed',
                    completed_at = ?
                WHERE tournament_id = ? AND round_number = ? 
                AND ((player1_id = ? AND player2_id = ?) OR (player1_id = ? AND player2_id = ?))
            """, (winner_id, datetime.now(timezone.utc).isoformat(), tournament_id, round_number,
                  winner_id, loser_id, loser_id, winner_id))
            
            # Update participant elimination status
            db.execute("""
                UPDATE tournament_participants SET
                    is_eliminated = TRUE,
                    elimination_round = ?,
                    eliminated_at = ?
                WHERE tournament_id = ? AND user_id = ?
            """, (round_number, datetime.now(timezone.utc).isoformat(), tournament_id, loser_id))
            
            # Check if round is complete and advance tournament
            check_and_advance_tournament_round(tournament_id, round_number)
            
    except Exception as e:
        logger.error(f"Error handling tournament match completion: {e}")

def check_and_advance_tournament_round(tournament_id: int, current_round: int):
    """Check if tournament round is complete and advance to next round"""
    try:
        with db_conn() as db:
            # Count remaining matches in current round
            pending_matches = db.execute("""
                SELECT COUNT(*) as count FROM tournament_matches
                WHERE tournament_id = ? AND round_number = ? AND match_status = 'pending'
            """, (tournament_id, current_round)).fetchone()
            
            if pending_matches["count"] > 0:
                logger.info(f"Tournament {tournament_id} round {current_round} still has {pending_matches['count']} pending matches")
                return  # Round not complete yet
            
            # Get winners from current round
            winners = db.execute("""
                SELECT winner_id FROM tournament_matches
                WHERE tournament_id = ? AND round_number = ? AND winner_id IS NOT NULL
            """, (tournament_id, current_round)).fetchall()
            
            winner_ids = [w["winner_id"] for w in winners if w["winner_id"]]
            
            if len(winner_ids) <= 1:
                # Tournament complete!
                logger.info(f"Tournament {tournament_id} completed with winner: {winner_ids[0] if winner_ids else 'None'}")
                complete_tournament(tournament_id, winner_ids[0] if winner_ids else None)
            else:
                # Create next round
                logger.info(f"Advancing tournament {tournament_id} to round {current_round + 1}")
                create_next_tournament_round(tournament_id, current_round + 1, winner_ids)
                
    except Exception as e:
        logger.error(f"Error checking tournament round advancement: {e}")

def complete_tournament(tournament_id: int, winner_id: int):
    """Complete tournament and distribute prizes"""
    try:
        with db_conn() as db:
            tournament = db.execute("SELECT * FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
            if not tournament:
                return
            
            # Update tournament status
            db.execute("""
                UPDATE tournaments SET
                    status = 'completed',
                    winner_id = ?,
                    end_date = ?
                WHERE id = ?
            """, (winner_id, datetime.now(timezone.utc).isoformat(), tournament_id))
            
            # Distribute prizes and titles
            if tournament["prize_pool"] > 0:
                distribute_tournament_rewards(tournament_id, tournament["prize_pool"])
            
            # Update participant stats
            update_tournament_stats(tournament_id)
            
            # Send completion notifications
            notify_tournament_completion(tournament_id)
            
            logger.info(f"Tournament {tournament_id} completed successfully")
            
    except Exception as e:
        logger.error(f"Error completing tournament: {e}")

def distribute_tournament_rewards(tournament_id: int, prize_pool: int):
    """Distribute rewards to tournament participants"""
    try:
        with db_conn() as db:
            # Get final rankings based on elimination rounds (later elimination = higher rank)
            participants = db.execute("""
                SELECT tp.user_id, tp.display_name, tp.elimination_round,
                       CASE WHEN t.winner_id = tp.user_id THEN 0 ELSE tp.elimination_round END as sort_order
                FROM tournament_participants tp
                JOIN tournaments t ON tp.tournament_id = t.id
                WHERE tp.tournament_id = ?
                ORDER BY sort_order DESC, tp.total_runs DESC
            """, (tournament_id,)).fetchall()
            
            if not participants:
                return
            
            # Distribute prizes to top 4 finishers
            total_prize = int(prize_pool)
            
            for i, participant in enumerate(participants[:4]):
                position = i + 1
                
                # Calculate prize and title based on position
                if position == 1:  # Winner
                    prize = int(total_prize * 0.5)  # 50% to winner
                    title = TOURNAMENT_REWARDS["winner"]["title"]
                    trophy_points = TOURNAMENT_REWARDS["winner"]["trophy_points"]
                elif position == 2:  # Runner-up
                    prize = int(total_prize * 0.3)  # 30% to runner-up
                    title = TOURNAMENT_REWARDS["runner_up"]["title"]
                    trophy_points = TOURNAMENT_REWARDS["runner_up"]["trophy_points"]
                elif position == 3:  # Third place
                    prize = int(total_prize * 0.15)  # 15% to third
                    title = TOURNAMENT_REWARDS["semi_finalist"]["title"]
                    trophy_points = TOURNAMENT_REWARDS["semi_finalist"]["trophy_points"]
                else:  # Fourth place
                    prize = int(total_prize * 0.05)  # 5% to fourth
                    title = TOURNAMENT_REWARDS["quarter_finalist"]["title"]
                    trophy_points = TOURNAMENT_REWARDS["quarter_finalist"]["trophy_points"]
                
                # Give coins to user
                db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", 
                          (prize, participant["user_id"]))
                
                # Record ranking
                db.execute("""
                    INSERT OR REPLACE INTO tournament_rankings (
                        tournament_id, user_id, final_position, 
                        tournament_points, prize_won, title_earned
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (tournament_id, participant["user_id"], position, 
                      trophy_points, prize, title))
                
                logger.info(f"Awarded {prize} coins to user {participant['user_id']} for position {position}")
                
    except Exception as e:
        logger.error(f"Error distributing tournament rewards: {e}")

def notify_tournament_completion(tournament_id: int):
    """Send tournament completion notifications to all participants"""
    try:
        with db_conn() as db:
            tournament = db.execute("SELECT name FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
            if not tournament:
                return
                
            # Get top 3 finishers
            rankings = db.execute("""
                SELECT tr.final_position, tr.title_earned, tp.display_name
                FROM tournament_rankings tr
                JOIN tournament_participants tp ON tr.tournament_id = tp.tournament_id AND tr.user_id = tp.user_id
                WHERE tr.tournament_id = ?
                ORDER BY tr.final_position
                LIMIT 3
            """, (tournament_id,)).fetchall()
            
            completion_text = f"🏆 {tournament['name']} - COMPLETED!\n\n"
            
            if rankings:
                for rank in rankings:
                    position_emoji = "🥇" if rank["final_position"] == 1 else "🥈" if rank["final_position"] == 2 else "🥉"
                    completion_text += f"{position_emoji} {rank['display_name']} - {rank['title_earned']}\n"
            
            completion_text += f"\nCongratulations to all participants!"
            
            # Send to all participants
            participants = db.execute("""
                SELECT user_id FROM tournament_participants WHERE tournament_id = ?
            """, (tournament_id,)).fetchall()
            
            for participant in participants:
                try:
                    bot.send_message(participant["user_id"], completion_text)
                except Exception as e:
                    logger.warning(f"Could not notify user {participant['user_id']}: {e}")
                    
    except Exception as e:
        logger.error(f"Error notifying tournament completion: {e}")

def complete_tournament_match(chat_id: int, g: dict):
    """Complete tournament match and handle results"""
    try:
        tournament_id = g.get("tournament_id")
        if not tournament_id:
            # Not a tournament match, handle as regular match
            complete_match(chat_id, g)
            return
        
        round_number = g.get("tournament_round", 1)
        
        # Determine winner
        if g["player_score"] > g["bot_score"]:
            winner_id = g.get("player_id")  # You'll need to ensure this is set when starting tournament matches
            result = "win"
            winner_name = "You"
        elif g["bot_score"] > g["player_score"]:
            winner_id = g.get("opponent_id", 0)  # Bot opponent
            result = "loss"  
            winner_name = "Bot"
        else:
            result = "tie"
            winner_name = "Tie"
            winner_id = None
        
        # Show match result
        margin = abs(g["player_score"] - g["bot_score"])
        margin_text = f"by {margin} runs" if result != "tie" else "Match Tied"
        
        tournament_match_summary = (
            f"🏆 TOURNAMENT MATCH COMPLETE\n\n"
            f"🏏 Your Score: {g['player_score']}/{g['player_wkts']}\n"
            f"🤖 Bot Score: {g['bot_score']}/{g['bot_wkts']}\n\n"
            f"🎯 Result: {winner_name} wins {margin_text}\n"
            f"Tournament: Round {round_number}\n\n"
            f"⏳ Waiting for other matches to complete..."
        )
        
        bot.send_message(chat_id, tournament_match_summary)
        
        # Handle tournament progression
        if winner_id:
            # For bot matches, we need to simulate the opponent
            opponent_id = g.get("opponent_id", 999999)  # Use a placeholder bot ID
            loser_id = opponent_id if result == "win" else g.get("player_id")
            if winner_id and loser_id:
                handle_tournament_match_completion(chat_id, g, winner_id, loser_id)
        
        # Clean up game state
        delete_game(chat_id)
        
    except Exception as e:
        logger.error(f"Error completing tournament match: {e}")
        # Fallback to regular match completion
        complete_match(chat_id, g)

# Add this function to safely check if tournament tables exist
def tournament_tables_exist():
    """Check if tournament tables exist in database"""
    try:
        with db_conn() as db:
            cursor = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tournaments'")
            result = cursor.fetchone()
            return result is not None
    except:
        return False

# Update your init_tournament_db to be safer
def init_tournament_db():
    """Initialize tournament-related database tables"""
    with db_conn() as db:
        try:
            # Check if enhanced tournament table already exists
            cursor = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tournaments'")
            existing_table = cursor.fetchone()
            
            if existing_table:
                # Table exists, add missing columns safely
                try:
                    db.execute("ALTER TABLE tournaments ADD COLUMN tournament_type TEXT DEFAULT 'knockout'")
                except sqlite3.OperationalError:
                    pass
                try:
                    db.execute("ALTER TABLE tournaments ADD COLUMN current_participants INTEGER DEFAULT 0")
                except sqlite3.OperationalError:
                    pass
                try:
                    db.execute("ALTER TABLE tournaments ADD COLUMN difficulty_level TEXT DEFAULT 'medium'")
                except sqlite3.OperationalError:
                    pass
                try:
                    db.execute("ALTER TABLE tournaments ADD COLUMN winner_id INTEGER")
                except sqlite3.OperationalError:
                    pass
                try:
                    db.execute("ALTER TABLE tournaments ADD COLUMN current_round INTEGER DEFAULT 1")
                except sqlite3.OperationalError:
                    pass
                try:
                    db.execute("ALTER TABLE tournaments ADD COLUMN total_rounds INTEGER")
                except sqlite3.OperationalError:
                    pass
            
            # Create additional tournament tables
            db.execute("""
                CREATE TABLE IF NOT EXISTS tournament_matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tournament_id INTEGER,
                    round_number INTEGER,
                    match_number INTEGER,
                    player1_id INTEGER,
                    player2_id INTEGER,
                    player1_name TEXT,
                    player2_name TEXT,
                    winner_id INTEGER,
                    player1_score INTEGER DEFAULT 0,
                    player2_score INTEGER DEFAULT 0,
                    player1_wickets INTEGER DEFAULT 0,
                    player2_wickets INTEGER DEFAULT 0,
                    match_status TEXT DEFAULT 'pending',
                    match_data TEXT,
                    scheduled_at TEXT,
                    completed_at TEXT,
                    created_at TEXT
                )
            """)
            
            db.execute("""
                CREATE TABLE IF NOT EXISTS tournament_rankings (
                    tournament_id INTEGER,
                    user_id INTEGER,
                    final_position INTEGER,
                    rounds_survived INTEGER,
                    total_runs INTEGER,
                    total_wickets INTEGER,
                    tournament_points INTEGER,
                    prize_won INTEGER,
                    title_earned TEXT,
                    PRIMARY KEY (tournament_id, user_id)
                )
            """)
            
            # Add coins column to users table safely
            try:
                db.execute("ALTER TABLE users ADD COLUMN coins INTEGER DEFAULT 100")
            except sqlite3.OperationalError:
                pass  # Column already exists
                
            # Add tournament stats columns safely
            try:
                db.execute("ALTER TABLE stats ADD COLUMN tournaments_played INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            try:
                db.execute("ALTER TABLE stats ADD COLUMN tournaments_won INTEGER DEFAULT 0") 
            except sqlite3.OperationalError:
                pass
            try:
                db.execute("ALTER TABLE stats ADD COLUMN tournament_points INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
                
        except Exception as e:
            logger.error(f"Error initializing tournament database: {e}")

            
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
        pass
    
    # Call tournament initialization AFTER basic tables are created
    try:
        init_tournament_db()
    except Exception as e:
        logger.error(f"Tournament DB init failed: {e}")
        

        db.execute("""
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                event TEXT,
                meta TEXT,
                created_at TEXT
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

def safe_save_game(chat_id: int, g: Dict[str, Any]):
    """Safely save game data with fallback for missing columns"""
    with db_conn() as db:
        now = datetime.now(timezone.utc).isoformat()
        g["updated_at"] = now
        
        # Get existing columns in games table
        cursor = db.cursor()
        cursor.execute("PRAGMA table_info(games)")
        existing_columns = [row[1] for row in cursor.fetchall()]
        
        # Core required columns that should always exist
        core_data = {
            'chat_id': chat_id,
            'state': g.get("state"),
            'innings': g.get("innings"),
            'batting': g.get("batting"),
            'player_score': g.get("player_score", 0),
            'bot_score': g.get("bot_score", 0),
            'player_wkts': g.get("player_wkts", 0),
            'bot_wkts': g.get("bot_wkts", 0),
            'balls_in_over': g.get("balls_in_over", 0),
            'overs_bowled': g.get("overs_bowled", 0),
            'target': g.get("target"),
            'overs_limit': g.get("overs_limit", 2),
            'wickets_limit': g.get("wickets_limit", 1)
        }
        
        # Additional columns that might not exist in old schema
        extended_data = {
            'match_format': g.get("match_format", "T2"),
            'difficulty_level': g.get("difficulty_level", "medium"),
            'player_balls_faced': g.get("player_balls_faced", 0),
            'bot_balls_faced': g.get("bot_balls_faced", 0),
            'player_fours': g.get("player_fours", 0),
            'player_sixes': g.get("player_sixes", 0),
            'bot_fours': g.get("bot_fours", 0),
            'bot_sixes': g.get("bot_sixes", 0),
            'extras': g.get("extras", 0),
            'powerplay_overs': g.get("powerplay_overs", 0),
            'is_powerplay': g.get("is_powerplay", False),
            'weather_condition': g.get("weather_condition", "clear"),
            'pitch_condition': g.get("pitch_condition", "normal"),
            'created_at': g.get("created_at", now),
            'updated_at': now
        }
        
        # Filter data based on existing columns
        final_data = core_data.copy()
        for key, value in extended_data.items():
            if key in existing_columns:
                final_data[key] = value
        
        # Build dynamic SQL
        columns = list(final_data.keys())
        placeholders = ['?' for _ in columns]
        values = list(final_data.values())
        
        # Create SET clause for UPDATE
        set_clause = ', '.join([f"{col}=excluded.{col}" for col in columns if col != 'chat_id'])
        
        sql = f"""
            INSERT INTO games ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT(chat_id) DO UPDATE SET {set_clause}
        """
        
        try:
            db.execute(sql, values)
            logger.info(f"Game saved for chat {chat_id} with {len(columns)} columns")
        except Exception as e:
            logger.error(f"Failed to save game: {e}")
            # Fallback to absolute minimum
            try:
                db.execute("""
                    INSERT INTO games (chat_id, state, player_score, bot_score, player_wkts, bot_wkts)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(chat_id) DO UPDATE SET 
                        state=excluded.state,
                        player_score=excluded.player_score,
                        bot_score=excluded.bot_score,
                        player_wkts=excluded.player_wkts,
                        bot_wkts=excluded.bot_wkts
                """, (chat_id, g.get("state"), g.get("player_score", 0), 
                      g.get("bot_score", 0), g.get("player_wkts", 0), g.get("bot_wkts", 0)))
                logger.warning("Used minimal game save as fallback")
            except Exception as e2:
                logger.error(f"Even fallback game save failed: {e2}")
                raise

def safe_load_game(chat_id: int) -> Optional[Dict[str, Any]]:
    """Safely load game with fallback for missing columns"""
    with db_conn() as db:
        try:
            cur = db.execute("SELECT * FROM games WHERE chat_id=?", (chat_id,))
            row = cur.fetchone()
            if not row:
                return None
            
            # Convert to dict and add defaults for missing fields
            game = dict(row)
            
            # Ensure all required fields exist with defaults
            defaults = {
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
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            for key, default_value in defaults.items():
                if key not in game or game[key] is None:
                    game[key] = default_value
            
            return game
            
        except Exception as e:
            logger.error(f"Failed to load game: {e}")
            return None 

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

def safe_start_new_game(chat_id: int, overs: int = DEFAULT_OVERS, wickets: int = DEFAULT_WICKETS, 
                       difficulty: str = "medium", user_id: int = None):
    """Safely start new game with schema compatibility"""
    g = default_game(overs, wickets, difficulty)
    safe_save_game(chat_id, g)
    
    # Get weather and pitch info safely
    weather = WEATHER_CONDITIONS.get(g.get("weather_condition", "clear"), {"description": "Clear skies"})
    pitch = PITCH_CONDITIONS.get(g.get("pitch_condition", "normal"), {"description": "Normal pitch"})
    
    match_info = (
        f"🏏 <b>New Match Started!</b>\n\n"
        f"📋 <b>Match Details:</b>\n"
        f"Format: <b>{g.get('match_format', 'T2')} ({g.get('overs_limit', 2)} over{'s' if g.get('overs_limit', 2) > 1 else ''}, "
        f"{g.get('wickets_limit', 1)} wicket{'s' if g.get('wickets_limit', 1) > 1 else ''})</b>\n"
        f"Difficulty: <b>{difficulty.title()}</b>\n"
        f"Powerplay: <b>{g.get('powerplay_overs', 0)} over{'s' if g.get('powerplay_overs', 0) > 1 else ''}</b>\n\n"
        f"🌤️ <b>Conditions:</b>\n"
        f"Weather: {weather['description']}\n"
        f"Pitch: {pitch['description']}\n\n"
        f"🪙 <b>Time for the toss!</b> Call it:"
    )
    
    bot.send_message(chat_id, match_info, reply_markup=kb_toss_choice())
    log_event(chat_id, "match_start", f"format={g.get('match_format', 'T2')} difficulty={difficulty} user={user_id}")



def safe_set_batting_order(chat_id: int, first_batting: str):
    """Safely set batting order and start match"""
    try:
        g = safe_load_game(chat_id)
        if not g:
            logger.error(f"No game found for chat {chat_id}")
            return
        
        g["state"] = "play"
        g["batting"] = first_batting
        g["is_powerplay"] = True
        safe_save_game(chat_id, g)
        
        powerplay_overs = g.get('powerplay_overs', 0)
        powerplay_text = f"⚡ <b>Powerplay active</b> (first {powerplay_overs} overs)" if powerplay_overs > 0 else ""
        
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
        
    except Exception as e:
        logger.error(f"Error setting batting order: {e}")
        bot.send_message(chat_id, "❌ Error starting match. Please try /play again.")

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

def enhanced_process_ball(chat_id: int, user_value: int):
    """Enhanced ball processing with tournament support"""
    g = safe_load_game(chat_id)
    if not g or g["state"] != "play":
        return
    
    # Check if this is a tournament match
    is_tournament_match = g.get("tournament_id") is not None
    
    # Your existing process_ball logic here...
    # (keeping the same logic but adding tournament handling at the end)
    
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
    
    # Generate commentary with tournament context
    commentary = get_commentary(g, user_value, bot_value, runs_scored, is_wicket)
    if is_tournament_match:
        commentary += f" [Tournament Match - Round {g.get('tournament_round', 1)}]"
    
    # Send appropriate animation
    if is_wicket:
        send_cricket_animation(chat_id, "wicket", commentary)
    elif runs_scored == 6:
        send_cricket_animation(chat_id, "six", commentary)
    elif runs_scored == 4:
        send_cricket_animation(chat_id, "four", commentary)
    else:
        bot.send_message(chat_id, commentary)
    
    # Tournament spectator notifications
    if is_tournament_match:
        notify_tournament_spectators(g.get("tournament_id"), chat_id, commentary)
    
    # Check for over completion and other game events
    over_completed = check_over_completion(g)
    powerplay_ended = check_powerplay_status(g)
    
    if over_completed:
        over_summary = f"🏁 <b>Over {g['overs_bowled']} completed</b>"
        if powerplay_ended:
            over_summary += "\n⚡ <b>Powerplay ended</b>"
        bot.send_message(chat_id, over_summary)
    
    # Check milestones
    check_milestones(chat_id, g)
    
    # Save ball-by-ball data
    save_ball_data(chat_id, g, user_value, bot_value, runs_scored, is_wicket, commentary)
    
    # Check if innings/match should end
    if check_innings_end(g):
        safe_save_game(chat_id, g)
        if is_tournament_match:
            end_tournament_match(chat_id, g)
        else:
            end_innings_or_match(chat_id)
        return
    
    # Save game state and show current status
    safe_save_game(chat_id, g)
    show_live_score(chat_id, g, detailed=False)

def end_tournament_match(chat_id: int, g: dict):
    """Handle end of tournament match"""
    if g["innings"] == 1:
        # End of first innings in tournament match
        start_second_innings(chat_id, g)
    else:
        # Tournament match complete
        complete_tournament_match(chat_id, g)



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
    g = safe_load_game(chat_id)
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
    
    safe_save_game(chat_id, g)
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
    existing_game = safe_load_game(message.chat.id)
    if existing_game and existing_game["state"] in ["toss", "play"]:
        bot.send_message(message.chat.id, 
                        "⚠️ You have an active match! Use /forfeit to abandon it, or continue playing.",
                        reply_markup=kb_match_actions())
        return
    
    safe_start_new_game(message.chat.id, 2, 1, "medium", message.from_user.id)

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
    g = safe_load_game(message.chat.id)
    if not g or g["state"] != "play":
        bot.send_message(message.chat.id, "❌ No active match found. Start one with /play")
        return
    show_live_score(message.chat.id, g, detailed=True)

@bot.message_handler(commands=["forfeit"])
def cmd_forfeit(message: types.Message):
    ensure_user(message)
    g = safe_load_game(message.chat.id)
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

def end_tournament_match(chat_id: int, g: dict):
    """Handle end of tournament match"""
    if g["innings"] == 1:
        # End of first innings in tournament match
        start_second_innings(chat_id, g)
    else:
        # Tournament match complete
        complete_tournament_match(chat_id, g)

def complete_tournament_match(chat_id: int, g: dict):
    """Complete tournament match and handle results"""
    tournament_id = g.get("tournament_id")
    round_number = g.get("tournament_round", 1)
    
    # Determine winner
    if g["player_score"] > g["bot_score"]:
        winner_id = g.get("player_id")  # You'll need to store this
        result = "win"
        winner_name = "You"
    elif g["bot_score"] > g["player_score"]:
        winner_id = g.get("opponent_id")  # For bot, this would be None
        result = "loss"  
        winner_name = "Bot"
    else:
        result = "tie"
        winner_name = "Tie"
    
    # Show match result
    margin = abs(g["player_score"] - g["bot_score"])
    margin_text = f"by {margin} runs" if result != "tie" else "Match Tied"
    
    tournament_match_summary = (
        f"🏆 <b>TOURNAMENT MATCH COMPLETE</b>\n\n"
        f"🏏 Your Score: {g['player_score']}/{g['player_wkts']}\n"
        f"🤖 Bot Score: {g['bot_score']}/{g['bot_wkts']}\n\n"
        f"🎯 <b>Result: {winner_name} wins {margin_text}</b>\n"
        f"Tournament: Round {round_number}\n\n"
        f"⏳ Waiting for other matches to complete..."
    )
    
    bot.send_message(chat_id, tournament_match_summary)
    
    # Handle tournament progression
    if tournament_id and winner_id:
        handle_tournament_match_completion(chat_id, g, winner_id, g.get("opponent_id"))
    
    # Clean up game state
    delete_game(chat_id)

def notify_tournament_spectators(tournament_id: int, match_chat_id: int, event: str):
    """Notify tournament spectators of match events"""
    # This would send updates to users who are spectating the tournament
    # Implementation would depend on how you track spectators
    pass

# Enhanced Tournament Dashboard
def show_tournament_dashboard(chat_id: int):
    """Show comprehensive tournament dashboard"""
    with db_conn() as db:
        # Get tournament statistics
        stats = {
            "total_tournaments": db.execute("SELECT COUNT(*) as count FROM tournaments").fetchone()["count"],
            "active_tournaments": db.execute("SELECT COUNT(*) as count FROM tournaments WHERE status IN ('registration', 'ongoing')").fetchone()["count"],
            "total_participants": db.execute("SELECT COUNT(DISTINCT user_id) as count FROM tournament_participants").fetchone()["count"],
            "total_prize_distributed": db.execute("SELECT SUM(prize_won) as total FROM tournament_rankings WHERE prize_won > 0").fetchone()["total"] or 0
        }
        
        # Upcoming tournaments
        upcoming = db.execute("""
            SELECT t.*, COUNT(tp.user_id) as participants
            FROM tournaments t
            LEFT JOIN tournament_participants tp ON t.id = tp.tournament_id
            WHERE t.status = 'registration'
            GROUP BY t.id
            ORDER BY t.registration_deadline ASC
            LIMIT 3
        """).fetchall()
        
        dashboard_text = (
            f"🏆 <b>Tournament Central Dashboard</b>\n\n"
            f"📊 <b>Overall Statistics:</b>\n"
            f"• Total Tournaments: {stats['total_tournaments']}\n"
            f"• Active Tournaments: {stats['active_tournaments']}\n"
            f"• Total Participants: {stats['total_participants']}\n"
            f"• Prizes Distributed: {stats['total_prize_distributed']} coins\n\n"
        )
        
        if upcoming:
            dashboard_text += "🔥 <b>Hot Tournaments:</b>\n"
            for tournament in upcoming:
                format_info = TOURNAMENT_FORMATS.get(tournament["format"], TOURNAMENT_FORMATS["quick"])
                spots_left = tournament["max_participants"] - tournament["participants"]
                
                dashboard_text += (
                    f"• {tournament['name']}\n"
                    f"  {format_info['name']} | {spots_left} spots left\n"
                    f"  Entry: {tournament['entry_fee']} coins\n\n"
                )
        else:
            dashboard_text += "📝 <b>No active tournaments - be the first to create one!</b>\n\n"
        
        dashboard_text += "🎯 <b>Quick Actions:</b>"
        
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("⚡ Quick Join", callback_data="tournament_quick_join"),
            types.InlineKeyboardButton("🏆 Create Tournament", callback_data="tournament_create")
        )
        kb.add(
            types.InlineKeyboardButton("📊 My Stats", callback_data="tournament_my_stats"),
            types.InlineKeyboardButton("🥇 Hall of Fame", callback_data="tournament_hall_of_fame")
        )
        kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
        
        bot.send_message(chat_id, dashboard_text, reply_markup=kb)

def show_tournament_hall_of_fame(chat_id: int):
    """Display tournament hall of fame"""
    with db_conn() as db:
        # All-time tournament winners
        champions = db.execute("""
            SELECT u.first_name, u.username, t.name as tournament_name, 
                   tr.prize_won, t.created_at
            FROM tournament_rankings tr
            JOIN users u ON tr.user_id = u.user_id
            JOIN tournaments t ON tr.tournament_id = t.id
            WHERE tr.final_position = 1
            ORDER BY t.created_at DESC
            LIMIT 10
        """).fetchall()
        
        # Most successful players
        legends = db.execute("""
            SELECT u.first_name, u.username,
                   COUNT(tr.tournament_id) as tournaments_won,
                   SUM(tr.prize_won) as total_winnings,
                   MAX(tr.prize_won) as biggest_win
            FROM tournament_rankings tr
            JOIN users u ON tr.user_id = u.user_id
            WHERE tr.final_position = 1
            GROUP BY tr.user_id
            ORDER BY tournaments_won DESC, total_winnings DESC
            LIMIT 5
        """).fetchall()
        
        hall_of_fame_text = "🏛️ <b>Tournament Hall of Fame</b>\n\n"
        
        if legends:
            hall_of_fame_text += "👑 <b>Legends:</b>\n"
            for i, legend in enumerate(legends, 1):
                name = legend["first_name"] or f"@{legend['username']}" if legend["username"] else "Anonymous"
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                hall_of_fame_text += (
                    f"{medal} {name}\n"
                    f"   🏆 {legend['tournaments_won']} championships\n"
                    f"   💰 {legend['total_winnings']} coins won\n\n"
                )
        
        if champions:
            hall_of_fame_text += "🏆 <b>Recent Champions:</b>\n"
            for champion in champions[:5]:
                name = champion["first_name"] or f"@{champion['username']}" if champion["username"] else "Anonymous"
                date = datetime.fromisoformat(champion["created_at"]).strftime("%b %d")
                hall_of_fame_text += f"• {name} - {champion['tournament_name']} ({date})\n"
        
        if not champions and not legends:
            hall_of_fame_text += "🎯 No champions yet - be the first to make history!"
        
        bot.send_message(chat_id, hall_of_fame_text)

def quick_join_tournament(chat_id: int, user_id: int):
    """Quick join the most suitable tournament"""
    with db_conn() as db:
        # Find best tournament for user
        suitable_tournaments = db.execute("""
            SELECT t.*, (t.max_participants - t.current_participants) as spots_left
            FROM tournaments t
            WHERE t.status = 'registration' 
            AND t.current_participants < t.max_participants
            AND t.id NOT IN (
                SELECT tournament_id FROM tournament_participants WHERE user_id = ?
            )
            ORDER BY spots_left ASC, t.entry_fee ASC
            LIMIT 1
        """, (user_id,)).fetchone()
        
        if not suitable_tournaments:
            bot.send_message(chat_id, 
                           "❌ No suitable tournaments available.\n"
                           "Try creating your own tournament!")
            return
        
        tournament = suitable_tournaments
        format_info = TOURNAMENT_FORMATS.get(tournament["format"], TOURNAMENT_FORMATS["quick"])
        
        quick_join_text = (
            f"⚡ <b>Quick Join Recommendation</b>\n\n"
            f"🏆 {tournament['name']}\n"
            f"🎮 Format: {format_info['name']}\n"
            f"💰 Entry Fee: {tournament['entry_fee']} coins\n"
            f"👥 Players: {tournament['current_participants']}/{tournament['max_participants']}\n\n"
            f"Join this tournament?"
        )
        
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton(
            f"🎯 Join Now ({tournament['entry_fee']} coins)",
            callback_data=f"join_tournament_{tournament['id']}"
        ))
        kb.add(types.InlineKeyboardButton("🔍 Browse All", callback_data="tournament_join"))
        
        bot.send_message(chat_id, quick_join_text, reply_markup=kb)

# Tournament Chat and Social Features
def create_tournament_chat_group(tournament_id: int):
    """Create a group chat for tournament participants"""
    # This would integrate with Telegram's group creation API
    # For now, we'll simulate with a broadcast system
    pass

def broadcast_tournament_update(tournament_id: int, message: str):
    """Broadcast update to all tournament participants"""
    with db_conn() as db:
        participants = db.execute("""
            SELECT user_id FROM tournament_participants WHERE tournament_id = ?
        """, (tournament_id,)).fetchall()
        
        for participant in participants:
            try:
                bot.send_message(participant["user_id"], f"📢 Tournament Update: {message}")
            except Exception as e:
                logger.warning(f"Failed to notify participant {participant['user_id']}: {e}")

# Tournament Betting/Prediction System (Virtual)
def create_tournament_predictions(tournament_id: int):
    """Allow users to predict tournament outcomes"""
    with db_conn() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS tournament_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id INTEGER,
                predictor_id INTEGER,
                predicted_winner_id INTEGER,
                predicted_runner_up_id INTEGER,
                prediction_points INTEGER DEFAULT 0,
                reward_coins INTEGER DEFAULT 0,
                created_at TEXT,
                FOREIGN KEY (tournament_id) REFERENCES tournaments (id),
                FOREIGN KEY (predictor_id) REFERENCES users (user_id),
                FOREIGN KEY (predicted_winner_id) REFERENCES users (user_id),
                FOREIGN KEY (predicted_runner_up_id) REFERENCES users (user_id)
            )
        """)

# Advanced Tournament Analytics
def generate_player_tournament_profile(user_id: int):
    """Generate detailed tournament profile for a player"""
    with db_conn() as db:
        profile = {}
        
        # Basic tournament stats
        basic_stats = db.execute("""
            SELECT 
                COUNT(tp.tournament_id) as tournaments_played,
                COUNT(CASE WHEN tr.final_position = 1 THEN 1 END) as wins,
                COUNT(CASE WHEN tr.final_position <= 2 THEN 1 END) as finals,
                COUNT(CASE WHEN tr.final_position <= 4 THEN 1 END) as top_4,
                AVG(CAST(tr.final_position AS REAL)) as avg_position,
                SUM(tr.prize_won) as total_winnings
            FROM tournament_participants tp
            LEFT JOIN tournament_rankings tr ON tp.tournament_id = tr.tournament_id AND tp.user_id = tr.user_id
            WHERE tp.user_id = ?
        """, (user_id,)).fetchone()
        
        profile["basic_stats"] = dict(basic_stats) if basic_stats else {}
        
        # Format preferences
        format_stats = db.execute("""
            SELECT t.format, 
                   COUNT(*) as played,
                   COUNT(CASE WHEN tr.final_position = 1 THEN 1 END) as won
            FROM tournament_participants tp
            JOIN tournaments t ON tp.tournament_id = t.id
            LEFT JOIN tournament_rankings tr ON tp.tournament_id = tr.tournament_id AND tp.user_id = tr.user_id
            WHERE tp.user_id = ?
            GROUP BY t.format
        """, (user_id,)).fetchall()
        
        profile["format_stats"] = [dict(row) for row in format_stats]
        
        return profile

# Tournament Replay System
def save_tournament_replay(tournament_id: int):
    """Save tournament replay data for later viewing"""
    with db_conn() as db:
        # Get all matches and create replay data
        matches = db.execute("""
            SELECT * FROM tournament_matches 
            WHERE tournament_id = ? 
            ORDER BY round_number, match_number
        """, (tournament_id,)).fetchall()
        
        replay_data = {
            "tournament_id": tournament_id,
            "matches": [dict(match) for match in matches],
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Store replay data (could be in a separate table or file system)
        db.execute("""
            INSERT INTO tournament_replays (tournament_id, replay_data, created_at)
            VALUES (?, ?, ?)
        """, (tournament_id, json.dumps(replay_data), replay_data["created_at"]))

# Complete the callback handler integration
def complete_tournament_callback_handler():
    """Additional tournament callback handlers"""
    # Add these to your main callback handler function
    
    tournament_callbacks = {
        "tournament_quick_join": lambda call: quick_join_tournament(call.message.chat.id, call.from_user.id),
        "tournament_my_stats": lambda call: show_my_tournaments(call.message.chat.id, call.from_user.id),
        "tournament_hall_of_fame": lambda call: show_tournament_hall_of_fame(call.message.chat.id),
        "tournament_dashboard": lambda call: show_tournament_dashboard(call.message.chat.id),
    }
    
    return tournament_callbacks

# Final Integration Updates
def update_main_process_ball():
    """Replace your existing process_ball with enhanced_process_ball"""
    # In your main code, replace:
    # process_ball(message.chat.id, number)
    # with:
    # enhanced_process_ball(message.chat.id, number)
    pass

def update_callback_handler_with_tournaments():
    """Integration instructions for callback handler"""
    # Add this to your existing handle_callbacks function:
    
    additional_callbacks = """
    # Add these cases to your existing elif chain in handle_callbacks:
    
    elif data == "tournament_quick_join":
        bot.answer_callback_query(call.id)
        quick_join_tournament(chat_id, user_id)
    
    elif data == "tournament_my_stats":
        bot.answer_callback_query(call.id) 
        show_my_tournaments(chat_id, user_id)
    
    elif data == "tournament_hall_of_fame":
        bot.answer_callback_query(call.id)
        show_tournament_hall_of_fame(chat_id)
        
    elif data == "tournament_dashboard":
        bot.answer_callback_query(call.id)
        show_tournament_dashboard(chat_id)
    
    elif data.startswith("tournament_details_"):
        tournament_id = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        show_tournament_details(chat_id, tournament_id)
    
    elif data.startswith("spectate_"):
        parts = data.split("_")
        tournament_id, match_id = int(parts[1]), int(parts[2])
        bot.answer_callback_query(call.id, "🎥 Spectating match...")
        # Implement spectator mode
    """
    
    return additional_callbacks


def migrate_existing_database():
    """Migrate existing database safely"""
    with db_conn() as db:
        try:
            # Try to add tournament columns to games table
            columns_to_add = [
                ("tournament_id", "INTEGER"),
                ("tournament_round", "INTEGER"), 
                ("opponent_id", "INTEGER"),
                ("is_tournament_match", "BOOLEAN DEFAULT FALSE")
            ]
            
            for col_name, col_type in columns_to_add:
                try:
                    db.execute(f"ALTER TABLE games ADD COLUMN {col_name} {col_type}")
                except sqlite3.OperationalError:
                    pass  # Column already exists
                    
        except Exception as e:
            logger.error(f"Database migration error: {e}")

def initialize_tournament_system():
    """Initialize the complete tournament system"""
    try:
        init_tournament_db()
        migrate_existing_database()
        logger.info("Tournament system initialized successfully!")
    except Exception as e:
        logger.error(f"Failed to initialize tournament system: {e}")
# Enhanced Achievement Checking
def check_all_achievements(user_id: int, game_result: dict):
    """Comprehensive achievement checking after each match"""
    with db_conn() as db:
        # Get current user stats
        stats = db.execute("SELECT * FROM stats WHERE user_id = ?", (user_id,)).fetchone()
        if not stats:
            return
        
        # Check each achievement
        for achievement in ACHIEVEMENTS_LIST:
            # Skip if already unlocked
            existing = db.execute("""
                SELECT user_id FROM user_achievements 
                WHERE user_id = ? AND achievement_id = ?
            """, (user_id, achievement["id"])).fetchone()
            
            if existing:
                continue
            
            # Check if requirement is met
            current_value = stats.get(achievement["requirement_type"], 0)
            
            # Special cases for complex achievements
            if achievement["requirement_type"] == "perfect_game":
                if game_result.get("wickets_lost", 1) == 0 and game_result.get("result") == "win":
                    award_achievement(user_id, achievement["id"])
            elif achievement["requirement_type"] == "fastest_50":
                if (game_result.get("runs_scored", 0) >= 50 and 
                    game_result.get("balls_faced", 999) <= achievement["requirement_value"]):
                    award_achievement(user_id, achievement["id"])
            elif current_value >= achievement["requirement_value"]:
                award_achievement(user_id, achievement["id"])

# Database Migration for Existing Bots

# Tournament Season System
def create_tournament_season():
    """Create seasonal tournament competitions"""
    with db_conn() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS tournament_seasons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                total_tournaments INTEGER DEFAULT 0,
                total_participants INTEGER DEFAULT 0,
                season_champion_id INTEGER,
                season_points_system TEXT,
                status TEXT DEFAULT 'upcoming',
                created_at TEXT,
                FOREIGN KEY (season_champion_id) REFERENCES users (user_id)
            )
        """)

# Initialize everything


# Add these new database functions
def init_tournament_db():
    """Initialize tournament-related database tables"""
    with db_conn() as db:
        # Enhanced tournaments table
        db.execute("""
            CREATE TABLE IF NOT EXISTS tournaments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                format TEXT NOT NULL,
                tournament_type TEXT DEFAULT 'knockout',
                max_participants INTEGER DEFAULT 16,
                current_participants INTEGER DEFAULT 0,
                entry_fee INTEGER DEFAULT 0,
                prize_pool INTEGER DEFAULT 0,
                status TEXT DEFAULT 'upcoming',
                difficulty_level TEXT DEFAULT 'medium',
                start_date TEXT,
                end_date TEXT,
                registration_deadline TEXT,
                created_by INTEGER,
                winner_id INTEGER,
                runner_up_id INTEGER,
                current_round INTEGER DEFAULT 1,
                total_rounds INTEGER,
                bracket_data TEXT,
                created_at TEXT,
                updated_at TEXT,
                FOREIGN KEY (created_by) REFERENCES users (user_id),
                FOREIGN KEY (winner_id) REFERENCES users (user_id),
                FOREIGN KEY (runner_up_id) REFERENCES users (user_id)
            )
        """)
        
        # Tournament participants with enhanced tracking
        db.execute("""
            CREATE TABLE IF NOT EXISTS tournament_participants (
                tournament_id INTEGER,
                user_id INTEGER,
                username TEXT,
                display_name TEXT,
                seed_number INTEGER,
                current_round INTEGER DEFAULT 1,
                is_eliminated BOOLEAN DEFAULT FALSE,
                elimination_round INTEGER,
                matches_played INTEGER DEFAULT 0,
                matches_won INTEGER DEFAULT 0,
                total_runs INTEGER DEFAULT 0,
                total_wickets INTEGER DEFAULT 0,
                joined_at TEXT,
                eliminated_at TEXT,
                PRIMARY KEY (tournament_id, user_id),
                FOREIGN KEY (tournament_id) REFERENCES tournaments (id),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)
        
        # Tournament matches
        db.execute("""
            CREATE TABLE IF NOT EXISTS tournament_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tournament_id INTEGER,
                round_number INTEGER,
                match_number INTEGER,
                player1_id INTEGER,
                player2_id INTEGER,
                player1_name TEXT,
                player2_name TEXT,
                winner_id INTEGER,
                player1_score INTEGER DEFAULT 0,
                player2_score INTEGER DEFAULT 0,
                player1_wickets INTEGER DEFAULT 0,
                player2_wickets INTEGER DEFAULT 0,
                match_status TEXT DEFAULT 'pending',
                match_data TEXT,
                scheduled_at TEXT,
                completed_at TEXT,
                created_at TEXT,
                FOREIGN KEY (tournament_id) REFERENCES tournaments (id),
                FOREIGN KEY (player1_id) REFERENCES users (user_id),
                FOREIGN KEY (player2_id) REFERENCES users (user_id),
                FOREIGN KEY (winner_id) REFERENCES users (user_id)
            )
        """)
        
        # Tournament leaderboards and rankings
        db.execute("""
            CREATE TABLE IF NOT EXISTS tournament_rankings (
                tournament_id INTEGER,
                user_id INTEGER,
                final_position INTEGER,
                rounds_survived INTEGER,
                total_runs INTEGER,
                total_wickets INTEGER,
                tournament_points INTEGER,
                prize_won INTEGER,
                title_earned TEXT,
                PRIMARY KEY (tournament_id, user_id),
                FOREIGN KEY (tournament_id) REFERENCES tournaments (id),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)
        
        # Add user coins/currency system
        db.execute("""
            ALTER TABLE users ADD COLUMN coins INTEGER DEFAULT 100
        """)
        
        # Add tournament stats to user stats
        db.execute("""
            ALTER TABLE stats ADD COLUMN tournaments_played INTEGER DEFAULT 0
        """)
        db.execute("""
            ALTER TABLE stats ADD COLUMN tournaments_won INTEGER DEFAULT 0
        """)
        db.execute("""
            ALTER TABLE stats ADD COLUMN tournament_points INTEGER DEFAULT 0
        """)

# Tournament Management Functions
def create_tournament(creator_id: int, name: str, format_key: str, tournament_type: str = "knockout", max_participants: int = 16):
    """Create a new tournament"""
    with db_conn() as db:
        format_info = TOURNAMENT_FORMATS.get(format_key, TOURNAMENT_FORMATS["quick"])
        prize_pool = format_info["entry_fee"] * max_participants * 0.8  # 80% of entry fees as prize
        
        now = datetime.now(timezone.utc).isoformat()
        registration_deadline = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
        
        # Calculate tournament rounds
        total_rounds = 1
        temp_participants = max_participants
        while temp_participants > 1:
            temp_participants = temp_participants // 2
            total_rounds += 1
        
        cursor = db.execute("""
            INSERT INTO tournaments (
                name, format, tournament_type, max_participants, entry_fee, 
                prize_pool, status, start_date, registration_deadline, 
                created_by, total_rounds, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name, format_key, tournament_type, max_participants, format_info["entry_fee"],
            int(prize_pool), "registration", now, registration_deadline, 
            creator_id, total_rounds, now, now
        ))
        
        return cursor.lastrowid

def join_tournament(tournament_id: int, user_id: int, username: str, display_name: str):
    """Join a tournament"""
    with db_conn() as db:
        # Check tournament status and availability
        tournament = db.execute("""
            SELECT * FROM tournaments WHERE id = ? AND status = 'registration'
        """, (tournament_id,)).fetchone()
        
        if not tournament:
            return {"success": False, "message": "Tournament not available for registration"}
        
        if tournament["current_participants"] >= tournament["max_participants"]:
            return {"success": False, "message": "Tournament is full"}
        
        # Check if user already joined
        existing = db.execute("""
            SELECT user_id FROM tournament_participants 
            WHERE tournament_id = ? AND user_id = ?
        """, (tournament_id, user_id)).fetchone()
        
        if existing:
            return {"success": False, "message": "You're already registered for this tournament"}
        
        # Check user's coins
        user = db.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if not user or user["coins"] < tournament["entry_fee"]:
            return {"success": False, "message": f"Insufficient coins! Need {tournament['entry_fee']} coins"}
        
        # Deduct entry fee and join tournament
        db.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", 
                  (tournament["entry_fee"], user_id))
        
        seed_number = tournament["current_participants"] + 1
        
        db.execute("""
            INSERT INTO tournament_participants (
                tournament_id, user_id, username, display_name, seed_number, joined_at
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (tournament_id, user_id, username, display_name, seed_number, 
              datetime.now(timezone.utc).isoformat()))
        
        # Update tournament participant count
        db.execute("""
            UPDATE tournaments SET 
                current_participants = current_participants + 1,
                updated_at = ?
            WHERE id = ?
        """, (datetime.now(timezone.utc).isoformat(), tournament_id))
        
        return {"success": True, "message": "Successfully joined tournament!", "seed": seed_number}

def start_tournament(tournament_id: int):
    """Start a tournament and generate brackets"""
    with db_conn() as db:
        tournament = db.execute("SELECT * FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
        
        if not tournament or tournament["status"] != "registration":
            return False
        
        participants = db.execute("""
            SELECT * FROM tournament_participants 
            WHERE tournament_id = ? 
            ORDER BY seed_number
        """, (tournament_id,)).fetchall()
        
        if len(participants) < 2:
            return False
        
        # Generate first round matches
        generate_tournament_matches(tournament_id, participants)
        
        # Update tournament status
        db.execute("""
            UPDATE tournaments SET 
                status = 'ongoing',
                start_date = ?,
                updated_at = ?
            WHERE id = ?
        """, (datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat(), tournament_id))
        
        return True

def generate_tournament_matches(tournament_id: int, participants: list):
    """Generate tournament bracket matches"""
    with db_conn() as db:
        # Create first round matches
        round_number = 1
        match_number = 1
        
        # Pair participants (1 vs last, 2 vs second-last, etc.)
        participants_list = list(participants)
        
        for i in range(0, len(participants_list), 2):
            if i + 1 < len(participants_list):
                p1 = participants_list[i]
                p2 = participants_list[i + 1]
                
                db.execute("""
                    INSERT INTO tournament_matches (
                        tournament_id, round_number, match_number,
                        player1_id, player2_id, player1_name, player2_name,
                        match_status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tournament_id, round_number, match_number,
                    p1["user_id"], p2["user_id"], p1["display_name"], p2["display_name"],
                    "pending", datetime.now(timezone.utc).isoformat()
                ))
                
                match_number += 1

# Tournament UI Functions
def kb_tournament_main() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🏆 Join Tournament", callback_data="tournament_join"),
        types.InlineKeyboardButton("🎯 Create Tournament", callback_data="tournament_create")
    )
    kb.add(
        types.InlineKeyboardButton("📊 My Tournaments", callback_data="tournament_my"),
        types.InlineKeyboardButton("🏅 Tournament Rankings", callback_data="tournament_rankings")
    )
    kb.add(
        types.InlineKeyboardButton("📋 Active Tournaments", callback_data="tournament_active"),
        types.InlineKeyboardButton("🏆 Past Winners", callback_data="tournament_winners")
    )
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
    return kb

def kb_tournament_formats() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    for format_key, format_info in TOURNAMENT_FORMATS.items():
        kb.add(types.InlineKeyboardButton(
            f"{format_info['name']} - {format_info['entry_fee']} 🪙",
            callback_data=f"tourney_format_{format_key}"
        ))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="tournament"))
    return kb

def kb_tournament_sizes() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    sizes = [4, 8, 16, 32]
    for size in sizes:
        kb.add(types.InlineKeyboardButton(
            f"{size} Players", callback_data=f"tourney_size_{size}"
        ))
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="tournament_create"))
    return kb

def show_tournaments(chat_id: int, status: str = "all"):
    """Display available tournaments"""
    with db_conn() as db:
        if status == "all":
            tournaments = db.execute("""
                SELECT t.*, u.first_name as creator_name 
                FROM tournaments t
                LEFT JOIN users u ON t.created_by = u.user_id
                WHERE t.status IN ('registration', 'ongoing')
                ORDER BY t.created_at DESC
                LIMIT 10
            """).fetchall()
        else:
            tournaments = db.execute("""
                SELECT t.*, u.first_name as creator_name 
                FROM tournaments t
                LEFT JOIN users u ON t.created_by = u.user_id
                WHERE t.status = ?
                ORDER BY t.created_at DESC
                LIMIT 10
            """, (status,)).fetchall()
        
        if not tournaments:
            bot.send_message(chat_id, 
                           "🏆 <b>No Active Tournaments</b>\n\n"
                           "Be the first to create one!")
            return
        
        for tournament in tournaments:
            format_info = TOURNAMENT_FORMATS.get(tournament["format"], TOURNAMENT_FORMATS["quick"])
            
            status_emoji = {
                "registration": "📝",
                "ongoing": "⚡",
                "completed": "✅"
            }.get(tournament["status"], "📋")
            
            tournament_text = (
                f"{status_emoji} <b>{tournament['name']}</b>\n\n"
                f"🎮 Format: {format_info['name']}\n"
                f"👥 Players: {tournament['current_participants']}/{tournament['max_participants']}\n"
                f"💰 Entry Fee: {tournament['entry_fee']} coins\n"
                f"🏆 Prize Pool: {tournament['prize_pool']} coins\n"
                f"📊 Status: {tournament['status'].title()}\n"
                f"👤 Created by: {tournament['creator_name'] or 'Anonymous'}"
            )
            
            # Add join button for registration tournaments
            kb = types.InlineKeyboardMarkup()
            if tournament["status"] == "registration":
                kb.add(types.InlineKeyboardButton(
                    f"🎯 Join Tournament ({tournament['entry_fee']} coins)",
                    callback_data=f"join_tournament_{tournament['id']}"
                ))
            elif tournament["status"] == "ongoing":
                kb.add(types.InlineKeyboardButton(
                    "📊 View Bracket", 
                    callback_data=f"tournament_bracket_{tournament['id']}"
                ))
            
            kb.add(types.InlineKeyboardButton(
                "📋 Tournament Details",
                callback_data=f"tournament_details_{tournament['id']}"
            ))
            
            bot.send_message(chat_id, tournament_text, reply_markup=kb)

def show_tournament_bracket(chat_id: int, tournament_id: int):
    """Display tournament bracket"""
    with db_conn() as db:
        tournament = db.execute("SELECT * FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
        if not tournament:
            bot.send_message(chat_id, "❌ Tournament not found")
            return
        
        # Get current round matches
        matches = db.execute("""
            SELECT * FROM tournament_matches 
            WHERE tournament_id = ? AND round_number = ?
            ORDER BY match_number
        """, (tournament_id, tournament["current_round"])).fetchall()
        
        bracket_text = f"🏆 <b>{tournament['name']} - Bracket</b>\n\n"
        bracket_text += f"🎯 Current Round: {tournament['current_round']}/{tournament['total_rounds']}\n\n"
        
        for match in matches:
            match_status = "vs" if match["match_status"] == "pending" else "✅"
            winner_indicator = ""
            
            if match["winner_id"]:
                if match["winner_id"] == match["player1_id"]:
                    winner_indicator = " 🏆"
                else:
                    winner_indicator = " 🏆"
            
            bracket_text += (
                f"🥊 <b>Match {match['match_number']}:</b>\n"
                f"   {match['player1_name']}{' 🏆' if match['winner_id'] == match['player1_id'] else ''}\n"
                f"   {match_status}\n"
                f"   {match['player2_name']}{' 🏆' if match['winner_id'] == match['player2_id'] else ''}\n\n"
            )
        
        bot.send_message(chat_id, bracket_text)

# Enhanced Achievements System
def show_achievements_enhanced(chat_id: int, user_id: int):
    """Show all achievements with completion status"""
    with db_conn() as db:
        # Get user's unlocked achievements
        unlocked_achievements = db.execute("""
            SELECT achievement_id FROM user_achievements WHERE user_id = ?
        """, (user_id,)).fetchall()
        
        unlocked_ids = [row["achievement_id"] for row in unlocked_achievements]
        
        # Get user stats for progress calculation
        stats = db.execute("SELECT * FROM stats WHERE user_id = ?", (user_id,)).fetchone()
        user_stats = dict(stats) if stats else {}
        
        achievements_text = "🏅 <b>Achievements Gallery</b>\n\n"
        
        unlocked_count = 0
        total_points = 0
        
        for achievement in ACHIEVEMENTS_LIST:
            is_unlocked = achievement["id"] in unlocked_ids
            
            if is_unlocked:
                unlocked_count += 1
                total_points += achievement["points"]
                status_icon = "✅"
                progress_text = "COMPLETED!"
            else:
                status_icon = "⬜"
                # Calculate progress
                current_value = user_stats.get(achievement["requirement_type"], 0)
                target_value = achievement["requirement_value"]
                
                if current_value >= target_value:
                    progress_text = "🎯 Ready to claim!"
                else:
                    progress_percentage = min(100, (current_value / target_value) * 100)
                    progress_text = f"📊 {current_value}/{target_value} ({progress_percentage:.0f}%)"
            
            achievements_text += (
                f"{status_icon} {achievement['icon']} <b>{achievement['name']}</b>\n"
                f"   {achievement['description']}\n"
                f"   {progress_text} • +{achievement['points']} points\n\n"
            )
        
        # Add summary
        summary_text = (
            f"📊 <b>Progress Summary</b>\n"
            f"🎯 Completed: {unlocked_count}/{len(ACHIEVEMENTS_LIST)}\n"
            f"⭐ Total Points: {total_points}\n"
            f"🏆 Completion: {(unlocked_count/len(ACHIEVEMENTS_LIST)*100):.1f}%"
        )
        
        full_text = achievements_text + summary_text
        
        # Split message if too long
        if len(full_text) > 4000:
            bot.send_message(chat_id, achievements_text)
            bot.send_message(chat_id, summary_text)
        else:
            bot.send_message(chat_id, full_text)

# Update the callback handler to include tournament functionality
def handle_tournament_callbacks(call: types.CallbackQuery):
    """Handle tournament-related callbacks"""
    data = call.data
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    message_id = call.message.message_id
    
    if data == "tournament":
        bot.answer_callback_query(call.id)
        bot.edit_message_text(
            "🏆 <b>Tournament Central</b>\n\n"
            "Welcome to the ultimate cricket competition hub!\n"
            "Join existing tournaments or create your own championship!",
            chat_id, message_id, reply_markup=kb_tournament_main()
        )
    
    elif data == "tournament_join":
        bot.answer_callback_query(call.id)
        bot.edit_message_text(
            "🎯 <b>Available Tournaments</b>\n\nBrowse and join active tournaments:",
            chat_id, message_id
        )
        show_tournaments(chat_id, "registration")
    
    elif data == "tournament_create":
        bot.answer_callback_query(call.id)
        bot.edit_message_text(
            "🎮 <b>Create Tournament</b>\n\nSelect tournament format:",
            chat_id, message_id, reply_markup=kb_tournament_formats()
        )
    
    elif data.startswith("tourney_format_"):
        format_key = data.split("_")[-1]
        set_session_data(user_id, "tournament_format", format_key)
        
        format_info = TOURNAMENT_FORMATS.get(format_key, TOURNAMENT_FORMATS["quick"])
        bot.edit_message_text(
            f"✅ <b>Format Selected:</b> {format_info['name']}\n\n"
            f"Choose tournament size:",
            chat_id, message_id, reply_markup=kb_tournament_sizes()
        )
    
    elif data.startswith("tourney_size_"):
        size = int(data.split("_")[-1])
        format_key = get_session_data(user_id, "tournament_format", "quick")
        
        # Create tournament
        tournament_name = f"{TOURNAMENT_FORMATS[format_key]['name']} Championship"
        tournament_id = create_tournament(user_id, tournament_name, format_key, "knockout", size)
        
        bot.edit_message_text(
            f"🏆 <b>Tournament Created!</b>\n\n"
            f"Name: {tournament_name}\n"
            f"Size: {size} players\n"
            f"Status: Registration Open\n\n"
            f"Tournament ID: #{tournament_id}\n"
            f"Share this with friends to join!",
            chat_id, message_id
        )
        
        # Clear session
        if user_id in user_sessions:
            user_sessions[user_id].clear()
    
    elif data.startswith("join_tournament_"):
        tournament_id = int(data.split("_")[-1])
        
        # Get user info
        user = bot.get_chat_member(chat_id, user_id).user
        display_name = user.first_name or f"@{user.username}" if user.username else f"User{user_id}"
        
        result = join_tournament(tournament_id, user_id, user.username or "", display_name)
        
        if result["success"]:
            bot.answer_callback_query(call.id, f"🎉 {result['message']}")
            bot.edit_message_text(
                f"✅ <b>Successfully Joined Tournament!</b>\n\n"
                f"Your seed: #{result['seed']}\n"
                f"Status: Waiting for more players...\n\n"
                f"Tournament will start when full!",
                chat_id, message_id
            )
        else:
            bot.answer_callback_query(call.id, f"❌ {result['message']}")
    
    elif data == "tournament_active":
        bot.answer_callback_query(call.id)
        show_tournaments(chat_id, "ongoing")
    
    elif data.startswith("tournament_bracket_"):
        tournament_id = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        show_tournament_bracket(chat_id, tournament_id)

# Update your main callback handler to include tournament and enhanced achievements
# Add this to your existing handle_callbacks function:

# In the main handle_callbacks function, add these cases:

# Also update your db_init() function to include:


# Add this tournament automation function
def check_tournament_automation():
    """Background task to manage tournament progression"""
    with db_conn() as db:
        # Check for tournaments ready to start
        ready_tournaments = db.execute("""
            SELECT t.* FROM tournaments t
            WHERE t.status = 'registration' 
            AND t.current_participants >= 2
            AND datetime(t.registration_deadline) <= datetime('now')
        """).fetchall()
        
        for tournament in ready_tournaments:
            if start_tournament(tournament["id"]):
                logger.info(f"Auto-started tournament {tournament['id']}")
        
        # Check for completed matches and advance rounds
        # This would be expanded based on your match completion logic

# Tournament Live Match System
def create_tournament_match(tournament_id: int, round_number: int, player1_id: int, player2_id: int):
    """Create and manage a tournament match"""
    with db_conn() as db:
        tournament = db.execute("SELECT * FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
        if not tournament:
            return None
        
        format_info = TOURNAMENT_FORMATS.get(tournament["format"], TOURNAMENT_FORMATS["quick"])
        
        # Create a special tournament game
        match_id = f"tournament_{tournament_id}_{round_number}_{player1_id}_{player2_id}"
        
        # Store match context in session for both players
        match_context = {
            "tournament_id": tournament_id,
            "round_number": round_number,
            "player1_id": player1_id,
            "player2_id": player2_id,
            "format": tournament["format"],
            "overs": format_info["overs"],
            "wickets": format_info["wickets"],
            "difficulty": tournament["difficulty_level"]
        }
        
        return match_context

def handle_tournament_match_completion(chat_id: int, game_data: dict, winner_id: int, loser_id: int):
    """Handle completion of a tournament match"""
    if not game_data.get("is_tournament_match"):
        return
    
    tournament_id = game_data.get("tournament_id")
    round_number = game_data.get("round_number")
    
    with db_conn() as db:
        # Update match result
        db.execute("""
            UPDATE tournament_matches SET
                winner_id = ?,
                match_status = 'completed',
                completed_at = ?
            WHERE tournament_id = ? AND round_number = ? 
            AND ((player1_id = ? AND player2_id = ?) OR (player1_id = ? AND player2_id = ?))
        """, (winner_id, datetime.now(timezone.utc).isoformat(), tournament_id, round_number,
              winner_id, loser_id, loser_id, winner_id))
        
        # Update participant status
        db.execute("""
            UPDATE tournament_participants SET
                is_eliminated = TRUE,
                elimination_round = ?,
                eliminated_at = ?
            WHERE tournament_id = ? AND user_id = ?
        """, (round_number, datetime.now(timezone.utc).isoformat(), tournament_id, loser_id))
        
        # Check if round is complete
        check_and_advance_tournament_round(tournament_id, round_number)

def check_and_advance_tournament_round(tournament_id: int, current_round: int):
    """Check if tournament round is complete and advance to next round"""
    with db_conn() as db:
        # Count remaining matches in current round
        pending_matches = db.execute("""
            SELECT COUNT(*) as count FROM tournament_matches
            WHERE tournament_id = ? AND round_number = ? AND match_status = 'pending'
        """, (tournament_id, current_round)).fetchone()["count"]
        
        if pending_matches > 0:
            return  # Round not complete yet
        
        # Get tournament info
        tournament = db.execute("SELECT * FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
        
        # Get winners from current round
        winners = db.execute("""
            SELECT winner_id FROM tournament_matches
            WHERE tournament_id = ? AND round_number = ?
        """, (tournament_id, current_round)).fetchall()
        
        winner_ids = [w["winner_id"] for w in winners]
        
        if len(winner_ids) <= 1:
            # Tournament complete!
            complete_tournament(tournament_id, winner_ids[0] if winner_ids else None)
        else:
            # Create next round
            create_next_tournament_round(tournament_id, current_round + 1, winner_ids)

def create_next_tournament_round(tournament_id: int, round_number: int, participant_ids: list):
    """Create matches for next tournament round"""
    with db_conn() as db:
        match_number = 1
        
        # Pair up participants for next round
        for i in range(0, len(participant_ids), 2):
            if i + 1 < len(participant_ids):
                p1_id = participant_ids[i]
                p2_id = participant_ids[i + 1]
                
                # Get participant names
                p1 = db.execute("SELECT display_name FROM tournament_participants WHERE tournament_id = ? AND user_id = ?", 
                               (tournament_id, p1_id)).fetchone()
                p2 = db.execute("SELECT display_name FROM tournament_participants WHERE tournament_id = ? AND user_id = ?", 
                               (tournament_id, p2_id)).fetchone()
                
                db.execute("""
                    INSERT INTO tournament_matches (
                        tournament_id, round_number, match_number,
                        player1_id, player2_id, player1_name, player2_name,
                        match_status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                """, (tournament_id, round_number, match_number,
                      p1_id, p2_id, p1["display_name"], p2["display_name"],
                      datetime.now(timezone.utc).isoformat()))
                
                match_number += 1
        
        # Update tournament current round
        db.execute("UPDATE tournaments SET current_round = ? WHERE id = ?", 
                  (round_number, tournament_id))
        
        # Notify participants
        notify_tournament_round_start(tournament_id, round_number)

def complete_tournament(tournament_id: int, winner_id: int):
    """Complete tournament and distribute prizes"""
    with db_conn() as db:
        tournament = db.execute("SELECT * FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
        
        # Update tournament status
        db.execute("""
            UPDATE tournaments SET
                status = 'completed',
                winner_id = ?,
                end_date = ?
            WHERE id = ?
        """, (winner_id, datetime.now(timezone.utc).isoformat(), tournament_id))
        
        # Distribute prizes and titles
        distribute_tournament_rewards(tournament_id, tournament["prize_pool"])
        
        # Update participant stats
        update_tournament_stats(tournament_id)
        
        # Send completion notifications
        notify_tournament_completion(tournament_id)

def distribute_tournament_rewards(tournament_id: int, prize_pool: int):
    """Distribute rewards to tournament participants"""
    with db_conn() as db:
        # Get final rankings
        participants = db.execute("""
            SELECT tp.*, tm.round_number as eliminated_round
            FROM tournament_participants tp
            LEFT JOIN (
                SELECT tournament_id, 
                       CASE WHEN winner_id = player1_id THEN player2_id 
                            ELSE player1_id END as eliminated_player,
                       round_number
                FROM tournament_matches 
                WHERE tournament_id = ?
            ) tm ON tp.tournament_id = tm.tournament_id AND tp.user_id = tm.eliminated_player
            WHERE tp.tournament_id = ?
            ORDER BY COALESCE(tm.round_number, 999) DESC, tp.total_runs DESC
        """, (tournament_id, tournament_id)).fetchall()
        
        # Distribute prizes based on final position
        total_prize = prize_pool
        
        for i, participant in enumerate(participants[:4]):  # Top 4 get prizes
            position = i + 1
            
            if position == 1:  # Winner
                prize = int(total_prize * 0.5)  # 50% to winner
                title = TOURNAMENT_REWARDS["winner"]["title"]
                trophy_points = TOURNAMENT_REWARDS["winner"]["trophy_points"]
            elif position == 2:  # Runner-up
                prize = int(total_prize * 0.3)  # 30% to runner-up
                title = TOURNAMENT_REWARDS["runner_up"]["title"]
                trophy_points = TOURNAMENT_REWARDS["runner_up"]["trophy_points"]
            elif position == 3:  # Third place
                prize = int(total_prize * 0.15)  # 15% to third
                title = TOURNAMENT_REWARDS["semi_finalist"]["title"]
                trophy_points = TOURNAMENT_REWARDS["semi_finalist"]["trophy_points"]
            else:  # Fourth place
                prize = int(total_prize * 0.05)  # 5% to fourth
                title = TOURNAMENT_REWARDS["quarter_finalist"]["title"]
                trophy_points = TOURNAMENT_REWARDS["quarter_finalist"]["trophy_points"]
            
            # Give coins to user
            db.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", 
                      (prize, participant["user_id"]))
            
            # Record ranking
            db.execute("""
                INSERT INTO tournament_rankings (
                    tournament_id, user_id, final_position, 
                    tournament_points, prize_won, title_earned
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (tournament_id, participant["user_id"], position, 
                  trophy_points, prize, title))

def notify_tournament_completion(tournament_id: int):
    """Send tournament completion notifications to all participants"""
    with db_conn() as db:
        tournament = db.execute("SELECT * FROM tournaments WHERE id = ?", (tournament_id,)).fetchone()
        rankings = db.execute("""
            SELECT tr.*, u.first_name, tp.display_name
            FROM tournament_rankings tr
            JOIN users u ON tr.user_id = u.user_id
            JOIN tournament_participants tp ON tr.tournament_id = tp.tournament_id AND tr.user_id = tp.user_id
            WHERE tr.tournament_id = ?
            ORDER BY tr.final_position
            LIMIT 3
        """, (tournament_id,)).fetchall()
        
        completion_text = f"🏆 <b>{tournament['name']} - COMPLETED!</b>\n\n"
        
        for rank in rankings:
            position_emoji = "🥇" if rank["final_position"] == 1 else "🥈" if rank["final_position"] == 2 else "🥉"
            completion_text += f"{position_emoji} {rank['display_name']} - {rank['title_earned']}\n"
        
        completion_text += f"\nTotal Prize Pool: {tournament['prize_pool']} coins distributed!"
        
        # Send to all participants
        participants = db.execute("""
            SELECT user_id FROM tournament_participants WHERE tournament_id = ?
        """, (tournament_id,)).fetchall()
        
        for participant in participants:
            try:
                bot.send_message(participant["user_id"], completion_text)
            except Exception as e:
                logger.warning(f"Could not notify user {participant['user_id']}: {e}")

# Enhanced Tournament Features
def show_tournament_details(chat_id: int, tournament_id: int):
    """Show detailed tournament information"""
    with db_conn() as db:
        tournament = db.execute("""
            SELECT t.*, u.first_name as creator_name 
            FROM tournaments t
            LEFT JOIN users u ON t.created_by = u.user_id
            WHERE t.id = ?
        """, (tournament_id,)).fetchone()
        
        if not tournament:
            bot.send_message(chat_id, "❌ Tournament not found")
            return
        
        format_info = TOURNAMENT_FORMATS.get(tournament["format"], TOURNAMENT_FORMATS["quick"])
        
        # Get participants
        participants = db.execute("""
            SELECT display_name, seed_number, matches_won, total_runs
            FROM tournament_participants
            WHERE tournament_id = ?
            ORDER BY seed_number
        """, (tournament_id,)).fetchall()
        
        details_text = (
            f"🏆 <b>{tournament['name']}</b>\n\n"
            f"📋 <b>Tournament Details:</b>\n"
            f"• Format: {format_info['name']}\n"
            f"• Type: {tournament['tournament_type'].title()}\n"
            f"• Status: {tournament['status'].title()}\n"
            f"• Players: {tournament['current_participants']}/{tournament['max_participants']}\n"
            f"• Entry Fee: {tournament['entry_fee']} coins\n"
            f"• Prize Pool: {tournament['prize_pool']} coins\n"
            f"• Created by: {tournament['creator_name'] or 'Anonymous'}\n\n"
        )
        
        if tournament["status"] == "ongoing":
            details_text += f"⚡ Current Round: {tournament['current_round']}/{tournament['total_rounds']}\n\n"
        
        if participants:
            details_text += "👥 <b>Participants:</b>\n"
            for i, p in enumerate(participants[:10]):  # Show first 10
                status = "🏆" if tournament["winner_id"] and tournament["winner_id"] == p.get("user_id") else ""
                details_text += f"{p['seed_number']}. {p['display_name']} {status}\n"
            
            if len(participants) > 10:
                details_text += f"... and {len(participants) - 10} more"
        
        bot.send_message(chat_id, details_text)

def show_my_tournaments(chat_id: int, user_id: int):
    """Show user's tournament history and current tournaments"""
    with db_conn() as db:
        # Current tournaments
        current = db.execute("""
            SELECT t.*, tp.seed_number, tp.is_eliminated
            FROM tournaments t
            JOIN tournament_participants tp ON t.id = tp.tournament_id
            WHERE tp.user_id = ? AND t.status IN ('registration', 'ongoing')
            ORDER BY t.created_at DESC
        """, (user_id,)).fetchall()
        
        # Past tournaments with rankings
        past = db.execute("""
            SELECT t.name, tr.final_position, tr.prize_won, tr.title_earned, t.created_at
            FROM tournaments t
            JOIN tournament_rankings tr ON t.id = tr.tournament_id
            WHERE tr.user_id = ? AND t.status = 'completed'
            ORDER BY t.created_at DESC
            LIMIT 10
        """, (user_id,)).fetchall()
        
        my_tournaments_text = "🏆 <b>My Tournaments</b>\n\n"
        
        if current:
            my_tournaments_text += "⚡ <b>Current Tournaments:</b>\n"
            for t in current:
                status = "❌ Eliminated" if t["is_eliminated"] else "✅ Active"
                my_tournaments_text += f"• {t['name']} - Seed #{t['seed_number']} ({status})\n"
            my_tournaments_text += "\n"
        
        if past:
            my_tournaments_text += "📚 <b>Tournament History:</b>\n"
            for t in past:
                position_emoji = "🥇" if t["final_position"] == 1 else "🥈" if t["final_position"] == 2 else "🥉" if t["final_position"] == 3 else f"#{t['final_position']}"
                my_tournaments_text += (f"{position_emoji} {t['name']}\n"
                                      f"   {t['title_earned']} • +{t['prize_won']} coins\n\n")
        
        if not current and not past:
            my_tournaments_text += "📝 No tournaments yet! Join one to get started."
        
        bot.send_message(chat_id, my_tournaments_text)

# Tournament Spectator Features
def show_live_tournament_matches(chat_id: int):
    """Show ongoing tournament matches that can be spectated"""
    with db_conn() as db:
        live_matches = db.execute("""
            SELECT tm.*, t.name as tournament_name
            FROM tournament_matches tm
            JOIN tournaments t ON tm.tournament_id = t.id
            WHERE tm.match_status = 'in_progress'
            ORDER BY tm.created_at DESC
            LIMIT 5
        """).fetchall()
        
        if not live_matches:
            bot.send_message(chat_id, "📺 No live tournament matches at the moment.")
            return
        
        for match in live_matches:
            match_text = (
                f"📺 <b>LIVE: {match['tournament_name']}</b>\n"
                f"Round {match['round_number']} - Match {match['match_number']}\n\n"
                f"🥊 {match['player1_name']} vs {match['player2_name']}\n"
                f"Score: {match['player1_score']}/{match['player1_wickets']} vs {match['player2_score']}/{match['player2_wickets']}"
            )
            
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("👀 Spectate", 
                                            callback_data=f"spectate_{match['tournament_id']}_{match['id']}"))
            
            bot.send_message(chat_id, match_text, reply_markup=kb)

# Tournament Statistics and Analytics
def generate_tournament_analytics():
    """Generate tournament analytics and insights"""
    with db_conn() as db:
        # Most successful players
        top_players = db.execute("""
            SELECT u.first_name, u.username,
                   COUNT(tr.tournament_id) as tournaments_played,
                   SUM(CASE WHEN tr.final_position = 1 THEN 1 ELSE 0 END) as wins,
                   AVG(tr.final_position) as avg_position,
                   SUM(tr.prize_won) as total_prizes
            FROM tournament_rankings tr
            JOIN users u ON tr.user_id = u.user_id
            GROUP BY tr.user_id
            HAVING tournaments_played >= 3
            ORDER BY wins DESC, avg_position ASC
            LIMIT 10
        """).fetchall()
        
        # Popular formats
        popular_formats = db.execute("""
            SELECT format, COUNT(*) as tournament_count,
                   AVG(current_participants) as avg_participants
            FROM tournaments
            WHERE status = 'completed'
            GROUP BY format
            ORDER BY tournament_count DESC
        """).fetchall()
        
        analytics = {
            "top_players": top_players,
            "popular_formats": popular_formats
        }
        
        return analytics

# Anti-cheat and Fair Play Systems
def detect_suspicious_activity(user_id: int, tournament_id: int):
    """Detect potential cheating or suspicious patterns"""
    with db_conn() as db:
        # Check for unusual win patterns, extremely high scores, etc.
        recent_matches = db.execute("""
            SELECT * FROM match_history
            WHERE user_id = ? AND created_at > datetime('now', '-24 hours')
            ORDER BY created_at DESC
        """, (user_id,)).fetchall()
        
        # Implement anti-cheat logic here
        # For example: consecutive perfect scores, impossible reaction times, etc.
        
        suspicious_flags = []
        
        # Check for too many perfect games
        perfect_games = sum(1 for match in recent_matches if match["player_wickets"] == 0 and match["player_score"] > 50)
        if perfect_games > 5:
            suspicious_flags.append("too_many_perfect_games")
        
        return suspicious_flags

# Tournament Rewards and Achievements Integration
def award_tournament_achievements(user_id: int, tournament_result: dict):
    """Award achievements based on tournament performance"""
    with db_conn() as db:
        # Check for tournament-specific achievements
        if tournament_result.get("final_position") == 1:
            # Award "Tournament Winner" achievement
            award_achievement(user_id, 12)  # Tournament Winner achievement
        
        # Update tournament stats
        db.execute("""
            UPDATE stats SET 
                tournaments_played = tournaments_played + 1,
                tournaments_won = tournaments_won + CASE WHEN ? = 1 THEN 1 ELSE 0 END
            WHERE user_id = ?
        """, (tournament_result.get("final_position", 0), user_id))

def award_achievement(user_id: int, achievement_id: int):
    """Award an achievement to a user"""
    with db_conn() as db:
        # Check if already awarded
        existing = db.execute("""
            SELECT user_id FROM user_achievements 
            WHERE user_id = ? AND achievement_id = ?
        """, (user_id, achievement_id)).fetchone()
        
        if not existing:
            db.execute("""
                INSERT INTO user_achievements (user_id, achievement_id, unlocked_at)
                VALUES (?, ?, ?)
            """, (user_id, achievement_id, datetime.now(timezone.utc).isoformat()))
            
            # Notify user
            achievement = next((a for a in ACHIEVEMENTS_LIST if a["id"] == achievement_id), None)
            if achievement:
                try:
                    bot.send_message(user_id, 
                                   f"🏅 <b>Achievement Unlocked!</b>\n\n"
                                   f"{achievement['icon']} <b>{achievement['name']}</b>\n"
                                   f"{achievement['description']}\n"
                                   f"+{achievement['points']} points!")
                except:
                    pass  # User might have blocked the bot

# Enhanced Tournament Matchmaking
def create_balanced_tournament_bracket(tournament_id: int):
    """Create a balanced tournament bracket based on player ratings"""
    with db_conn() as db:
        # Get participants with their skill ratings
        participants = db.execute("""
            SELECT tp.*, s.wins, s.losses, s.avg_score, s.high_score
            FROM tournament_participants tp
            JOIN stats s ON tp.user_id = s.user_id
            WHERE tp.tournament_id = ?
            ORDER BY s.wins DESC, s.avg_score DESC
        """, (tournament_id,)).fetchall()
        
        # Implement seeding based on player stats
        # Higher skilled players get better seeds
        for i, participant in enumerate(participants):
            db.execute("""
                UPDATE tournament_participants 
                SET seed_number = ?
                WHERE tournament_id = ? AND user_id = ?
            """, (i + 1, tournament_id, participant["user_id"]))

# Add this to your background tasks initialization
def start_tournament_background_tasks():
    """Start tournament-specific background tasks"""
    def tournament_automation():
        while True:
            try:
                check_tournament_automation()
                time.sleep(300)  # Check every 5 minutes
            except Exception as e:
                logger.error(f"Error in tournament automation: {e}")
                time.sleep(600)  # Wait 10 minutes on error
    
    thread = threading.Thread(target=tournament_automation, daemon=True)
    thread.start()
    logger.info("Tournament background tasks started")

# Add tournament tasks to your main setup
def enhanced_setup_bot():
    """Enhanced setup with tournament features"""
    setup_bot()  # Your existing setup
    start_tournament_background_tasks()


# ======================================================
# Callback Query Handlers
# ======================================================

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call: types.CallbackQuery):
    try:
        data = call.data
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        message_id = call.message.message_id
        
        logger.info(f"Callback received: {data} from user {user_id}")
        
        # Main menu navigation
        if data == "quick_play":
            bot.answer_callback_query(call.id, "Starting quick match...")
            try:
                # Check for existing game first
                existing_game = safe_load_game(chat_id)
                if existing_game and existing_game.get("state") in ["toss", "play"]:
                    bot.edit_message_text(
                        "⚠️ You have an active match! Use 🏳️ Forfeit to abandon it, or continue playing.",
                        chat_id, message_id, reply_markup=kb_match_actions()
                    )
                    return
                
                safe_start_new_game(chat_id, 2, 1, "medium", user_id)
            except Exception as e:
                logger.error(f"Error starting quick play: {e}")
                bot.edit_message_text(
                    "❌ Error starting match. Please try again.", 
                    chat_id, message_id, reply_markup=kb_main_menu()
                )
            
        elif data == "custom_match":
            bot.answer_callback_query(call.id)
            try:
                bot.edit_message_text(
                    "🎮 <b>Custom Match</b>\n\nChoose your match format:", 
                    chat_id, message_id, reply_markup=kb_format_select()
                )
            except Exception as e:
                logger.error(f"Error showing custom match: {e}")
                bot.answer_callback_query(call.id, "❌ Error loading custom match options")
                

        
        elif data.startswith("tournament_") or data.startswith("tourney_") or data.startswith("join_tournament_"):
            handle_tournament_callbacks(call)
        elif data == "achievements":
            bot.answer_callback_query(call.id)
            show_achievements_enhanced(chat_id, user_id)  # REPLACE show_achievements        
        elif data.startswith("format_"):
            bot.answer_callback_query(call.id)
            try:
                if data == "format_random":
                    overs = random.randint(1, 10)
                    wickets = random.randint(1, 3)
                    format_name = f"Random T{overs}"
                else:
                    parts = data.split("_")
                    if len(parts) >= 3:
                        overs, wickets = int(parts[1]), int(parts[2])
                        format_name = f"T{overs}"
                    else:
                        # Fallback for malformed format
                        overs, wickets = 2, 1
                        format_name = "T2"
                
                # Store format in session
                set_session_data(user_id, "selected_overs", overs)
                set_session_data(user_id, "selected_wickets", wickets)
                set_session_data(user_id, "format_name", format_name)
                
                # Show difficulty selection
                bot.edit_message_text(
                    f"🎯 <b>Format Selected:</b> {format_name} ({wickets} wicket{'s' if wickets > 1 else ''})\n\n"
                    f"Choose difficulty level:",
                    chat_id, message_id, reply_markup=kb_difficulty_select()
                )
            except Exception as e:
                logger.error(f"Error handling format selection: {e}")
                bot.edit_message_text(
                    "❌ Error processing format selection. Please try again.",
                    chat_id, message_id, reply_markup=kb_format_select()
                )
                
        elif data.startswith("diff_"):
            bot.answer_callback_query(call.id)
            try:
                difficulty = data.split("_")[1]
                
                # Get format from session
                overs = get_session_data(user_id, "selected_overs", 2)
                wickets = get_session_data(user_id, "selected_wickets", 1)
                format_name = get_session_data(user_id, "format_name", "T2")
                
                # Validate difficulty
                if difficulty not in DIFFICULTY_SETTINGS:
                    difficulty = "medium"
                
                difficulty_desc = DIFFICULTY_SETTINGS[difficulty]["description"]
                
                bot.edit_message_text(
                    f"✅ <b>Match Configuration:</b>\n"
                    f"Format: {format_name} ({wickets} wicket{'s' if wickets > 1 else ''})\n"
                    f"Difficulty: {difficulty.title()} - {difficulty_desc}\n\n"
                    f"Starting match...",
                    chat_id, message_id
                )
                
                # Start the custom match
                safe_start_new_game(chat_id, overs, wickets, difficulty, user_id)
                
                # Clear session data
                session = get_user_session(user_id)
                session.clear()
                
            except Exception as e:
                logger.error(f"Error handling difficulty selection: {e}")
                bot.edit_message_text(
                    "❌ Error starting custom match. Please try again.",
                    chat_id, message_id, reply_markup=kb_main_menu()
                )
        
        elif data == "back_main" or data == "main_menu":
            bot.answer_callback_query(call.id)
            try:
                bot.edit_message_text(
                    "🏏 <b>Cricket Bot</b> - Main Menu\n\nSelect an option to continue:", 
                    chat_id, message_id, reply_markup=kb_main_menu()
                )
                # Clear any session data
                if user_id in user_sessions:
                    user_sessions[user_id].clear()
            except Exception as e:
                logger.error(f"Error returning to main menu: {e}")
                bot.answer_callback_query(call.id, "❌ Error loading main menu")
                
        elif data == "my_stats":
            bot.answer_callback_query(call.id)
            try:
                show_user_stats(chat_id, user_id)
            except Exception as e:
                logger.error(f"Error showing stats: {e}")
                bot.answer_callback_query(call.id, "❌ Error loading statistics")
                
        elif data == "leaderboard":
            bot.answer_callback_query(call.id)
            try:
                show_leaderboard(chat_id)
            except Exception as e:
                logger.error(f"Error showing leaderboard: {e}")
                bot.answer_callback_query(call.id, "❌ Error loading leaderboard")
                
        elif data.startswith("lb_"):
            bot.answer_callback_query(call.id)
            try:
                category = data.split("_", 1)[1]
                show_leaderboard(chat_id, category)
            except Exception as e:
                logger.error(f"Error showing leaderboard category: {e}")
                bot.answer_callback_query(call.id, "❌ Error loading leaderboard")
                
        elif data == "achievements":
            bot.answer_callback_query(call.id)
            try:
                show_achievements(chat_id, user_id)
            except Exception as e:
                logger.error(f"Error showing achievements: {e}")
                bot.answer_callback_query(call.id, "❌ Error loading achievements")
                
        # Toss handling
        elif data.startswith("toss_"):
            handle_toss(call)
            
        elif data in ["choose_bat", "choose_bowl"]:
            handle_batting_choice(call)
            
        # Match actions
        elif data == "live_score":
            bot.answer_callback_query(call.id)
            try:
                g = safe_load_game(chat_id)
                if g and g.get("state") == "play":
                    show_live_score(chat_id, g, detailed=True)
                else:
                    bot.answer_callback_query(call.id, "❌ No active match found")
            except Exception as e:
                logger.error(f"Error showing live score: {e}")
                bot.answer_callback_query(call.id, "❌ Error loading score")
            
        elif data == "forfeit_confirm":
            bot.answer_callback_query(call.id, "Match forfeited!")
            try:
                delete_game(chat_id)
                bot.edit_message_text(
                    "🏳️ <b>Match Forfeited</b>\n\nUse /play to start a new match.", 
                    chat_id, message_id, reply_markup=kb_main_menu()
                )
            except Exception as e:
                logger.error(f"Error forfeiting match: {e}")
                
        elif data == "play_again":
            bot.answer_callback_query(call.id, "Starting new match...")
            try:
                safe_start_new_game(chat_id, 2, 1, "medium", user_id)
            except Exception as e:
                logger.error(f"Error starting new match: {e}")
                bot.answer_callback_query(call.id, "❌ Error starting new match")
        
        # Tournament and Challenges (placeholder)
        elif data == "tournament":
            handle_tournament_callbacks(call)
        elif data.startswith("tournament_") or data.startswith("tourney_") or data.startswith("join_tournament_"):
            handle_tournament_callbacks(call)
            
        elif data == "challenges":
            bot.answer_callback_query(call.id, "🎯 Daily challenges coming soon!")
            
        elif data == "help":
            bot.answer_callback_query(call.id)
            try:
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
                    f"/help - Show this help\n\n"
                    f"<b>🎯 Pro Tips:</b>\n"
                    f"• Use powerplay overs wisely\n"
                    f"• Watch the required run rate\n"
                    f"• Different difficulties change bot behavior"
                )
                bot.edit_message_text(help_text, chat_id, message_id, reply_markup=kb_main_menu())
            except Exception as e:
                logger.error(f"Error showing help: {e}")
                bot.answer_callback_query(call.id, "❌ Error loading help")
        
        else:
            logger.warning(f"Unknown callback data: {data}")
            bot.answer_callback_query(call.id, "🚧 Feature coming soon!")
            
    except Exception as e:
        logger.error(f"Error in callback handler: {e}")
        bot.answer_callback_query(call.id, "❌ Something went wrong!")

def show_achievements(chat_id: int, user_id: int):
    """Placeholder for original achievements function"""
    show_achievements_enhanced(chat_id, user_id)

def handle_toss(call: types.CallbackQuery):
    """Handle toss with safe game operations"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    try:
        g = safe_load_game(chat_id)
        
        if not g or g.get("state") != "toss":
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
            safe_set_batting_order(chat_id, first_batting)
        
        bot.edit_message_text(toss_text, chat_id, message_id, reply_markup=markup)
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"Error handling toss: {e}")
        bot.answer_callback_query(call.id, "❌ Error processing toss")


def handle_batting_choice(call: types.CallbackQuery):
    """Handle batting choice with safe operations"""
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    try:
        choice = call.data.split("_")[1]  # bat or bowl
        
        first_batting = "player" if choice == "bat" else "bot"
        
        choice_text = (
            f"✅ <b>You chose to {choice} first!</b>\n\n"
            f"{'🏏 Get ready to bat!' if choice == 'bat' else '🎯 Get ready to bowl!'}\n\n"
            f"Starting match..."
        )
        
        bot.edit_message_text(choice_text, chat_id, message_id)
        bot.answer_callback_query(call.id)
        
        # Start the match
        safe_set_batting_order(chat_id, first_batting)
        
    except Exception as e:
        logger.error(f"Error handling batting choice: {e}")
        bot.answer_callback_query(call.id, "❌ Error processing choice")



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
                enhanced_process_ball(message.chat.id, number)
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
            enhanced_process_ball(message.chat.id, number)
            return
        
        # Handle quick commands through text
        text_lower = text.lower()
        
        if text_lower in ["score", "📊 score"]:
            g = safe_load_game(message.chat.id)
            if g and g["state"] == "play":
                show_live_score(message.chat.id, g)
            else:
                bot.reply_to(message, "❌ No active match. Start one with /play")
        
        elif text_lower in ["forfeit", "🏳️ forfeit", "quit"]:
            g = safe_load_game(message.chat.id)
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
    initialize_tournament_system()  # ADD this line
    
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