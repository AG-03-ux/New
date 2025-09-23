# Database Migration System
import logging
from datetime import datetime, timezone
from contextlib import contextmanager
import os
import time
from contextlib import contextmanager
import sqlite3

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
                import sqlite3
                conn = sqlite3.connect(os.getenv("DB_PATH", "cricket_bot.db"), timeout=30.0)
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

logger = logging.getLogger("cricket-bot")

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

def apply_migration(version, description, migration_sql):
    """Apply a single migration"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            # Execute the migration SQL
            if isinstance(migration_sql, list):
                for sql in migration_sql:
                    cur.execute(sql)
            else:
                cur.execute(migration_sql)
            
            # Record the migration
            now = datetime.now(timezone.utc).isoformat()
            if is_postgres:
                cur.execute("""
                    INSERT INTO schema_version (version, description, applied_at)
                    VALUES (%s, %s, %s)
                """, (version, description, now))
            else:
                cur.execute("""
                    INSERT INTO schema_version (version, description, applied_at)
                    VALUES (?, ?, ?)
                """, (version, description, now))
            
            logger.info(f"Applied migration {version}: {description}")
            
    except Exception as e:
        logger.error(f"Error applying migration {version}: {e}")
        raise

def migration_v1_add_indexes():
    """Migration 1: Add database indexes for performance"""
    is_postgres = bool(os.getenv("DATABASE_URL"))
    
    migrations = [
        "CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)",
        "CREATE INDEX IF NOT EXISTS idx_stats_wins ON stats(wins DESC)",
        "CREATE INDEX IF NOT EXISTS idx_stats_high_score ON stats(high_score DESC)",
        "CREATE INDEX IF NOT EXISTS idx_games_state ON games(state)",
        "CREATE INDEX IF NOT EXISTS idx_history_chat_id ON history(chat_id)",
        "CREATE INDEX IF NOT EXISTS idx_tournaments_status ON tournaments(status)",
        "CREATE INDEX IF NOT EXISTS idx_tournament_participants_tournament_id ON tournament_participants(tournament_id)",
        "CREATE INDEX IF NOT EXISTS idx_daily_challenges_expires_at ON daily_challenges(expires_at)",
        "CREATE INDEX IF NOT EXISTS idx_user_challenges_user_id ON user_challenges(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_match_history_user_id ON match_history(user_id)"
    ]
    
    return migrations

def migration_v2_add_user_preferences():
    """Migration 2: Add user preferences table"""
    is_postgres = bool(os.getenv("DATABASE_URL"))
    
    if is_postgres:
        bigint_type = "BIGINT"
        fk_constraint = "FOREIGN KEY (user_id) REFERENCES users (user_id)"
    else:
        bigint_type = "INTEGER"
        # SQLite foreign keys need to be enabled
        fk_constraint = ""  # Handle separately
    
    migrations = [
        f"""
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id {bigint_type} PRIMARY KEY,
            preferred_format TEXT DEFAULT 'T2',
            preferred_difficulty TEXT DEFAULT 'medium',
            notifications_enabled INTEGER DEFAULT 1,  -- Use INTEGER for SQLite boolean
            sound_effects INTEGER DEFAULT 1,
            auto_play INTEGER DEFAULT 0,
            theme TEXT DEFAULT 'default',
            language TEXT DEFAULT 'en',
            timezone TEXT DEFAULT 'UTC',
            created_at TEXT,
            updated_at TEXT
            {f', {fk_constraint}' if fk_constraint else ''}
        )
        """
    ]
    return migrations

def migration_v3_add_achievements():
    """Migration 3: Add achievements system"""
    is_postgres = bool(os.getenv("DATABASE_URL"))
    
    if is_postgres:
        bigint_type = "BIGINT"
        timestamp_type = "TIMESTAMP"
        bool_type = "BOOLEAN"
        
        migrations = [
            """
            CREATE TABLE IF NOT EXISTS achievements (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                category TEXT,
                icon TEXT,
                points INTEGER DEFAULT 0,
                requirement_type TEXT,
                requirement_value INTEGER,
                is_hidden BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS user_achievements (
                user_id {bigint_type},
                achievement_id INTEGER,
                unlocked_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                progress INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, achievement_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
                FOREIGN KEY (achievement_id) REFERENCES achievements (id) ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_user_achievements_user_id ON user_achievements(user_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_achievements_category ON achievements(category)
            """,
            # Add default achievements with conflict handling
            """
            INSERT INTO achievements (name, description, category, icon, points, requirement_type, requirement_value) 
            VALUES 
                ('First Victory', 'Win your first match', 'basic', '🏆', 10, 'wins', 1),
                ('Century Maker', 'Score 100+ runs in a single match', 'batting', '💯', 25, 'high_score', 100),
                ('Consistent Player', 'Win 5 matches in a row', 'streak', '🔥', 50, 'win_streak', 5),
                ('Big Hitter', 'Hit 50 sixes total', 'batting', '🚀', 30, 'sixes_hit', 50),
                ('Experienced Player', 'Play 10 matches', 'basic', '🎮', 15, 'games_played', 10),
                ('Tournament Winner', 'Win a tournament', 'tournament', '👑', 100, 'tournaments_won', 1),
                ('Perfect Over', 'Score 36 runs in an over', 'batting', '🎯', 75, 'perfect_over', 1),
                ('Hat-trick Hero', 'Take 3 wickets in 3 balls', 'bowling', '🎩', 50, 'hat_tricks', 1)
            ON CONFLICT (name) DO NOTHING
            """
        ]
    else:
        # SQLite
        bigint_type = "INTEGER"
        timestamp_type = "TEXT"
        bool_type = "INTEGER"
        
        migrations = [
            """
            CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                category TEXT,
                icon TEXT,
                points INTEGER DEFAULT 0,
                requirement_type TEXT,
                requirement_value INTEGER,
                is_hidden INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS user_achievements (
                user_id {bigint_type},
                achievement_id INTEGER,
                unlocked_at {timestamp_type} DEFAULT CURRENT_TIMESTAMP,
                progress INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, achievement_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
                FOREIGN KEY (achievement_id) REFERENCES achievements (id) ON DELETE CASCADE
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_user_achievements_user_id ON user_achievements(user_id)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_achievements_category ON achievements(category)
            """,
            # SQLite version with INSERT OR IGNORE
            """
            INSERT OR IGNORE INTO achievements (name, description, category, icon, points, requirement_type, requirement_value) 
            VALUES 
                ('First Victory', 'Win your first match', 'basic', '🏆', 10, 'wins', 1),
                ('Century Maker', 'Score 100+ runs in a single match', 'batting', '💯', 25, 'high_score', 100),
                ('Consistent Player', 'Win 5 matches in a row', 'streak', '🔥', 50, 'win_streak', 5),
                ('Big Hitter', 'Hit 50 sixes total', 'batting', '🚀', 30, 'sixes_hit', 50),
                ('Experienced Player', 'Play 10 matches', 'basic', '🎮', 15, 'games_played', 10),
                ('Tournament Winner', 'Win a tournament', 'tournament', '👑', 100, 'tournaments_won', 1),
                ('Perfect Over', 'Score 36 runs in an over', 'batting', '🎯', 75, 'perfect_over', 1),
                ('Hat-trick Hero', 'Take 3 wickets in 3 balls', 'bowling', '🎩', 50, 'hat_tricks', 1)
            """
        ]
    
    return migrations

def migration_v4_add_session_storage():
    """Migration 4: Add persistent session storage"""
    is_postgres = bool(os.getenv("DATABASE_URL"))
    
    if is_postgres:
        migrations = [
            """
            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id BIGINT PRIMARY KEY,
                session_data TEXT,
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_user_sessions_expires_at ON user_sessions(expires_at)
            """
        ]
    else:
        # SQLite version without foreign key in CREATE TABLE
        migrations = [
            """
            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id INTEGER PRIMARY KEY,
                session_data TEXT,
                expires_at TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_user_sessions_expires_at ON user_sessions(expires_at)
            """
        ]
    
    return migrations

# Migration registry - add new migrations here
MIGRATIONS = {
    1: ("Add database indexes for performance", migration_v1_add_indexes),
    2: ("Add user preferences system", migration_v2_add_user_preferences), 
    3: ("Add achievements system", migration_v3_add_achievements),
    4: ("Add persistent session storage", migration_v4_add_session_storage),
}

def migrate_database():
    """Run all pending database migrations"""
    try:
        logger.info("Starting database migration check...")
        
        # Ensure schema_version table exists
        create_schema_version_table()
        
        current_version = get_db_version()
        target_version = max(MIGRATIONS.keys()) if MIGRATIONS else 0
        
        logger.info(f"Current database version: {current_version}")
        logger.info(f"Target database version: {target_version}")
        
        if current_version >= target_version:
            logger.info("Database is up to date")
            return
        
        # Apply pending migrations
        for version in range(current_version + 1, target_version + 1):
            if version in MIGRATIONS:
                description, migration_func = MIGRATIONS[version]
                logger.info(f"Applying migration {version}: {description}")
                
                migration_sql = migration_func()
                apply_migration(version, description, migration_sql)
                
                logger.info(f"Migration {version} completed successfully")
            else:
                logger.warning(f"Migration {version} not found, skipping")
        
        logger.info(f"Database migration completed. New version: {get_db_version()}")
        
    except Exception as e:
        logger.error(f"Database migration failed: {e}", exc_info=True)
        raise

def rollback_migration(target_version):
    """Rollback to a specific version (use with caution!)"""
    current_version = get_db_version()
    
    if target_version >= current_version:
        logger.info("No rollback needed")
        return
    
    logger.warning(f"Rolling back from version {current_version} to {target_version}")
    logger.warning("This operation may cause data loss!")
    
    # This is a simplified rollback - in production you'd want
    # to implement proper rollback scripts for each migration
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            if is_postgres:
                cur.execute("""
                    DELETE FROM schema_version 
                    WHERE version > %s
                """, (target_version,))
            else:
                cur.execute("""
                    DELETE FROM schema_version 
                    WHERE version > ?
                """, (target_version,))
                
        logger.info(f"Rollback completed to version {target_version}")
        
    except Exception as e:
        logger.error(f"Rollback failed: {e}")
        raise

# Helper function to safely add columns to existing tables (for SQLite)
def safe_add_column(table_name, column_definition):
    """Safely add a column to a table (SQLite compatible)"""
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            is_postgres = bool(os.getenv("DATABASE_URL"))
            
            if is_postgres:
                cur.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_definition}")
            else:
                # For SQLite, check if column exists first
                cur.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cur.fetchall()]
                column_name = column_definition.split()[0]
                
                if column_name not in columns:
                    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}")
                    logger.info(f"Added column {column_name} to {table_name}")
                else:
                    logger.info(f"Column {column_name} already exists in {table_name}")
                    
    except Exception as e:
        logger.error(f"Error adding column to {table_name}: {e}")
        raise