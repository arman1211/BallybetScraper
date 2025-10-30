import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
import logging
from typing import Set, Dict, List, Tuple
import threading


class GameStatusUpdater:
    """
    Fast status updater that monitors both pregame and live scrapers.
    Updates pregame games to 'starting_soon' and tracks finished live games.
    """

    def __init__(
        self,
        pregame_db: str = "ballybet_data/db/ballybet.db",
        live_db: str = "ballybet_data/db/ballybet_live.db",
        status_db: str = "ballybet_data/db/game_status.db",
        update_interval: int = 5,
        starting_soon_minutes: int = 30
    ):
        self.pregame_db_path = Path(pregame_db)
        self.live_db_path = Path(live_db)
        self.status_db_path = Path(status_db)
        self.update_interval = update_interval
        self.starting_soon_threshold = timedelta(minutes=starting_soon_minutes)
        
        # Ensure status db directory exists
        self.status_db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.setup_logging()
        self.init_status_database()
        
        # Cache for tracking live game IDs
        self.current_live_games: Set[str] = set()
        self.previous_live_games: Set[str] = set()
        
        self.running = False
        self.lock = threading.Lock()

    def setup_logging(self):
        """Setup dedicated logger for status updater."""
        log_dir = Path("ballybet_data/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = log_dir / "status_updater.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ],
            encoding='utf-8'
        )
        self.logger = logging.getLogger(__name__)

    def init_status_database(self):
        """Initialize status tracking database."""
        with sqlite3.connect(self.status_db_path) as conn:
            # Track status changes
            conn.execute("""
                CREATE TABLE IF NOT EXISTS game_status_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id TEXT NOT NULL,
                    old_status TEXT,
                    new_status TEXT NOT NULL,
                    changed_at TEXT NOT NULL,
                    source TEXT NOT NULL
                )
            """)
            
            # Track finished games
            conn.execute("""
                CREATE TABLE IF NOT EXISTS finished_games (
                    game_id TEXT PRIMARY KEY,
                    sport_name TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    start_time TEXT,
                    finished_at TEXT,
                    last_seen_live TEXT
                )
            """)
            
            # Create indexes for fast queries
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_status_game_id 
                ON game_status_history(game_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_status_changed_at 
                ON game_status_history(changed_at)
            """)
            
            conn.commit()
        
        self.logger.info(f"Status database initialized at {self.status_db_path}")

    def get_pregame_games_starting_soon(self) -> List[Tuple[str, str, str]]:
        """
        Fast query to get pregame games starting within threshold.
        Returns: List of (game_id, current_status, start_time)
        """
        if not self.pregame_db_path.exists():
            return []
        
        now = datetime.now(timezone.utc)
        threshold_time = now + self.starting_soon_threshold
        
        with sqlite3.connect(self.pregame_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT game_id, status, start_time
                FROM games
                WHERE status != 'starting_soon'
                AND datetime(start_time) <= datetime(?)
                AND datetime(start_time) > datetime(?)
            """, (threshold_time.isoformat(), now.isoformat()))
            
            return cursor.fetchall()

    def get_current_live_games(self) -> Dict[str, Dict]:
        """
        Fast query to get all current live games.
        Returns: Dict[game_id, game_info]
        """
        if not self.live_db_path.exists():
            return {}
        
        with sqlite3.connect(self.live_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT game_id, sport_name, home_team, away_team, 
                       start_time, scraped_at
                FROM live_games
                ORDER BY scraped_at DESC
            """)
            
            games = {}
            for row in cursor.fetchall():
                games[row[0]] = {
                    'game_id': row[0],
                    'sport_name': row[1],
                    'home_team': row[2],
                    'away_team': row[3],
                    'start_time': row[4],
                    'scraped_at': row[5]
                }
            
            return games

    def update_pregame_status(self, updates: List[Tuple[str, str, str]]) -> int:
        """
        Batch update pregame statuses and log changes.
        Returns: Number of updated games
        """
        if not updates:
            return 0
        
        updated_count = 0
        now_iso = datetime.now(timezone.utc).isoformat()
        
        with sqlite3.connect(self.pregame_db_path) as pregame_conn, \
             sqlite3.connect(self.status_db_path) as status_conn:
            
            pregame_cursor = pregame_conn.cursor()
            status_cursor = status_conn.cursor()
            
            for game_id, old_status, start_time in updates:
                # Update pregame database
                pregame_cursor.execute("""
                    UPDATE games 
                    SET status = 'starting_soon'
                    WHERE game_id = ?
                """, (game_id,))
                
                # Log status change
                status_cursor.execute("""
                    INSERT INTO game_status_history 
                    (game_id, old_status, new_status, changed_at, source)
                    VALUES (?, ?, ?, ?, ?)
                """, (game_id, old_status, 'starting_soon', now_iso, 'pregame_updater'))
                
                updated_count += 1
            
            pregame_conn.commit()
            status_conn.commit()
        
        return updated_count

    def detect_finished_games(self) -> List[Dict]:
        """
        Detect games that were live but are no longer in live feed.
        Returns: List of finished game info
        """
        with self.lock:
            finished = self.previous_live_games - self.current_live_games
        
        if not finished:
            return []
        
        finished_games = []
        now_iso = datetime.now(timezone.utc).isoformat()
        
        # Get full info for finished games from last known live state
        with sqlite3.connect(self.live_db_path) as conn:
            cursor = conn.cursor()
            placeholders = ','.join('?' * len(finished))
            cursor.execute(f"""
                SELECT game_id, sport_name, home_team, away_team, 
                       start_time, scraped_at
                FROM live_games
                WHERE game_id IN ({placeholders})
            """, tuple(finished))
            
            for row in cursor.fetchall():
                finished_games.append({
                    'game_id': row[0],
                    'sport_name': row[1],
                    'home_team': row[2],
                    'away_team': row[3],
                    'start_time': row[4],
                    'last_seen_live': row[5],
                    'finished_at': now_iso
                })
        
        return finished_games

    def record_finished_games(self, finished_games: List[Dict]) -> int:
        """
        Record finished games and log status changes.
        Returns: Number of recorded games
        """
        if not finished_games:
            return 0
        
        with sqlite3.connect(self.status_db_path) as conn:
            cursor = conn.cursor()
            
            for game in finished_games:
                # Record finished game
                cursor.execute("""
                    INSERT OR REPLACE INTO finished_games
                    (game_id, sport_name, home_team, away_team, 
                     start_time, finished_at, last_seen_live)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    game['game_id'], game['sport_name'], 
                    game['home_team'], game['away_team'],
                    game['start_time'], game['finished_at'], 
                    game['last_seen_live']
                ))
                
                # Log status change
                cursor.execute("""
                    INSERT INTO game_status_history
                    (game_id, old_status, new_status, changed_at, source)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    game['game_id'], 'live', 'finished', 
                    game['finished_at'], 'live_monitor'
                ))
            
            conn.commit()
        
        return len(finished_games)

    def update_cycle(self):
        """Single update cycle - optimized for speed."""
        cycle_start = time.time()
        
        try:
            # Phase 1: Update pregame games (parallel-safe)
            games_to_update = self.get_pregame_games_starting_soon()
            pregame_updated = self.update_pregame_status(games_to_update)
            
            # Phase 2: Track live games
            current_live = self.get_current_live_games()
            
            with self.lock:
                self.previous_live_games = self.current_live_games.copy()
                self.current_live_games = set(current_live.keys())
            
            # Phase 3: Detect and record finished games
            finished_games = self.detect_finished_games()
            finished_count = self.record_finished_games(finished_games)
            
            # Log summary if changes occurred
            if pregame_updated > 0 or finished_count > 0:
                self.logger.info(
                    f"✅ Update cycle: {pregame_updated} starting soon, "
                    f"{finished_count} finished, "
                    f"{len(current_live)} currently live"
                )
            
            cycle_time = time.time() - cycle_start
            if cycle_time > self.update_interval:
                self.logger.warning(
                    f"⚠️ Update cycle took {cycle_time:.2f}s "
                    f"(target: {self.update_interval}s)"
                )
        
        except Exception as e:
            self.logger.error(f"Error in update cycle: {e}", exc_info=True)

    def run(self):
        """Main run loop."""
        self.logger.info(
            f"🚀 Starting Game Status Updater "
            f"(update interval: {self.update_interval}s, "
            f"starting soon threshold: {self.starting_soon_threshold.seconds // 60}min)"
        )
        
        self.running = True
        
        try:
            while self.running:
                cycle_start = time.time()
                
                self.update_cycle()
                
                # Sleep for remaining time to maintain interval
                elapsed = time.time() - cycle_start
                sleep_time = max(0, self.update_interval - elapsed)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            self.logger.info("⏹️ Status updater stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error in main loop: {e}", exc_info=True)
        finally:
            self.running = False

    def stop(self):
        """Gracefully stop the updater."""
        self.running = False


def run_status_updater():
    """Entry point for running as separate worker."""
    updater = GameStatusUpdater(
        update_interval=5,  # 5 second updates
        starting_soon_minutes=30
    )
    updater.run()


if __name__ == "__main__":
    run_status_updater()