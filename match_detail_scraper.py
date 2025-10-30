import sqlite3
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path
import logging
import json
from collections import defaultdict
from asyncio import Semaphore, PriorityQueue
import time


class MatchDetailScraper:
    """
    Fetches detailed match information (expanded odds, player props, etc.)
    using a priority queue system for efficient processing.
    
    Priority Levels:
    1. Live matches (highest priority)
    2. Starting within 1 hour
    3. Starting within 6 hours
    4. Starting within 24 hours
    5. All others (lowest priority)
    """

    def __init__(self, data_dir: str = "ballybet_data", update_interval: int = 30):
        self.base_url = "https://eu1.offering-api.kambicdn.com/offering/v2018/bcsustn"
        self.data_dir = Path(data_dir)
        self.update_interval = update_interval
        
        # Concurrency settings
        self.max_concurrent = 20  # Can handle more for detail fetching
        self.request_delay = 0.05
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        # Cache settings (in-memory cache)
        self.cache = {}  # {match_id: (data, timestamp)}
        self.cache_ttl = {
            'live': 30,      # 30s for live
            'soon': 60,      # 1 min for starting soon
            'today': 300,    # 5 min for today
            'future': 600    # 10 min for future
        }
        
        # Priority queue
        self.match_queue = PriorityQueue()
        
        # Setup
        (self.data_dir / "logs").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "db").mkdir(exist_ok=True)
        self.setup_logging()
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        
        # Database paths
        self.pregame_db = self.data_dir / "db" / "ballybet.db"
        self.live_db = self.data_dir / "db" / "ballybet_live.db"
        self.detail_db = self.data_dir / "db" / "ballybet_details.db"
        
        self.init_database()

    def setup_logging(self):
        log_file = self.data_dir / "logs" / "detail_scraper.log"
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

    def init_database(self):
        """Initialize database for detailed odds."""
        with sqlite3.connect(self.detail_db) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")
            
            # Detailed odds table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS detailed_odds (
                    detail_id TEXT PRIMARY KEY,
                    game_id TEXT,
                    match_id INTEGER,
                    bet_type TEXT,
                    bet_subtype TEXT,
                    designation TEXT,
                    player_name TEXT,
                    price REAL,
                    points REAL,
                    status TEXT,
                    scraped_at TEXT,
                    priority INTEGER,
                    FOREIGN KEY (game_id) REFERENCES games(game_id)
                )
            """)
            
            # Track last fetch time for each match
            conn.execute("""
                CREATE TABLE IF NOT EXISTS match_fetch_log (
                    match_id INTEGER PRIMARY KEY,
                    game_id TEXT,
                    last_fetched TEXT,
                    fetch_count INTEGER DEFAULT 0,
                    priority_level INTEGER
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_detailed_game ON detailed_odds(game_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_detailed_match ON detailed_odds(match_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fetch_log ON match_fetch_log(last_fetched)")
            
            conn.commit()
        
        self.logger.info(f"Detail database initialized at {self.detail_db}")

    def get_priority_and_ttl(self, start_time: str, is_live: bool = False) -> Tuple[int, int]:
        """
        Determine priority level and cache TTL based on start time.
        
        Priority: 1 (highest) to 5 (lowest)
        Returns: (priority, ttl_seconds)
        """
        if is_live:
            return (1, self.cache_ttl['live'])
        
        try:
            start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            time_until = (start_dt - now).total_seconds()
            
            if time_until < 0:  # Already started (shouldn't happen but handle it)
                return (1, self.cache_ttl['live'])
            elif time_until < 3600:  # Within 1 hour
                return (2, self.cache_ttl['soon'])
            elif time_until < 21600:  # Within 6 hours
                return (3, self.cache_ttl['today'])
            elif time_until < 86400:  # Within 24 hours
                return (4, self.cache_ttl['today'])
            else:  # Future
                return (5, self.cache_ttl['future'])
        except:
            return (5, self.cache_ttl['future'])

    def is_cache_valid(self, match_id: int, priority: int) -> bool:
        """Check if cached data is still valid."""
        if match_id not in self.cache:
            return False
        
        data, timestamp, cached_priority = self.cache[match_id]
        age = time.time() - timestamp
        
        # Determine appropriate TTL
        ttl_map = {1: 30, 2: 60, 3: 300, 4: 300, 5: 600}
        ttl = ttl_map.get(priority, 300)
        
        return age < ttl

    async def populate_queue_from_databases(self):
        """Populate priority queue with matches from pregame and live databases."""
        matches = []
        now = datetime.now(timezone.utc)
        
        # Get pregame matches
        if self.pregame_db.exists():
            with sqlite3.connect(self.pregame_db) as conn:
                cursor = conn.execute("""
                    SELECT game_id, match_id, start_time 
                    FROM games 
                    WHERE match_id IS NOT NULL
                """)
                for row in cursor.fetchall():
                    game_id, match_id, start_time = row
                    priority, ttl = self.get_priority_and_ttl(start_time, is_live=False)
                    
                    # Only queue high/medium priority matches
                    if priority <= 4:
                        matches.append((priority, match_id, game_id, start_time, False))
        
        # Get live matches (highest priority)
        if self.live_db.exists():
            with sqlite3.connect(self.live_db) as conn:
                cursor = conn.execute("""
                    SELECT game_id, match_id, start_time 
                    FROM live_games 
                    WHERE match_id IS NOT NULL
                """)
                for row in cursor.fetchall():
                    game_id, match_id, start_time = row
                    matches.append((1, match_id, game_id, start_time, True))
        
        self.logger.info(f"Found {len(matches)} matches to queue")
        
        # Add to priority queue
        for priority, match_id, game_id, start_time, is_live in matches:
            # Skip if cache is valid
            if self.is_cache_valid(match_id, priority):
                continue
            
            await self.match_queue.put((priority, match_id, game_id, start_time, is_live))

    async def fetch_match_detail(self, session: aiohttp.ClientSession, 
                                 match_id: int, semaphore: Semaphore) -> Optional[Dict]:
        """Fetch detailed match information with retry logic."""
        async with semaphore:
            await asyncio.sleep(self.request_delay)
            
            url = f"{self.base_url}/betoffer/event/{match_id}.json"
            params = {"lang": "en_US", "market": "US-TN"}
            
            for attempt in range(3):
                try:
                    async with session.get(url, params=params, headers=self.headers) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 404:
                            self.logger.warning(f"Match {match_id} not found (404)")
                            return None
                        elif response.status == 429:
                            wait = 2 ** attempt
                            self.logger.warning(f"Rate limited, waiting {wait}s")
                            await asyncio.sleep(wait)
                        else:
                            self.logger.warning(f"HTTP {response.status} for match {match_id}")
                            return None
                except asyncio.TimeoutError:
                    if attempt < 2:
                        self.logger.warning(f"Timeout on match {match_id}, attempt {attempt + 1}")
                        await asyncio.sleep(1)
                    else:
                        self.logger.error(f"Timeout after 3 attempts: match {match_id}")
                        return None
                except Exception as e:
                    self.logger.error(f"Error fetching match {match_id}: {e}")
                    return None
            
            return None

    def process_detailed_odds(self, data: Dict, match_id: int, game_id: str, 
                             priority: int) -> List[Dict]:
        """Extract all detailed odds from match detail response."""
        odds_list = []
        scraped_at = datetime.now(timezone.utc).isoformat()
        
        bet_offers = data.get("betOffers", [])
        event = data.get("events", [{}])[0] if data.get("events") else {}
        
        for offer in bet_offers:
            bet_type_info = offer.get("betOfferType", {})
            bet_type = bet_type_info.get("name", "unknown")
            outcomes = offer.get("outcomes", [])
            
            # Check if it's a player prop
            criterion = offer.get("criterion", {})
            player_name = criterion.get("participant") if criterion else None
            
            for outcome in outcomes:
                designation = outcome.get("label", "")
                odds_value = outcome.get("odds", 0) / 1000.0 if "odds" in outcome else None
                line_value = outcome.get("line", 0) / 1000.0 if "line" in outcome else None
                status = outcome.get("status", "OPEN")
                
                # Generate unique ID for this detailed odd
                detail_id = f"{match_id}:{bet_type}:{designation}:{player_name or 'team'}"
                if line_value:
                    detail_id += f":{line_value}"
                
                odds_list.append({
                    "detail_id": detail_id.replace(" ", "_").lower(),
                    "game_id": game_id,
                    "match_id": match_id,
                    "bet_type": bet_type,
                    "bet_subtype": criterion.get("label") if criterion else None,
                    "designation": designation,
                    "player_name": player_name,
                    "price": odds_value,
                    "points": line_value,
                    "status": status,
                    "scraped_at": scraped_at,
                    "priority": priority
                })
        
        return odds_list

    def save_detailed_odds(self, all_odds: List[Dict], fetch_log: List[Tuple]):
        """Save detailed odds and update fetch log."""
        if not all_odds:
            return
        
        with sqlite3.connect(self.detail_db) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            
            # Insert detailed odds
            cursor.executemany("""
                INSERT OR REPLACE INTO detailed_odds 
                VALUES (:detail_id, :game_id, :match_id, :bet_type, :bet_subtype, 
                        :designation, :player_name, :price, :points, :status, 
                        :scraped_at, :priority)
            """, all_odds)
            
            # Update fetch log
            cursor.executemany("""
                INSERT INTO match_fetch_log (match_id, game_id, last_fetched, fetch_count, priority_level)
                VALUES (?, ?, ?, 1, ?)
                ON CONFLICT(match_id) DO UPDATE SET
                    last_fetched = excluded.last_fetched,
                    fetch_count = fetch_count + 1,
                    priority_level = excluded.priority_level
            """, fetch_log)
            
            conn.commit()

    async def process_batch(self, batch: List[Tuple]) -> Tuple[int, int]:
        """Process a batch of matches concurrently."""
        semaphore = Semaphore(self.max_concurrent)
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent + 5,
            limit_per_host=10,
            ttl_dns_cache=300
        )
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout
        ) as session:
            tasks = []
            for priority, match_id, game_id, start_time, is_live in batch:
                tasks.append(self.fetch_match_detail(session, match_id, semaphore))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        all_odds = []
        fetch_log = []
        success_count = 0
        now_iso = datetime.now(timezone.utc).isoformat()
        
        for i, result in enumerate(results):
            if isinstance(result, Exception) or result is None:
                continue
            
            priority, match_id, game_id, start_time, is_live = batch[i]
            
            # Process odds
            odds = self.process_detailed_odds(result, match_id, game_id, priority)
            all_odds.extend(odds)
            
            # Update cache
            self.cache[match_id] = (result, time.time(), priority)
            
            # Log fetch
            fetch_log.append((match_id, game_id, now_iso, priority))
            success_count += 1
        
        # Save to database
        if all_odds:
            self.save_detailed_odds(all_odds, fetch_log)
        
        return success_count, len(all_odds)

    async def run_cycle(self):
        """Single update cycle."""
        cycle_start = time.time()
        
        # Populate queue
        queue_start = time.time()
        await self.populate_queue_from_databases()
        queue_size = self.match_queue.qsize()
        queue_time = time.time() - queue_start
        
        if queue_size == 0:
            self.logger.info("No matches need updating (all cached)")
            return
        
        self.logger.info(f"Processing {queue_size} matches from queue")
        
        # Process in batches
        batch_size = 30
        total_success = 0
        total_odds = 0
        batches_processed = 0
        
        fetch_start = time.time()
        
        while not self.match_queue.empty():
            batch = []
            for _ in range(min(batch_size, self.match_queue.qsize())):
                if not self.match_queue.empty():
                    item = await self.match_queue.get()
                    batch.append(item)
            
            if batch:
                success, odds_count = await self.process_batch(batch)
                total_success += success
                total_odds += odds_count
                batches_processed += 1
        
        fetch_time = time.time() - fetch_start
        total_time = time.time() - cycle_start
        
        self.logger.info(
            f"✅ Processed {total_success}/{queue_size} matches, "
            f"{total_odds} detailed odds | "
            f"Queue: {queue_time:.2f}s | Fetch: {fetch_time:.2f}s | "
            f"Total: {total_time:.2f}s"
        )

    def run_continuous(self):
        """Run continuous update loop."""
        self.logger.info(
            f"🚀 Starting Match Detail Scraper | "
            f"Interval: {self.update_interval}s | "
            f"Concurrent: {self.max_concurrent}"
        )
        
        try:
            while True:
                loop_start = time.time()
                
                asyncio.run(self.run_cycle())
                
                elapsed = time.time() - loop_start
                sleep_time = max(0, self.update_interval - elapsed)
                
                if sleep_time > 0:
                    self.logger.info(f"💤 Sleeping {sleep_time:.1f}s until next cycle...")
                    time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            self.logger.info("⏹️  Detail scraper stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    scraper = MatchDetailScraper(
        data_dir="ballybet_data",
        update_interval=30  # 30 second cycles
    )
    scraper.run_continuous()