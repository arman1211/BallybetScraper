import requests
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
import re
import time
from pathlib import Path
import logging
import asyncio
import aiohttp
import sqlite3
from concurrent.futures import ThreadPoolExecutor

class OptimizedLiveBallyBetScraper:
    """
    High-speed live scraper optimized for sub-6 second updates.
    
    Key optimizations:
    1. Async HTTP requests for parallel fetching
    2. Connection pooling for reduced latency
    3. Batch database writes with WAL mode
    4. Minimal JSON file writes (only on-demand)
    5. In-memory caching to reduce DB reads
    """

    def __init__(self, data_dir: str = "ballybet_data", poll_interval: int = 6):
        self.SPORTS_TO_SCRAPE = [
            "football", "basketball", "tennis",
            "cricket", "ice_hockey", "esports",
            "american_football", "baseball"
        ]
        
        self.base_url = "https://eu1.offering-api.kambicdn.com/offering/v2018/bcsustn"
        self.data_dir = Path(data_dir)
        self.poll_interval_seconds = poll_interval
        
        # More realistic timeout settings
        self.timeout = aiohttp.ClientTimeout(total=25, connect=8)
        self.max_concurrent = 8  # Limit concurrent requests
        self.request_delay = 0.1  # 100ms between requests
        
        # Setup directories
        self.live_data_dir = self.data_dir / "json_live"
        self.live_data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "logs").mkdir(exist_ok=True)
        (self.data_dir / "db").mkdir(exist_ok=True)

        self.setup_logging()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }

        # Database setup with optimizations
        self.db_path = self.data_dir / "db" / "ballybet_live.db"
        self.init_database()
        
        # In-memory cache for tracking changes
        self.current_live_games = {}
        self.save_json_counter = 0
        self.json_save_interval = 12  # Save JSON every 12 cycles (1 min if 5s interval)

    def setup_logging(self):
        log_file = self.data_dir / "logs" / "live_scraper_optimized.log"
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
        """Initialize DB with performance optimizations."""
        with sqlite3.connect(self.db_path) as conn:
            # Enable WAL mode for concurrent reads/writes
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
            conn.execute("PRAGMA temp_store=MEMORY")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS live_games (
                    game_id TEXT PRIMARY KEY,
                    match_id INTEGER,
                    sport_name TEXT,
                    league_name TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    start_time TEXT,
                    scraped_at TEXT,
                    status TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS live_scores (
                    game_id TEXT,
                    scraped_at TEXT,
                    home_score INTEGER,
                    away_score INTEGER,
                    last_scorer TEXT,
                    score_version INTEGER,
                    period TEXT,
                    minute INTEGER,
                    second INTEGER,
                    clock_running BOOLEAN,
                    PRIMARY KEY (game_id, scraped_at),
                    FOREIGN KEY (game_id) REFERENCES live_games(game_id)
                )
            """)
            
            # Live statistics table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS live_statistics (
                    game_id TEXT,
                    scraped_at TEXT,
                    stat_type TEXT,
                    home_value INTEGER,
                    away_value INTEGER,
                    PRIMARY KEY (game_id, scraped_at, stat_type),
                    FOREIGN KEY (game_id) REFERENCES live_games(game_id)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS live_odds (
                    game_id TEXT,
                    line_id TEXT,
                    bet_type TEXT,
                    designation TEXT,
                    price REAL,
                    points REAL,
                    status TEXT,
                    scraped_at TEXT,
                    PRIMARY KEY (game_id, line_id),
                    FOREIGN KEY (game_id) REFERENCES live_games(game_id)
                )
            """)
            
            # Create indexes for fast queries
            conn.execute("CREATE INDEX IF NOT EXISTS idx_scores_game_time ON live_scores(game_id, scraped_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_stats_game_time ON live_statistics(game_id, scraped_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_odds_game_time ON live_odds(game_id, scraped_at)")
            
            conn.commit()
            self.logger.info(f"Optimized live database initialized at {self.db_path}")

    async def fetch_sport_async(self, session: aiohttp.ClientSession, sport: str, 
                                semaphore: asyncio.Semaphore) -> Tuple[str, Optional[List[Dict]]]:
        """Async fetch for a single sport with retry and rate limiting."""
        async with semaphore:
            # Pace requests
            await asyncio.sleep(self.request_delay)
            
            url = f"{self.base_url}/listView/{sport}/all/all/all/in-play.json"
            params = {"lang": "en_US", "market": "US-TN", "useCombined": "true"}
            
            # Retry logic
            for attempt in range(3):
                try:
                    async with session.get(url, params=params, headers=self.headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            return sport, data.get("events", [])
                        elif response.status == 429:  # Rate limit
                            wait = 2 ** attempt
                            self.logger.warning(f"Rate limited on {sport}, waiting {wait}s")
                            await asyncio.sleep(wait)
                        else:
                            self.logger.warning(f"HTTP {response.status} for {sport}")
                            return sport, None
                except asyncio.TimeoutError:
                    if attempt < 2:
                        self.logger.warning(f"Timeout on {sport}, attempt {attempt + 1}")
                        await asyncio.sleep(1)
                    else:
                        self.logger.error(f"Timeout after 3 attempts: {sport}")
                        return sport, None
                except Exception as e:
                    self.logger.error(f"Error fetching {sport}: {e}")
                    return sport, None
            
            return sport, None

    async def scrape_all_sports_async(self) -> List[Dict]:
        """Fetch all sports concurrently with controlled rate."""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent + 3,
            limit_per_host=5,
            ttl_dns_cache=300,
            force_close=False
        )
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout
        ) as session:
            tasks = [
                self.fetch_sport_async(session, sport, semaphore)
                for sport in self.SPORTS_TO_SCRAPE
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_events = []
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Task failed: {result}")
                continue
            sport, events = result
            if events:
                all_events.extend(events)
        
        return all_events

    # ID generation methods (same as before)
    def generate_game_id(self, home_team: str, away_team: str, start_time: str) -> str:
        def clean_name(name: str) -> str:
            name = re.sub(r"[^\w\s]", "", name)
            name = re.sub(r"\s+", "_", name.strip()).lower()
            return name
        home_clean, away_clean = clean_name(home_team), clean_name(away_team)
        try:
            date_str = datetime.fromisoformat(start_time.replace("Z", "+00:00")).strftime("%Y%m%d_%H%M")
        except: 
            date_str = "unknown"
        return f"{home_clean}:{away_clean}:{date_str}"

    def clean_team_name(self, name: str) -> str:
        name = re.sub(r"\s+(FC|CF|United|City)$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(r"\s+", "_", name.strip()).lower()
        return name

    def generate_stable_line_id(self, home_team: str, away_team: str, bet_type: str, 
                                designation: str, points: Optional[float] = None) -> str:
        home_clean, away_clean = self.clean_team_name(home_team), self.clean_team_name(away_team)
        points_str = str(points).replace(".", "_") if points is not None else ""
        sep = ":"
        bookmaker = "ballybet_live"

        if bet_type == "moneyline":
            if designation == "home": return f"{home_clean}{sep}moneyline{sep}{bookmaker}"
            elif designation == "away": return f"{away_clean}{sep}moneyline{sep}{bookmaker}"
            else: return f"draw{sep}moneyline{sep}{bookmaker}"
        elif bet_type == "total":
            return f"game_total{sep}{designation.lower()}{sep}{points_str}{sep}{bookmaker}"
        return f"{home_clean}{sep}{away_clean}{sep}{bet_type}{sep}{designation.lower()}{sep}{points_str}{sep}{bookmaker}"

    def process_live_events(self, events: List[Dict]) -> List[Tuple[Dict, Dict, List[Dict], List[Dict]]]:
        """Process live events into structured data for efficient storage."""
        processed_data = []
        scraped_at_iso = datetime.now(timezone.utc).isoformat()
        
        for item in events:
            event, bet_offers, live_data = item.get("event"), item.get("betOffers"), item.get("liveData")
            if not all([event, live_data]):  # bet_offers can be empty for some live events
                continue

            home_team, away_team, start_time = event.get("homeName"), event.get("awayName"), event.get("start")
            if not all([home_team, away_team, start_time]): 
                continue

            game_id = self.generate_game_id(home_team, away_team, start_time)
            odds_list = self.extract_odds(bet_offers or [], home_team, away_team, game_id, scraped_at_iso)
            
            # Extract structured live data
            scores_data, stats_data = self.extract_live_data(live_data, game_id, scraped_at_iso)

            game_info = {
                "game_id": game_id, 
                "match_id": event.get("id"),
                "sport_name": event.get("sport", "").lower(), 
                "league_name": event.get("group", ""),
                "home_team": home_team, 
                "away_team": away_team, 
                "start_time": start_time,
                "scraped_at": scraped_at_iso,
                "status": "LIVE"
            }
            
            processed_data.append((game_info, scores_data, stats_data, odds_list))
            
            # Update in-memory cache
            self.current_live_games[game_id] = game_info
        
        return processed_data

    def extract_odds(self, bet_offers: List[Dict], home_team: str, away_team: str, 
                    game_id: str, scraped_at: str) -> List[Dict]:
        """Fast odds extraction."""
        odds_list = []
        
        for offer in bet_offers:
            offer_type = offer.get("betOfferType", {}).get("name")
            
            if offer_type == "Match":
                bet_type_for_id = "moneyline"
                for outcome in offer.get("outcomes", []):
                    label = outcome.get("label")
                    if label == "1": designation = "home"
                    elif label == "2": designation = "away"
                    elif label == "X": designation = "draw"
                    else: continue
                    
                    odds_list.append({
                        "line_id": self.generate_stable_line_id(home_team, away_team, bet_type_for_id, designation),
                        "game_id": game_id, 
                        "bet_type": bet_type_for_id, 
                        "designation": designation,
                        "price": outcome.get("odds", 0) / 1000.0 if "odds" in outcome else None,
                        "points": None, 
                        "status": outcome.get("status", "SUSPENDED"), 
                        "scraped_at": scraped_at
                    })
                    
            elif offer_type == "Over/Under":
                bet_type_for_id = "total"
                outcomes = offer.get("outcomes", [])
                if len(outcomes) >= 2:
                    points = outcomes[0].get("line", 0) / 1000.0
                    for outcome in outcomes[:2]:
                        designation = outcome.get("label", "").lower()
                        if designation in ["over", "under"]:
                            odds_list.append({
                                "line_id": self.generate_stable_line_id(home_team, away_team, bet_type_for_id, designation, points),
                                "game_id": game_id, 
                                "bet_type": bet_type_for_id, 
                                "designation": designation,
                                "price": outcome.get("odds", 0) / 1000.0 if "odds" in outcome else None,
                                "points": points, 
                                "status": outcome.get("status", "SUSPENDED"), 
                                "scraped_at": scraped_at
                            })
        
        return odds_list

    def extract_live_data(self, live_data: Dict, game_id: str, scraped_at: str) -> Tuple[Dict, List[Dict]]:
        """Extract structured live score and statistics data."""
        scores_data = {}
        stats_data = []
        
        # Extract score data
        if "score" in live_data:
            score = live_data["score"]
            
            # Handle tennis scores (can be "AD", "40", "30", etc.)
            def parse_score(score_str: str) -> int:
                if not score_str or score_str == "0":
                    return 0
                if score_str == "15":
                    return 15
                if score_str == "30":
                    return 30
                if score_str == "40":
                    return 40
                if score_str == "AD":
                    return 50  # Advantage comes after 40
                try:
                    return int(score_str)
                except ValueError:
                    return 0  # Default for unknown formats
            
            scores_data = {
                "game_id": game_id,
                "scraped_at": scraped_at,
                "home_score": parse_score(score.get("home", "0")),
                "away_score": parse_score(score.get("away", "0")),
                "last_scorer": score.get("who", "UNKNOWN"),
                "score_version": score.get("version", 0),
                "period": "",
                "minute": 0,
                "second": 0,
                "clock_running": False
            }
        
        # Extract clock data
        if "matchClock" in live_data:
            clock = live_data["matchClock"]
            scores_data.update({
                "period": clock.get("period", ""),
                "minute": clock.get("minute", 0),
                "second": clock.get("second", 0),
                "clock_running": clock.get("running", False)
            })
        
        # Extract statistics
        if "statistics" in live_data:
            statistics = live_data["statistics"]
            for sport_key, sport_stats in statistics.items():
                if sport_key == "football" and "home" in sport_stats and "away" in sport_stats:
                    home_stats = sport_stats["home"]
                    away_stats = sport_stats["away"]
                    
                    # Common football stats
                    stat_types = ["yellowCards", "redCards", "corners", "shots", "shotsOnTarget"]
                    for stat_type in stat_types:
                        if stat_type in home_stats and stat_type in away_stats:
                            stats_data.append({
                                "game_id": game_id,
                                "scraped_at": scraped_at,
                                "stat_type": stat_type,
                                "home_value": home_stats[stat_type],
                                "away_value": away_stats[stat_type]
                            })
        
        return scores_data, stats_data

    def save_snapshot_to_db_optimized(self, processed_data: List[Tuple[Dict, Dict, List[Dict], List[Dict]]]):
        """Optimized batch database write with structured live data."""
        if not processed_data: 
            return
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            
            # Batch insert games
            games = [game_info for game_info, _, _, _ in processed_data]
            cursor.executemany(
                "INSERT OR REPLACE INTO live_games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [(g["game_id"], g["match_id"], g["sport_name"], g["league_name"], 
                  g["home_team"], g["away_team"], g["start_time"], g["scraped_at"], g["status"]) 
                 for g in games]
            )
            
            # Batch insert live scores
            scores_data = [scores for _, scores, _, _ in processed_data if scores]
            if scores_data:
                cursor.executemany(
                    "INSERT OR REPLACE INTO live_scores VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [(s["game_id"], s["scraped_at"], s["home_score"], s["away_score"],
                      s["last_scorer"], s["score_version"], s["period"], s["minute"], 
                      s["second"], s["clock_running"]) 
                     for s in scores_data]
                )
            
            # Batch insert live statistics
            all_stats = [stat for _, _, stats_list, _ in processed_data for stat in stats_list]
            if all_stats:
                cursor.executemany(
                    "INSERT OR REPLACE INTO live_statistics VALUES (?, ?, ?, ?, ?)",
                    [(s["game_id"], s["scraped_at"], s["stat_type"], s["home_value"], s["away_value"]) 
                     for s in all_stats]
                )
            
            # Batch insert odds
            all_odds = [odd for _, _, _, odds_list in processed_data for odd in odds_list]
            if all_odds:
                cursor.executemany(
                    "INSERT OR REPLACE INTO live_odds VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [(o["game_id"], o["line_id"], o["bet_type"], o["designation"],
                      o["price"], o["points"], o["status"], o["scraped_at"]) 
                     for o in all_odds]
                )
            
            conn.commit()

    def save_snapshot_to_json(self, processed_data: List[Tuple[Dict, Dict, List[Dict], List[Dict]]]):
        """Save JSON less frequently to reduce I/O."""
        if not processed_data:
            return
        
        json_events = []
        for game_info, scores_data, stats_data, odds_list in processed_data:
            event_data = game_info.copy()
            event_data['live_scores'] = scores_data
            event_data['live_statistics'] = stats_data
            event_data['odds'] = odds_list
            json_events.append(event_data)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.live_data_dir / f"live_snapshot_{timestamp}.json"
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump({
                "snapshot_time_utc": datetime.now(timezone.utc).isoformat(),
                "live_events_count": len(json_events),
                "events": json_events
            }, f, indent=2)

    async def update_cycle_async(self):
        """Single optimized update cycle."""
        cycle_start = time.time()
        
        # Fetch all sports concurrently
        all_live_events = await self.scrape_all_sports_async()
        fetch_time = time.time() - cycle_start
        
        if not all_live_events:
            self.logger.info(f"No live events (fetch: {fetch_time:.2f}s)")
            return
        
        # Process events
        process_start = time.time()
        processed_data = self.process_live_events(all_live_events)
        process_time = time.time() - process_start
        
        # Save to database (always)
        db_start = time.time()
        self.save_snapshot_to_db_optimized(processed_data)
        db_time = time.time() - db_start
        
        # Count actual data saved
        games_count = len(processed_data)
        scores_count = sum(1 for _, scores, _, _ in processed_data if scores)
        stats_count = sum(len(stats) for _, _, stats, _ in processed_data)
        odds_count = sum(len(odds) for _, _, _, odds in processed_data)
        
        # Save JSON periodically
        self.save_json_counter += 1
        if self.save_json_counter >= self.json_save_interval:
            self.save_snapshot_to_json(processed_data)
            self.save_json_counter = 0
        
        total_time = time.time() - cycle_start
        self.logger.info(
            f"✅ Updated {games_count} games | "
            f"Scores: {scores_count} | Stats: {stats_count} | Odds: {odds_count} | "
            f"Fetch: {fetch_time:.2f}s | Process: {process_time:.2f}s | "
            f"DB: {db_time:.2f}s | Total: {total_time:.2f}s"
        )
        
        if total_time > self.poll_interval_seconds:
            self.logger.warning(f"⚠️ Cycle exceeded {self.poll_interval_seconds}s target!")

    def run(self):
        """Main run loop with asyncio."""
        self.logger.info(
            f"🚀 Starting Optimized Live Scraper | "
            f"Poll interval: {self.poll_interval_seconds}s | "
            f"Concurrent: {self.max_concurrent} | Target: <8s per cycle"
        )
        
        try:
            while True:
                loop_start = time.time()
                
                # Run async update cycle
                asyncio.run(self.update_cycle_async())
                
                # Sleep for remaining time
                elapsed = time.time() - loop_start
                sleep_time = max(0, self.poll_interval_seconds - elapsed)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            self.logger.info("⏹️ Scraper stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)

if __name__ == "__main__":
    scraper = OptimizedLiveBallyBetScraper(poll_interval=6)
    scraper.run()
    scraper = OptimizedLiveBallyBetScraper(poll_interval=5)
    scraper.run()