import requests
import json
import sqlite3
from datetime import datetime, timezone
from typing import List, Dict, Optional, Iterator
import re
import time
from pathlib import Path
import logging
import asyncio
import aiohttp
from asyncio import Semaphore

class OptimizedBallyBetScraper:
    """
    Reliable high-speed pregame scraper with smart rate limiting.
    
    Key features:
    1. Controlled concurrency to avoid API overload
    2. Retry logic with exponential backoff
    3. Realistic timeouts
    4. Smart request pacing
    """

    def __init__(self, data_dir: str = "ballybet_data"):
        self.SPORTS_TO_SCRAPE = [
            "football", "basketball", "american_football",
            "ice_hockey", "tennis", "baseball", "esports"
        ]

        self.base_url = "https://eu1.offering-api.kambicdn.com/offering/v2018/bcsustn"
        self.data_dir = Path(data_dir)
        
        # Realistic timeouts
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        # Rate limiting
        self.max_concurrent = 15  # Lower to avoid overwhelming API
        self.request_delay = 0.05  # 50ms between batches

        # Directory setup
        self.data_dir.mkdir(exist_ok=True)
        (self.data_dir / "json").mkdir(exist_ok=True)
        (self.data_dir / "db").mkdir(exist_ok=True)
        (self.data_dir / "logs").mkdir(exist_ok=True)

        self.setup_logging()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }

        self.db_path = self.data_dir / "db" / "ballybet.db"
        self.init_database()
        
        # Cache for leagues
        self.leagues_cache = []
        self.cache_timestamp = 0
        self.cache_ttl = 3600
        
        self.json_save_counter = 0
        self.json_save_interval = 6

    def setup_logging(self):
        log_file = self.data_dir / "logs" / "scraper_optimized.log"
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
        """Initialize with performance optimizations."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")
            conn.execute("PRAGMA temp_store=MEMORY")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    game_id TEXT PRIMARY KEY, match_id INTEGER, sport_name TEXT,
                    home_team TEXT, away_team TEXT, start_time TEXT, scraped_at TEXT,
                    league_name TEXT, status TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS odds (
                    line_id TEXT,
                    game_id TEXT,
                    bet_type TEXT,
                    designation TEXT,
                    price REAL,
                    points REAL,
                    scraped_at TEXT,
                    PRIMARY KEY (game_id, line_id),
                    FOREIGN KEY (game_id) REFERENCES games(game_id)
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_games_sport ON games(sport_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_games_start ON games(start_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_odds_game ON odds(game_id)")
            
            conn.commit()
            self.logger.info(f"Optimized database initialized at {self.db_path}")

    def get_current_timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    async def fetch_with_retry(self, session: aiohttp.ClientSession, url: str, 
                               params: Dict, max_retries: int = 2) -> Optional[Dict]:
        """Fetch with exponential backoff retry."""
        for attempt in range(max_retries + 1):
            try:
                async with session.get(url, params=params, headers=self.headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:  # Rate limit
                        wait = 2 ** attempt
                        self.logger.warning(f"Rate limited, waiting {wait}s")
                        await asyncio.sleep(wait)
                    else:
                        self.logger.warning(f"HTTP {response.status} for {url}")
                        return None
            except asyncio.TimeoutError:
                if attempt < max_retries:
                    wait = 1 * (attempt + 1)
                    self.logger.warning(f"Timeout attempt {attempt + 1}, retrying in {wait}s")
                    await asyncio.sleep(wait)
                else:
                    self.logger.error(f"Timeout after {max_retries} retries: {url}")
                    return None
            except Exception as e:
                self.logger.error(f"Error fetching {url}: {e}")
                return None
        return None

    async def fetch_discovery_data(self, session: aiohttp.ClientSession) -> Optional[Dict]:
        """Fetch discovery data with retry."""
        url = f"{self.base_url}/group.json"
        params = {"lang": "en_US", "market": "US-TN"}
        return await self.fetch_with_retry(session, url, params)

    async def discover_leagues_async(self) -> List[Dict[str, str]]:
        """Discover leagues with caching."""
        current_time = time.time()
        
        if self.leagues_cache and (current_time - self.cache_timestamp) < self.cache_ttl:
            self.logger.info(f"Using cached leagues ({len(self.leagues_cache)} total)")
            return self.leagues_cache
        
        self.logger.info("Discovering leagues...")
        leagues = []
        
        connector = aiohttp.TCPConnector(limit=5, limit_per_host=5)
        async with aiohttp.ClientSession(connector=connector, timeout=self.timeout) as session:
            data = await self.fetch_discovery_data(session)
            
            if not data or "group" not in data:
                self.logger.error("Failed to fetch discovery data")
                return self.leagues_cache
            
            for sport_group in data["group"]["groups"]:
                sport_name = sport_group.get("termKey")
                if not sport_name or sport_name not in self.SPORTS_TO_SCRAPE:
                    continue
                
                (self.data_dir / "json" / sport_name).mkdir(exist_ok=True)
                
                for country in sport_group.get("groups", []):
                    for league in country.get("groups", []):
                        if league.get("eventCount", 0) > 0:
                            path = f"/{sport_name}/{country['termKey']}/{league['termKey']}"
                            leagues.append({
                                "sport": sport_name,
                                "league_name": league["name"],
                                "path": path
                            })
        
        self.leagues_cache = leagues
        self.cache_timestamp = current_time
        self.logger.info(f"Discovered {len(leagues)} leagues")
        return leagues

    async def fetch_league_async(self, session: aiohttp.ClientSession, 
                                 league_info: Dict, semaphore: Semaphore) -> tuple:
        """Fetch single league with semaphore control."""
        async with semaphore:
            # Small delay to pace requests
            await asyncio.sleep(self.request_delay)
            
            sport, path, league_name = league_info["sport"], league_info["path"], league_info["league_name"]
            url = f"{self.base_url}/listView{path}/all/matches.json"
            params = {"lang": "en_US", "market": "US-TN", "useCombined": "true"}
            
            data = await self.fetch_with_retry(session, url, params)
            if data:
                return league_info, data.get("events", [])
            return league_info, None

    # ID generation methods (unchanged)
    def generate_game_id(self, home_team: str, away_team: str, start_time: str) -> str:
        def clean_name(name: str) -> str:
            name = re.sub(r"[^\w\s]", "", name)
            name = re.sub(r"\s+", "_", name.strip()).lower()
            return name
        home_clean = clean_name(home_team)
        away_clean = clean_name(away_team)
        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y%m%d_%H%M")
        except:
            date_str = "unknown"
        return f"{home_clean}:{away_clean}:{date_str}"

    def clean_team_name(self, name: str) -> str:
        name = re.sub(r"\s+(FC|CF|United|City|Town|Rovers)$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(r"\s+", "_", name.strip()).lower()
        return name

    def generate_stable_line_id(self, home_team: str, away_team: str, bet_type: str,
                                designation: str, points: Optional[float] = None) -> str:
        home_clean = self.clean_team_name(home_team)
        away_clean = self.clean_team_name(away_team)
        points_str = f"{points}".replace(".", "_") if points is not None else ""
        sep = ":"
        bookmaker = "ballybet"

        if bet_type == "moneyline":
            if designation.lower() == "home": return f"{home_clean}{sep}moneyline{sep}{bookmaker}"
            elif designation.lower() == "away": return f"{away_clean}{sep}moneyline{sep}{bookmaker}"
            elif designation.lower() in ["draw", "tie"]: return f"draw{sep}moneyline{sep}{bookmaker}"
            else: return f"{designation.lower()}{sep}moneyline{sep}{bookmaker}"
        elif bet_type == "spread":
            points_str = f"{points}".replace(".", "_") if points is not None else ""
            if designation.lower() == "home": 
                return f"{home_clean}{sep}spread{sep}{points_str}{sep}{bookmaker}"
            elif designation.lower() == "away": 
                return f"{away_clean}{sep}spread{sep}{points_str}{sep}{bookmaker}"
            else: 
                return f"{designation.lower()}{sep}spread{sep}{points_str}{sep}{bookmaker}"
        elif bet_type == "total":
            return f"game_total{sep}{designation.lower()}{sep}{points_str}{sep}{bookmaker}"
        else:
            return f"{home_clean}{sep}{away_clean}{sep}{bet_type}{sep}{designation.lower()}{sep}{points_str}{sep}{bookmaker}"

    def process_events_data(self, events: List[Dict], sport_name: str, league_name: str) -> List[Dict]:
        """Fast in-memory processing."""
        structured_matches = []
        scraped_at_iso = self.get_current_timestamp()
        
        for item in events:
            event, bet_offers = item.get("event"), item.get("betOffers")
            if not event or not bet_offers: continue

            home_team, away_team, start_time = event.get("homeName"), event.get("awayName"), event.get("start")
            if not all([home_team, away_team, start_time]): continue

            game_id = self.generate_game_id(home_team, away_team, start_time)
            odds_data = self.extract_odds(bet_offers, home_team, away_team)

            structured_matches.append({
                "game_id": game_id, "matchId": event.get("id"), "homeTeam": home_team,
                "awayTeam": away_team, "startTime": start_time, "scrapedAt": scraped_at_iso,
                "sport": sport_name, "league": league_name, "status": event.get("state"),
                "odds": odds_data.get("odds", {}),
            })
        return structured_matches

    def extract_odds(self, bet_offers: List[Dict], home_team: str, away_team: str) -> Dict:
        """Fast odds extraction with comprehensive bet type support."""
        odds = {"moneyline": {}, "total": {}, "spread": {}}
        
        for offer in bet_offers:
            offer_type = offer.get("betOfferType", {}).get("name")
            outcomes = offer.get("outcomes", [])
            if not outcomes: continue

            # Moneyline bets
            if offer_type == "Match":
                bet_type_for_id = "moneyline"
                for outcome in outcomes:
                    # Try different ways to identify the outcome type
                    outcome_type = outcome.get("type", "")
                    label = outcome.get("label", "")
                    
                    if outcome_type == "OT_ONE" or label == "1":
                        designation = "home"
                    elif outcome_type == "OT_TWO" or label == "2":
                        designation = "away"
                    elif outcome_type == "OT_CROSS" or label == "X":
                        designation = "draw"
                    else:
                        continue
                    
                    line_id = self.generate_stable_line_id(home_team, away_team, bet_type_for_id, designation)
                    odds["moneyline"][designation] = {
                        "line_id": line_id,
                        "price": outcome.get("odds", 0) / 1000.0,
                        "designation": designation
                    }
            
            # Over/Under (Totals)
            elif offer_type == "Over/Under":
                bet_type_for_id = "total"
                if len(outcomes) >= 2:
                    points = outcomes[0].get("line", 0) / 1000.0
                    for outcome in outcomes[:2]:
                        designation = outcome.get("label", "").lower()
                        if designation in ["over", "under"]:
                            line_id = self.generate_stable_line_id(home_team, away_team, bet_type_for_id, designation, points)
                            odds["total"][designation] = {
                                "line_id": line_id,
                                "price": outcome.get("odds", 0) / 1000.0,
                                "points": points,
                                "designation": designation
                            }
            
            # Spread/Handicap bets
            elif offer_type in ["Handicap", "Asian Handicap", "Spread"]:
                bet_type_for_id = "spread"
                if len(outcomes) >= 2:
                    # Find the handicap line from the first outcome
                    handicap_line = outcomes[0].get("line", 0) / 1000.0
                    
                    for outcome in outcomes[:2]:
                        label = outcome.get("label", "")
                        if label == "1":
                            designation = "home"
                            points = handicap_line
                        elif label == "2":
                            designation = "away" 
                            points = -handicap_line
                        else:
                            continue
                            
                        line_id = self.generate_stable_line_id(home_team, away_team, bet_type_for_id, designation, points)
                        odds["spread"][designation] = {
                            "line_id": line_id,
                            "price": outcome.get("odds", 0) / 1000.0,
                            "points": points,
                            "designation": designation
                        }
        
        return {"odds": odds}

    def save_to_database_batch(self, all_matches: List[Dict]):
        """Optimized batch database save with error handling."""
        if not all_matches:
            return
        
        games_saved = 0
        odds_saved = 0
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.cursor()
            
            # Save games
            game_data = []
            for match in all_matches:
                try:
                    game_data.append((
                        match["game_id"], match["matchId"], match["sport"], 
                        match["homeTeam"], match["awayTeam"], match["startTime"], 
                        match["scrapedAt"], match["league"], match["status"]
                    ))
                    games_saved += 1
                except Exception as e:
                    self.logger.error(f"Error preparing game data for {match.get('game_id', 'unknown')}: {e}")
            
            # Batch insert games
            if game_data:
                try:
                    cursor.executemany(
                        "INSERT OR REPLACE INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        game_data
                    )
                except Exception as e:
                    self.logger.error(f"Error saving games batch: {e}")
            
            # Save odds with individual error handling
            odds_data = []
            for match in all_matches:
                for bet_type, bet_group in match.get("odds", {}).items():
                    if not isinstance(bet_group, dict):
                        continue
                    for odd_data in bet_group.values():
                        if not isinstance(odd_data, dict):
                            continue
                        try:
                            odd_record = (
                                odd_data.get("line_id", ""), 
                                match["game_id"], 
                                bet_type, 
                                odd_data.get("designation", ""), 
                                odd_data.get("price"), 
                                odd_data.get("points"), 
                                match["scrapedAt"]
                            )
                            odds_data.append(odd_record)
                            odds_saved += 1
                        except Exception as e:
                            self.logger.error(f"Error preparing odd data for game {match.get('game_id', 'unknown')}: {e}")
            
            # Batch insert odds
            if odds_data:
                try:
                    cursor.executemany(
                        "INSERT OR REPLACE INTO odds VALUES (?, ?, ?, ?, ?, ?, ?)",
                        odds_data
                    )
                except Exception as e:
                    self.logger.error(f"Error saving odds batch: {e}")
            
            conn.commit()
        
        self.logger.info(f"💾 Saved {games_saved} games and {odds_saved} odds to database")

    def save_json_periodic(self, sport_name: str, data: List[Dict]):
        """Save JSON periodically."""
        if not data:
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_file = self.data_dir / "json" / sport_name / f"batch_{timestamp}.json"
        
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump({
                "sport": sport_name,
                "scraped_at": self.get_current_timestamp(),
                "match_count": len(data),
                "matches": data
            }, f, indent=2, ensure_ascii=False)

    async def scrape_all_leagues_async(self, leagues: List[Dict]) -> tuple:
        """Fetch all leagues with controlled concurrency."""
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
            tasks = [
                self.fetch_league_async(session, league, semaphore)
                for league in leagues
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_matches = []
        sport_matches = {}
        failed_count = 0
        
        for result in results:
            if isinstance(result, Exception):
                self.logger.error(f"Task exception: {result}")
                failed_count += 1
                continue
            
            league_info, events = result
            if events is None:
                failed_count += 1
                continue
            
            if events:
                matches = self.process_events_data(
                    events,
                    league_info["sport"],
                    league_info["league_name"]
                )
                all_matches.extend(matches)
                
                sport = league_info["sport"]
                if sport not in sport_matches:
                    sport_matches[sport] = []
                sport_matches[sport].extend(matches)
        
        if failed_count > 0:
            self.logger.warning(f"⚠️  {failed_count} leagues failed to fetch")
        
        return all_matches, sport_matches

    async def run_cycle_async(self):
        """Single optimized scrape cycle."""
        cycle_start = time.time()
        
        discover_start = time.time()
        leagues = await self.discover_leagues_async()
        discover_time = time.time() - discover_start
        
        if not leagues:
            self.logger.error("No leagues to scrape")
            return
        
        scrape_start = time.time()
        all_matches, sport_matches = await self.scrape_all_leagues_async(leagues)
        scrape_time = time.time() - scrape_start
        
        db_start = time.time()
        self.save_to_database_batch(all_matches)
        db_time = time.time() - db_start
        
        self.json_save_counter += 1
        if self.json_save_counter >= self.json_save_interval:
            for sport, matches in sport_matches.items():
                self.save_json_periodic(sport, matches)
            self.json_save_counter = 0
            json_saved = "Yes"
        else:
            json_saved = "No"
        
        total_time = time.time() - cycle_start
        
        self.logger.info(
            f"✅ Scraped {len(leagues)} leagues, {len(all_matches)} matches | "
            f"Discover: {discover_time:.2f}s | Scrape: {scrape_time:.2f}s | "
            f"DB: {db_time:.2f}s | Total: {total_time:.2f}s | JSON: {json_saved}"
        )
        
        return total_time

    def run_continuous(self, interval: int = 15):
        """Run continuously with safe interval."""
        self.logger.info(
            f"🚀 Starting Optimized Pregame Scraper | "
            f"Interval: {interval}s | Concurrent: {self.max_concurrent}"
        )
        
        try:
            while True:
                loop_start = time.time()
                
                cycle_time = asyncio.run(self.run_cycle_async())
                
                elapsed = time.time() - loop_start
                sleep_time = max(0, interval - elapsed)
                
                if sleep_time > 0:
                    self.logger.info(f"💤 Sleeping {sleep_time:.1f}s until next cycle...")
                    time.sleep(sleep_time)
                else:
                    self.logger.warning(f"⚠️  Cycle took longer than {interval}s!")

        except KeyboardInterrupt:
            self.logger.info("⏹️  Scraper stopped by user")
        except Exception as e:
            self.logger.error(f"Fatal error: {e}", exc_info=True)

    def run_once(self):
        """Run single scrape cycle."""
        self.logger.info("🚀 Starting Single Scrape Cycle")
        asyncio.run(self.run_cycle_async())
        self.logger.info("✅ Scrape Complete")


if __name__ == "__main__":
    scraper = OptimizedBallyBetScraper(data_dir="ballybet_data")
    # More realistic 15 second interval to avoid overwhelming API
    scraper.run_continuous(interval=15)