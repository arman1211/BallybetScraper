import requests
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
import re
import time
from pathlib import Path
import logging
import concurrent.futures
import sqlite3

class LiveBallyBetScraper:
    """
    Scrapes LIVE, in-play sports data from the Bally Bet (Kambi) API.

    This script continuously polls the API to find currently live events across
    multiple configured sports, fetches their real-time data (scores, odds), 
    and saves periodic snapshots into a dedicated SQLite database and JSON files.
    """

    def __init__(self, data_dir: str = "ballybet_data", poll_interval: int = 60):
        self.SPORTS_TO_SCRAPE = [
            "football", "basketball", "tennis",
            "cricket", "ice_hockey", "esports"
        ]
        
        self.base_url = "https://eu1.offering-api.kambicdn.com/offering/v2018/bcsustn"
        self.session = requests.Session()
        self.data_dir = Path(data_dir)
        self.poll_interval_seconds = poll_interval

        # Setup directories
        self.live_data_dir = self.data_dir / "json_live"
        self.live_data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "logs").mkdir(exist_ok=True)
        (self.data_dir / "db").mkdir(exist_ok=True)

        self.setup_logging()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        self.session.headers.update(self.headers)

        # --- DATABASE SETUP ---
        self.db_path = self.data_dir / "db" / "ballybet_live.db"
        self.init_database()
        # --- END DATABASE SETUP ---

    def setup_logging(self):
        """Sets up a dedicated logger for the live scraper."""
        log_file = self.data_dir / "logs" / "live_scraper.log"
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
        """Initializes a separate SQLite database for live games and odds."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS live_games (
                    game_id TEXT PRIMARY KEY,
                    match_id INTEGER,
                    sport_name TEXT,
                    league_name TEXT,
                    home_team TEXT,
                    away_team TEXT,
                    start_time TEXT,
                    scraped_at TEXT,
                    live_data TEXT  -- Storing the live data object as a JSON string
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS live_odds (
                    line_id TEXT PRIMARY KEY,
                    game_id TEXT,
                    bet_type TEXT,
                    designation TEXT,
                    price REAL,
                    points REAL,
                    status TEXT,
                    scraped_at TEXT,
                    FOREIGN KEY (game_id) REFERENCES live_games(game_id)
                )
                """
            )
            conn.commit()
            self.logger.info(f"Live database initialized at {self.db_path}")

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP Request failed for {url}: {e}")
            return None

    # --- ID GENERATION METHODS ---

    def generate_game_id(self, home_team: str, away_team: str, start_time: str) -> str:
        def clean_name(name: str) -> str:
            name = re.sub(r"[^\w\s]", "", name)
            name = re.sub(r"\s+", "_", name.strip()).lower()
            return name
        home_clean, away_clean = clean_name(home_team), clean_name(away_team)
        try:
            date_str = datetime.fromisoformat(start_time.replace("Z", "+00:00")).strftime("%Y%m%d_%H%M")
        except: date_str = "unknown"
        return f"{home_clean}:{away_clean}:{date_str}"

    def clean_team_name(self, name: str) -> str:
        name = re.sub(r"\s+(FC|CF|United|City)$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(r"\s+", "_", name.strip()).lower()
        return name

    def generate_stable_line_id(self, home_team: str, away_team: str, bet_type: str, designation: str, points: Optional[float] = None) -> str:
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
    
    # --- END ID METHODS ---

    def scrape_live_sport(self, sport: str) -> List[Dict]:
        """Fetches all live events for a single sport."""
        self.logger.info(f"Checking for live events in: {sport.upper()}")
        url = f"{self.base_url}/listView/{sport}/all/all/all/in-play.json"
        params = {"lang": "en_US", "market": "US-TN", "useCombined": "true"}
        data = self._make_request(url, params)
        if not data or "events" not in data:
            self.logger.info(f"No live events found for {sport.upper()}.")
            return []
        return data["events"]

    def process_live_events(self, events: List[Dict]) -> List[Tuple[Dict, List[Dict]]]:
        """Translates the raw API data into a format ready for DB insertion."""
        processed_data = []
        scraped_at_iso = datetime.now(timezone.utc).isoformat()
        for item in events:
            event, bet_offers, live_data = item.get("event"), item.get("betOffers"), item.get("liveData")
            if not all([event, bet_offers, live_data]): continue

            home_team, away_team, start_time = event.get("homeName"), event.get("awayName"), event.get("start")
            if not all([home_team, away_team, start_time]): continue

            game_id = self.generate_game_id(home_team, away_team, start_time)
            odds_list = self.extract_odds(bet_offers, home_team, away_team, game_id, scraped_at_iso)

            game_info = {
                "game_id": game_id, "match_id": event.get("id"),
                "sport_name": event.get("sport").lower(), "league_name": event.get("group"),
                "home_team": home_team, "away_team": away_team, "start_time": start_time,
                "scraped_at": scraped_at_iso,
                "live_data": json.dumps({ # Serialize live data to a JSON string
                    "score": live_data.get("score"), "game_clock": live_data.get("matchClock"),
                    "statistics": live_data.get("statistics")
                })
            }
            processed_data.append((game_info, odds_list))
        return processed_data

    def extract_odds(self, bet_offers: List[Dict], home_team: str, away_team: str, game_id: str, scraped_at: str) -> List[Dict]:
        """Extracts and structures odds into a flat list for the database."""
        odds_list = []
        for offer in bet_offers:
            offer_type = offer.get("betOfferType", {}).get("name")
            
            if offer_type == "Match":
                bet_type_for_id = "moneyline"
                for outcome in offer.get("outcomes", []):
                    if outcome.get("label") == "1": designation = "home"
                    elif outcome.get("label") == "2": designation = "away"
                    elif outcome.get("label") == "X": designation = "draw"
                    else: continue
                    
                    odds_list.append({
                        "line_id": self.generate_stable_line_id(home_team, away_team, bet_type_for_id, designation),
                        "game_id": game_id, "bet_type": bet_type_for_id, "designation": designation,
                        "price": outcome.get("odds", 0) / 1000.0 if "odds" in outcome else None,
                        "points": None, "status": outcome.get("status", "SUSPENDED"), "scraped_at": scraped_at
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
                                "game_id": game_id, "bet_type": bet_type_for_id, "designation": designation,
                                "price": outcome.get("odds", 0) / 1000.0 if "odds" in outcome else None,
                                "points": points, "status": outcome.get("status", "SUSPENDED"), "scraped_at": scraped_at
                            })
        return odds_list

    def save_snapshot_to_db(self, processed_data: List[Tuple[Dict, List[Dict]]]):
        if not processed_data: return
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for game_info, odds_list in processed_data:
                cursor.execute("INSERT OR REPLACE INTO live_games VALUES (:game_id, :match_id, :sport_name, :league_name, :home_team, :away_team, :start_time, :scraped_at, :live_data)", game_info)
                if odds_list:
                    cursor.executemany("INSERT OR REPLACE INTO live_odds VALUES (:line_id, :game_id, :bet_type, :designation, :price, :points, :status, :scraped_at)", odds_list)
            conn.commit()
        self.logger.info(f"💾 Updated database with snapshot of {len(processed_data)} live events.")
    
    def save_snapshot_to_json(self, processed_data: List[Tuple[Dict, List[Dict]]]):
        if not processed_data:
            self.logger.info("Snapshot is empty. Nothing to save to JSON.")
            return
        
        # Re-structure for the desired JSON format
        json_events = []
        for game_info, odds_list in processed_data:
            event_data = game_info.copy()
            event_data['live_data'] = json.loads(event_data['live_data']) # Decode JSON string for file
            event_data['odds'] = odds_list
            json_events.append(event_data)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.live_data_dir / f"live_snapshot_{timestamp}.json"
        
        snapshot_wrapper = {
            "snapshot_time_utc": datetime.now(timezone.utc).isoformat(),
            "live_events_count": len(json_events),
            "events": json_events
        }
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(snapshot_wrapper, f, indent=2)
            
        self.logger.info(f"✅ Saved JSON snapshot of {len(json_events)} live events to {file_path}")

    def run(self):
        self.logger.info(f"🚀 Starting Live Scraper. Polling every {self.poll_interval_seconds} seconds. Press Ctrl+C to stop.")
        try:
            while True:
                all_live_events = []
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.SPORTS_TO_SCRAPE)) as executor:
                    future_to_sport = {executor.submit(self.scrape_live_sport, sport): sport for sport in self.SPORTS_TO_SCRAPE}
                    for future in concurrent.futures.as_completed(future_to_sport):
                        events = future.result()
                        if events:
                            all_live_events.extend(events)
                
                if not all_live_events:
                    self.logger.info(f"No live events found across all configured sports. Waiting...")
                else:
                    processed_data_for_db = self.process_live_events(all_live_events)
                    self.save_snapshot_to_db(processed_data_for_db)
                    self.save_snapshot_to_json(processed_data_for_db)
                
                self.logger.info(f"Next poll in {self.poll_interval_seconds} seconds...")
                time.sleep(self.poll_interval_seconds)

        except KeyboardInterrupt:
            self.logger.info("⏹️ Scraper stopped by user. Exiting gracefully.")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)

if __name__ == "__main__":
    live_scraper = LiveBallyBetScraper(poll_interval=60)
    live_scraper.run()