import requests
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional, Iterator, Tuple
import re
import time
from pathlib import Path
import logging
import concurrent.futures
import sqlite3

class FuturesScraper:
    """
    Scrapes "Futures" or "Outright" betting data for all discoverable leagues
    by dynamically fetching the API's group structure on each run.
    """

    def __init__(self, data_dir: str = "ballybet_data"):
        self.base_url = "https://eu1.offering-api.kambicdn.com/offering/v2018/bcsustn"
        self.session = requests.Session()
        self.data_dir = Path(data_dir)

        # Setup directories
        self.futures_data_dir = self.data_dir / "json_futures"
        self.futures_data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "logs").mkdir(exist_ok=True)
        (self.data_dir / "db").mkdir(exist_ok=True)

        self.setup_logging()
        self.headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        self.session.headers.update(self.headers)

        self.db_path = self.data_dir / "db" / "ballybet_futures.db"
        self.init_database()

    def setup_logging(self):
        log_file = self.data_dir / "logs" / "futures_scraper.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler()],
            encoding='utf-8'
        )
        self.logger = logging.getLogger(__name__)

    def init_database(self):
        """Initializes a two-table database for futures data."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS competitions (
                    competition_id TEXT PRIMARY KEY, competition_name TEXT,
                    sport TEXT, scraped_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS futures_odds (
                    line_id TEXT PRIMARY KEY, competition_id TEXT, market_name TEXT,
                    participant_name TEXT, price REAL, scraped_at TEXT,
                    FOREIGN KEY (competition_id) REFERENCES competitions(competition_id)
                )
                """
            )
            conn.commit()
            self.logger.info(f"Futures database with two tables initialized at {self.db_path}")

    # --- ID GENERATION METHODS ---
    def _clean_name_for_id(self, name: str) -> str:
        name = re.sub(r"[^\w\s]", "", str(name))
        name = re.sub(r"\s+", "_", name.strip()).lower()
        return name

    def generate_competition_id(self, competition_name: str, sport: str) -> str:
        return f"{self._clean_name_for_id(sport)}:{self._clean_name_for_id(competition_name)}"

    def generate_stable_line_id(self, competition_name: str, market_name: str, participant: str) -> str:
        return f"{self._clean_name_for_id(participant)}:{self._clean_name_for_id(market_name)}:{self._clean_name_for_id(competition_name)}:ballybet_future"

    # --- NETWORKING & DISCOVERY ---
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        try:
            response = self.session.get(url, params=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if response is not None and (response.status_code in [404, 400]):
                self.logger.debug(f"Request failed ({response.status_code}) for endpoint: {url}")
            else:
                self.logger.error(f"HTTP Request failed for {url}: {e}")
            return None

    def _discover_recursive(self, current_group: Dict, path_parts: List[str]) -> Iterator[Dict]:
        term_key = current_group.get("termKey")
        if term_key and current_group.get("eventCount", 0) > 0:
            current_path = "/".join(path_parts)
            yield {"name": current_group.get("name"), "path": current_path, "sport": path_parts[0]}
        for subgroup in current_group.get("groups", []):
            sub_term_key = subgroup.get("termKey")
            if sub_term_key:
                yield from self._discover_recursive(subgroup, path_parts + [sub_term_key])

    def discover_all_competitions(self) -> Iterator[Dict]:
        """
        --- MODIFIED ---
        Loads group.json directly from the API and starts the recursive search.
        """
        self.logger.info("Discovering all competitions by fetching group.json from the API...")
        discovery_url = f"{self.base_url}/group.json"
        data = self._make_request(discovery_url, params={"lang": "en_US", "market": "US-TN"})
        
        if not data or "group" not in data:
            self.logger.error("Failed to fetch discovery data from API. Cannot find markets.")
            return

        for sport_group in data["group"].get("groups", []):
            sport_term_key = sport_group.get("termKey")
            if sport_term_key:
                yield from self._discover_recursive(sport_group, [sport_term_key])
    
    def scrape_future_market(self, market_info: Dict) -> Optional[Tuple[Dict, List[Dict]]]:
        path = market_info["path"]
        self.logger.info(f"Scraping futures for: {market_info['name']}")
        url = f"{self.base_url}/listView/{path}/all/competitions.json"
        data = self._make_request(url, params={"lang": "en_US", "market": "US-TN"})
        if not data: return None

        bet_offers = data.get("betOffers")
        if not bet_offers and data.get("events"):
             bet_offers = data["events"][0].get("betOffers", [])
        if not bet_offers: return None
        
        return self.process_outright_data(bet_offers, data.get("events", []), market_info)

    def process_outright_data(self, bet_offers: List[Dict], events: List[Dict], market_info: Dict) -> Optional[Tuple[Dict, List[Dict]]]:
        scraped_at = datetime.now(timezone.utc).isoformat()
        competition_name = events[0].get("event", {}).get("name") if events else market_info['name']
        sport = market_info.get("sport")
        
        competition_id = self.generate_competition_id(competition_name, sport)
        
        competition_info = {
            "competition_id": competition_id, "competition_name": competition_name,
            "sport": sport, "scraped_at": scraped_at
        }
        
        odds_list = []
        for offer in bet_offers:
            market_name = offer.get("criterion", {}).get("label", "Winner")
            for outcome in offer.get("outcomes", []):
                participant = outcome.get("participant")
                if not participant: continue
                line_id = self.generate_stable_line_id(competition_name, market_name, participant)
                odds_list.append({
                    "line_id": line_id, "competition_id": competition_id, "market_name": market_name,
                    "participant_name": participant, "price": outcome.get("odds", 0) / 1000.0,
                    "scraped_at": scraped_at
                })
        
        if not odds_list: return None
        return (competition_info, odds_list)

    def save_to_database(self, all_futures_data: List[Tuple[Dict, List[Dict]]]):
        if not all_futures_data: return
        total_odds = 0
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for competition_info, odds_list in all_futures_data:
                cursor.execute("INSERT OR REPLACE INTO competitions VALUES (:competition_id, :competition_name, :sport, :scraped_at)", competition_info)
                if odds_list:
                    cursor.executemany("INSERT OR REPLACE INTO futures_odds VALUES (:line_id, :competition_id, :market_name, :participant_name, :price, :scraped_at)", odds_list)
                    total_odds += len(odds_list)
            conn.commit()
        self.logger.info(f"💾 Updated database with {total_odds} odds across {len(all_futures_data)} competitions.")

    def save_to_json(self, all_futures_data: List[Tuple[Dict, List[Dict]]]):
        if not all_futures_data:
            self.logger.info("No futures data to save to JSON.")
            return
        json_output = []
        for competition_info, odds_list in all_futures_data:
            comp_copy = competition_info.copy()
            comp_copy["odds"] = odds_list
            json_output.append(comp_copy)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.futures_data_dir / f"futures_snapshot_{timestamp}.json"
        output_wrapper = {"scraped_at_utc": datetime.now(timezone.utc).isoformat(), "competitions_count": len(json_output), "competitions": json_output}
        with open(file_path, "w", encoding='utf-8') as f: json.dump(output_wrapper, f, indent=2)
        self.logger.info(f"✅ Saved JSON snapshot to {file_path}")

    def run(self):
        self.logger.info("🚀 Starting All-Futures Scraper")
        start_time = time.time()
        all_competitions = list(self.discover_all_competitions())
        if not all_competitions: return
            
        all_futures_data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_market = {executor.submit(self.scrape_future_market, market): market for market in all_competitions}
            for future in concurrent.futures.as_completed(future_to_market):
                try:
                    market_data_tuple = future.result()
                    if market_data_tuple: all_futures_data.append(market_data_tuple)
                except Exception as e:
                    market_name = future_to_market[future].get('name', 'Unknown')
                    self.logger.error(f"Failed to process market {market_name}: {e}", exc_info=True)

        if not all_futures_data:
            self.logger.warning("No futures data was successfully scraped. Nothing to save.")
        else:
            self.save_to_database(all_futures_data)
            self.save_to_json(all_futures_data)
        
        duration = time.time() - start_time
        self.logger.info("=" * 50)
        self.logger.info(f"🎉 Futures Scraping Complete! Duration: {duration:.2f} seconds.")
        self.logger.info(f"💾 Data saved in '{self.futures_data_dir}' and '{self.db_path}'")
        self.logger.info("=" * 50)

if __name__ == "__main__":
    scraper = FuturesScraper()
    scraper.run()