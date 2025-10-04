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


class PlayerPropsScraper:
    """
    Scrapes player prop betting data from the Bally Bet (Kambi) API.
    This version saves data to a structured SQLite database with stable line_ids
    and also to JSON files.
    """

    def __init__(self, data_dir: str = "ballybet_data"):
        # --- CONFIGURATION ---
        self.LEAGUES_TO_SCRAPE = {
            "EPL": "football/england/premier_league",
            "NBA": "basketball/usa/nba",
            "NFL": "american_football/usa/nfl",
        }
        # --- END CONFIGURATION ---

        self.base_url = "https://eu1.offering-api.kambicdn.com/offering/v2018/bcsustn"
        self.session = requests.Session()
        self.data_dir = Path(data_dir)

        # Setup directories
        self.props_data_dir = self.data_dir / "json_props"
        self.props_data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "logs").mkdir(exist_ok=True)
        (self.data_dir / "db").mkdir(exist_ok=True)

        self.setup_logging()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        self.session.headers.update(self.headers)

        # --- DATABASE SETUP ---
        self.db_path = self.data_dir / "db" / "ballybet_props.db"
        self.init_database()
        # --- END DATABASE SETUP ---

    def setup_logging(self):
        log_file = self.data_dir / "logs" / "props_scraper.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file, encoding="utf-8"),
                logging.StreamHandler(),
            ],
            encoding="utf-8",
        )
        self.logger = logging.getLogger(__name__)

    def init_database(self):
        """Initializes the SQLite database with games and odds tables for player props."""
        with sqlite3.connect(self.db_path) as conn:
            # Table for game information
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS player_props_games (
                    game_id TEXT PRIMARY KEY,
                    match_id INTEGER,
                    match_name TEXT,
                    start_time TEXT,
                    league_name TEXT,
                    sport_name TEXT,
                    scraped_at TEXT
                )
                """
            )
            # Table for individual prop odds, linked by game_id
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS player_props_odds (
                    line_id TEXT PRIMARY KEY,
                    game_id TEXT,
                    market_name TEXT,
                    player_name TEXT,
                    label TEXT,
                    price REAL,
                    line REAL,
                    scraped_at TEXT,
                    FOREIGN KEY (game_id) REFERENCES player_props_games(game_id)
                )
                """
            )
            conn.commit()
            self.logger.info(f"Props database initialized at {self.db_path}")

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        try:
            response = self.session.get(url, params=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP Request failed for {url}: {e}")
            return None

    def get_event_ids_for_league(self, league_path: str) -> List[int]:
        self.logger.info(f"Fetching event IDs for league path: {league_path}")
        url = f"{self.base_url}/listView/{league_path}/all/matches.json"
        params = {"lang": "en_US", "market": "US-TN"}
        data = self._make_request(url, params)
        if not data or "events" not in data:
            self.logger.warning(f"Could not fetch events for {league_path}.")
            return []
        event_ids = [event["event"]["id"] for event in data["events"]]
        self.logger.info(f"Found {len(event_ids)} events.")
        return event_ids

    def get_all_markets_for_event(self, event_id: int) -> Optional[Dict]:
        self.logger.debug(f"Fetching all markets for event ID: {event_id}")
        url = f"{self.base_url}/betoffer/event/{event_id}.json"
        params = {"lang": "en_US", "market": "US-TN"}
        return self._make_request(url, params)

    def generate_stable_line_id(
        self, game_id: str, market: str, player: str, label: str, line: Optional[float]
    ) -> str:
        """Generates a unique, repeatable ID for a specific prop bet."""

        def clean(text: str) -> str:
            return re.sub(r"[^a-zA-Z0-9]", "", str(text)).lower()

        line_str = str(line).replace(".", "_") if line is not None else "na"
        # Truncate long market names to keep ID length reasonable
        market_clean = clean(market)[:30]

        return f"{clean(player)}:{market_clean}:{clean(label)}:{line_str}:ballybet_prop"

    def process_event_markets(
        self, event_json: Dict
    ) -> Optional[Tuple[Dict, List[Dict]]]:
        """Processes the full event JSON to extract game info and a list of all player props."""
        if not event_json or not event_json.get("events"):
            return None

        event = event_json["events"][0]
        props_list = []
        scraped_at = datetime.now(timezone.utc).isoformat()

        home_team = event.get("homeName")
        away_team = event.get("awayName")
        start_time = event.get("start")

        if not all([home_team, away_team, start_time]):
            return None

        game_id = re.sub(
            r"[^\w]", "", f"{home_team}{away_team}{start_time.split('T')[0]}"
        ).lower()

        for offer in event_json.get("betOffers", []):
            offer_type_name = offer.get("betOfferType", {}).get("name", "")
            criterion_label = offer.get("criterion", {}).get("label", "")

            is_player_prop_type = "player" in offer_type_name.lower()
            is_player_prop_label = "player" in criterion_label.lower() or re.search(
                r"(to score|shots|assists|saves|rebounds|yards)", criterion_label, re.I
            )

            if not (is_player_prop_type or is_player_prop_label):
                continue

            for outcome in offer.get("outcomes", []):
                if "participant" in outcome and outcome["participant"]:
                    player_name = outcome["participant"]
                    market_name = criterion_label
                    label = outcome.get("label")
                    line = (
                        outcome.get("line", 0) / 1000.0 if "line" in outcome else None
                    )

                    line_id = self.generate_stable_line_id(
                        game_id, market_name, player_name, label, line
                    )

                    props_list.append(
                        {
                            "line_id": line_id,
                            "game_id": game_id,
                            "market_name": market_name,
                            "player_name": player_name,
                            "label": label,
                            "price": (
                                outcome.get("odds", 0) / 1000.0
                                if "odds" in outcome
                                else None
                            ),
                            "line": line,
                            "scraped_at": scraped_at,
                        }
                    )

        if not props_list:
            return None

        game_info = {
            "game_id": game_id,
            "match_id": event.get("id"),
            "match_name": event.get("name"),
            "start_time": start_time,
            "league_name": event.get("group"),
            "sport_name": event.get("sport").lower(),
            "scraped_at": scraped_at,
        }

        return (game_info, props_list)

    def save_to_database(self, processed_data: List[Tuple[Dict, List[Dict]]]):
        """Saves the processed games and props to the SQLite database."""
        if not processed_data:
            return

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for game_info, props_list in processed_data:
                # Insert/replace the game info
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO player_props_games 
                    (game_id, match_id, match_name, start_time, league_name, sport_name, scraped_at)
                    VALUES (:game_id, :match_id, :match_name, :start_time, :league_name, :sport_name, :scraped_at)
                    """,
                    game_info,
                )

                # Insert/replace all props for that game
                cursor.executemany(
                    """
                    INSERT OR REPLACE INTO player_props_odds
                    (line_id, game_id, market_name, player_name, label, price, line, scraped_at)
                    VALUES (:line_id, :game_id, :market_name, :player_name, :label, :price, :line, :scraped_at)
                    """,
                    props_list,
                )
            conn.commit()
        self.logger.info(
            f"💾 Updated database with props for {len(processed_data)} games."
        )

    def save_to_json(
        self, league_name: str, processed_data: List[Tuple[Dict, List[Dict]]]
    ):
        """Saves the scraped data to a structured JSON file."""
        if not processed_data:
            self.logger.info(
                f"No player props found for {league_name} to save to JSON."
            )
            return

        # Restructure data for JSON output
        json_output_games = []
        for game_info, props_list in processed_data:
            game_info_copy = game_info.copy()
            game_info_copy["player_props"] = props_list
            json_output_games.append(game_info_copy)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.props_data_dir / f"playerprops_{league_name}_{timestamp}.json"

        output = {
            "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
            "league": league_name,
            "games_with_props": len(json_output_games),
            "games": json_output_games,
        }

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        self.logger.info(
            f"Saved props for {len(json_output_games)} games from {league_name} to {file_path}"
        )

    def run(self):
        self.logger.info("🚀 Starting Player Props Scraper")
        start_time = time.time()

        for league_name, league_path in self.LEAGUES_TO_SCRAPE.items():
            event_ids = self.get_event_ids_for_league(league_path)
            if not event_ids:
                continue

            all_game_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_id = {
                    executor.submit(self.get_all_markets_for_event, eid): eid
                    for eid in event_ids
                }
                for future in concurrent.futures.as_completed(future_to_id):
                    event_json = future.result()
                    if event_json:
                        processed_data = self.process_event_markets(event_json)
                        if processed_data:
                            all_game_data.append(processed_data)

            # Save to both JSON and Database
            self.save_to_json(league_name, all_game_data)
            self.save_to_database(all_game_data)

        duration = time.time() - start_time
        self.logger.info("=" * 50)
        self.logger.info(
            f"🎉 Player Props Scraping Complete! Duration: {duration:.2f} seconds."
        )
        self.logger.info(
            f"💾 Data saved in '{self.props_data_dir}' and '{self.db_path}'"
        )
        self.logger.info("=" * 50)


if __name__ == "__main__":
    scraper = PlayerPropsScraper()
    scraper.run()
