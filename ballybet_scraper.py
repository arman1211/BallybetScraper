import requests
import json
import sqlite3
from datetime import datetime, timezone
from typing import List, Dict, Optional, Iterator
import re
import time
import random
from pathlib import Path
import logging
import concurrent.futures


class BallyBetScraper:
    """
    Scrapes sports betting data from the Bally Bet (Kambi) API for multiple sports.

    This class discovers all available leagues for a configured list of sports,
    scrapes match data and odds, and saves the information into both
    JSON files and a SQLite database.
    """

    def __init__(self, data_dir: str = "ballybet_data"):
        # --- NEW ---: List of sports to scrape.
        # You can find the 'termKey' for other sports by inspecting the group.json API response.
        self.SPORTS_TO_SCRAPE = [
            "football",
            "basketball",
            "american_football",
            "ice_hockey",
            "tennis",
            "baseball",
            "esports",
        ]
        # --- END NEW ---

        self.base_url = "https://eu1.offering-api.kambicdn.com/offering/v2018/bcsustn"
        self.session = requests.Session()
        self.data_dir = Path(data_dir)

        # Create directory structure
        self.data_dir.mkdir(exist_ok=True)
        (self.data_dir / "json").mkdir(exist_ok=True)
        (self.data_dir / "db").mkdir(exist_ok=True)
        (self.data_dir / "logs").mkdir(exist_ok=True)

        self.setup_logging()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        self.session.headers.update(self.headers)

        self.db_path = self.data_dir / "db" / "ballybet.db"
        self.init_database()

    def setup_logging(self):
        """Sets up logging to file and console with UTF-8 encoding."""
        log_file = self.data_dir / "logs" / "scraper.log"

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

        summary_log_file = self.data_dir / "logs" / "league_summary.log"
        self.summary_logger = logging.getLogger("league_summary")
        summary_handler = logging.FileHandler(summary_log_file, encoding="utf-8")
        summary_formatter = logging.Formatter("%(asctime)s - %(message)s")
        summary_handler.setFormatter(summary_formatter)
        self.summary_logger.addHandler(summary_handler)
        self.summary_logger.setLevel(logging.INFO)
        self.summary_logger.propagate = False

    def init_database(self):
        """Initializes the SQLite database with games and odds tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS games (
                    game_id TEXT PRIMARY KEY, match_id INTEGER, sport_name TEXT,
                    home_team TEXT, away_team TEXT, start_time TEXT, scraped_at TEXT,
                    league_name TEXT, status TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS odds (
                    line_id TEXT PRIMARY KEY, game_id TEXT, bet_type TEXT, designation TEXT,
                    price REAL, points REAL, scraped_at TEXT,
                    FOREIGN KEY (game_id) REFERENCES games(game_id)
                )
                """
            )
            conn.commit()
            self.logger.info(f"Database initialized at {self.db_path}")

    def get_current_timestamp(self) -> str:
        """Returns the current UTC time in ISO 8601 format."""
        return datetime.now(timezone.utc).isoformat()

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Makes an HTTP GET request with error handling."""
        try:
            response = self.session.get(url, params=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP Request failed for {url}: {e}")
            return None

    def discover_leagues(self) -> Iterator[Dict[str, str]]:
        """
        Discovers all available leagues for the sports configured
        in self.SPORTS_TO_SCRAPE.
        """
        self.logger.info(
            f"Discovering leagues for configured sports: {self.SPORTS_TO_SCRAPE}"
        )
        discovery_url = f"{self.base_url}/group.json"
        params = {"lang": "en_US", "market": "US-TN"}

        data = self._make_request(discovery_url, params)
        if not data or "group" not in data:
            self.logger.error("Failed to fetch discovery data.")
            return

        # --- MODIFIED ---: Loop through all sports from the API
        for sport_group in data["group"]["groups"]:
            sport_name = sport_group.get("termKey")
            if not sport_name or sport_name not in self.SPORTS_TO_SCRAPE:
                continue  # Skip sport if it's not in our list

            self.logger.info(
                f"Found sport: {sport_group.get('name')}. Checking for leagues..."
            )
            # Create a directory for the sport if it doesn't exist
            (self.data_dir / "json" / sport_name).mkdir(exist_ok=True)

            # Leagues are typically nested under countries (regions)
            for country in sport_group.get("groups", []):
                for league in country.get("groups", []):
                    # Check for a positive event count to ensure the league is active
                    if league.get("eventCount", 0) > 0:
                        path = f"/{sport_name}/{country['termKey']}/{league['termKey']}"
                        self.logger.info(
                            f"Discovered league: {league['name']} ({path})"
                        )
                        yield {
                            "sport": sport_name,
                            "league_name": league["name"],
                            "path": path,
                        }
        # --- END MODIFIED ---

    def scrape_league(self, league_info: Dict) -> Optional[List[Dict]]:
        """Scrapes all match data for a single league."""
        sport = league_info["sport"]
        path = league_info["path"]
        league_name = league_info["league_name"]

        self.logger.info(f"Scraping league: {league_name} ({sport})")

        url = f"{self.base_url}/listView{path}/all/matches.json"
        params = {"lang": "en_US", "market": "US-TN", "useCombined": "true"}

        data = self._make_request(url, params)
        if not data or "events" not in data:
            self.logger.warning(
                f"No data or 'events' key found for league: {league_name}"
            )
            return []

        return self.process_events_data(data["events"], sport, league_name)

    def process_events_data(
        self, events: List[Dict], sport_name: str, league_name: str
    ) -> List[Dict]:
        """Processes the raw event data from the API into a structured format."""
        structured_matches = []
        scraped_at_iso = self.get_current_timestamp()

        for item in events:
            event = item.get("event")
            bet_offers = item.get("betOffers")
            if not event or not bet_offers:
                continue

            home_team = event.get("homeName")
            away_team = event.get("awayName")
            start_time = event.get("start")

            if not all([home_team, away_team, start_time]):
                self.logger.warning(
                    f"Skipping event with missing data: {event.get('id')}"
                )
                continue

            game_id = self.generate_game_id(home_team, away_team, start_time)
            odds_data = self.extract_odds(bet_offers, home_team, away_team)

            structured_match = {
                "game_id": game_id,
                "matchId": event.get("id"),
                "homeTeam": home_team,
                "awayTeam": away_team,
                "startTime": start_time,
                "scrapedAt": scraped_at_iso,
                "sport": sport_name,
                "league": league_name,
                "status": event.get("state"),
                "odds": odds_data.get("odds", {}),
            }
            structured_matches.append(structured_match)

        return structured_matches

    def extract_odds(
        self, bet_offers: List[Dict], home_team: str, away_team: str
    ) -> Dict:
        """Extracts and structures odds from the bet offers list."""
        odds = {"moneyline": {}, "total": {}}
        for offer in bet_offers:
            offer_type = offer.get("betOfferType", {}).get("name")
            outcomes = offer.get("outcomes", [])
            if not outcomes:
                continue

            if offer_type == "Match":
                for outcome in outcomes:
                    price = outcome.get("odds", 0) / 1000.0
                    if outcome.get("label") == "1":
                        designation = "home"
                    elif outcome.get("label") == "2":
                        designation = "away"
                    elif outcome.get("label") == "X":
                        designation = "draw"
                    else:
                        continue
                    line_id = self.generate_stable_line_id(
                        home_team, away_team, "moneyline", designation
                    )
                    odds["moneyline"][designation] = {
                        "line_id": line_id,
                        "price": price,
                        "designation": designation,
                    }
            elif offer_type == "Over/Under":
                if len(outcomes) >= 2:
                    points = outcomes[0].get("line", 0) / 1000.0
                    for outcome in outcomes[:2]:
                        label = outcome.get("label", "").lower()
                        price = outcome.get("odds", 0) / 1000.0
                        if label in ["over", "under"]:
                            line_id = self.generate_stable_line_id(
                                home_team, away_team, "total", label, points
                            )
                            odds["total"][label] = {
                                "line_id": line_id,
                                "price": price,
                                "points": points,
                                "designation": label,
                            }
        return {"odds": odds}

    def generate_game_id(self, home_team: str, away_team: str, start_time: str) -> str:
        """Generates a clean, unique ID for a game."""

        def clean_name(name: str) -> str:
            return re.sub(r"[^\w]", "", name.lower())

        home_clean, away_clean = clean_name(home_team), clean_name(away_team)
        try:
            date_str = datetime.fromisoformat(
                start_time.replace("Z", "+00:00")
            ).strftime("%Y%m%d")
        except (ValueError, TypeError):
            date_str = "unknown"
        return f"{home_clean}:{away_clean}:{date_str}"

    def generate_stable_line_id(
        self,
        home_team: str,
        away_team: str,
        bet_type: str,
        designation: str,
        points: Optional[float] = None,
    ) -> str:
        """Generates a stable, predictable ID for a betting line."""

        def clean_name(name: str) -> str:
            return re.sub(r"[^\w]", "", name.lower())

        home_clean, away_clean = clean_name(home_team), clean_name(away_team)
        points_str = str(points).replace(".", "_") if points is not None else ""
        if bet_type == "moneyline":
            if designation == "home":
                team = home_clean
            elif designation == "away":
                team = away_clean
            else:
                team = "draw"
            return f"{team}:{bet_type}:ballybet"
        elif bet_type == "total":
            return f"game_total:{designation}:{points_str}:ballybet"
        return (
            f"{home_clean}:{away_clean}:{bet_type}:{designation}:{points_str}:ballybet"
        )

    def save_data(self, sport_name: str, league_name: str, data: List[Dict]):
        """Saves processed data to a JSON file and the SQLite database."""
        if not data:
            return
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        clean_league_name = re.sub(r"[^a-zA-Z0-9]", "_", league_name)
        json_file = (
            self.data_dir
            / "json"
            / sport_name
            / f"{clean_league_name}_{timestamp}.json"
        )
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "sport": sport_name,
                    "league": league_name,
                    "scraped_at": self.get_current_timestamp(),
                    "match_count": len(data),
                    "matches": data,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )
        self.logger.info(
            f"Saved {len(data)} matches from '{league_name}' to {json_file}"
        )
        self.save_to_database(data)

    def save_to_database(self, matches: List[Dict]):
        """Saves a list of structured matches to the database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for match in matches:
                game_data = (
                    match["game_id"],
                    match["matchId"],
                    match["sport"],
                    match["homeTeam"],
                    match["awayTeam"],
                    match["startTime"],
                    match["scrapedAt"],
                    match["league"],
                    match["status"],
                )
                cursor.execute(
                    "INSERT OR REPLACE INTO games (game_id, match_id, sport_name, home_team, away_team, start_time, scraped_at, league_name, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    game_data,
                )
                for bet_type, bet_group in match.get("odds", {}).items():
                    for odd_data in bet_group.values():
                        odd_record = (
                            odd_data["line_id"],
                            match["game_id"],
                            bet_type,
                            odd_data["designation"],
                            odd_data.get("price"),
                            odd_data.get("points"),
                            match["scrapedAt"],
                        )
                        cursor.execute(
                            "INSERT OR REPLACE INTO odds (line_id, game_id, bet_type, designation, price, points, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            odd_record,
                        )
            conn.commit()
        self.logger.info(f"Updated database with {len(matches)} matches.")

    def run(self, max_workers: int = 5):
        """Main execution method to run the full scraping process."""
        self.logger.info("🚀 Starting Bally Bet Multi-Sport Scraper")
        start_time = time.time()

        leagues = list(self.discover_leagues())
        if not leagues:
            self.logger.error(
                "No leagues discovered for the configured sports. Exiting."
            )
            return

        total_matches_scraped = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_league = {
                executor.submit(self.scrape_league, league): league
                for league in leagues
            }
            for future in concurrent.futures.as_completed(future_to_league):
                league_info = future_to_league[future]
                try:
                    match_data = future.result()
                    if match_data is not None:
                        path = league_info["path"]
                        params_str = "lang=en_US&market=US-TN&useCombined=true"
                        full_endpoint = f"{self.base_url}/listView{path}/all/matches.json?{params_str}"
                        num_matches = len(match_data)
                        self.summary_logger.info(
                            f"LEAGUE: \"{league_info['league_name']}\" ({league_info['sport']}) | MATCHES: {num_matches} | ENDPOINT: {full_endpoint}"
                        )
                    if match_data:
                        self.save_data(
                            league_info["sport"], league_info["league_name"], match_data
                        )
                        total_matches_scraped += len(match_data)
                except Exception as e:
                    self.logger.error(
                        f"Error processing league {league_info['league_name']}: {e}"
                    )
                    self.summary_logger.error(
                        f"LEAGUE: \"{league_info.get('league_name', 'Unknown')}\" | STATUS: FAILED | ERROR: {e}"
                    )

        duration = time.time() - start_time
        self.logger.info("=" * 50)
        self.logger.info("🎉 Scraping Complete!")
        self.logger.info(f"⏱️  Duration: {duration:.2f} seconds")
        self.logger.info(f" leagues processed: {len(leagues)}")
        self.logger.info(f"🎯 Total matches scraped: {total_matches_scraped}")
        self.logger.info(f"💾 Data saved in '{self.data_dir}'")
        self.logger.info(
            f"📊 Summary log created at 'ballybet_data/logs/league_summary.log'"
        )
        self.logger.info("=" * 50)


if __name__ == "__main__":
    scraper = BallyBetScraper(data_dir="ballybet_data")
    scraper.run(max_workers=5)
