import requests
import json
import sqlite3
from datetime import datetime, timezone
from typing import List, Dict, Optional, Iterator
import re
import time
from pathlib import Path
import logging
import concurrent.futures

class BallyBetScraper:
    """
    Scrapes sports betting data from the Bally Bet (Kambi) API for multiple sports.
    This version uses the ID generation logic from the provided Pinnacle scraper example.
    """

    def __init__(self, data_dir: str = "ballybet_data"):
        self.SPORTS_TO_SCRAPE = [
            "football",
            "basketball",
            "american_football",
            "ice_hockey",
            "tennis",
            "baseball",
            "esports",
        ]

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
        log_file = self.data_dir / "logs" / "scraper.log"
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

        summary_log_file = self.data_dir / "logs" / "league_summary.log"
        self.summary_logger = logging.getLogger("league_summary")
        summary_handler = logging.FileHandler(summary_log_file, encoding='utf-8')
        summary_formatter = logging.Formatter("%(asctime)s - %(message)s")
        summary_handler.setFormatter(summary_formatter)
        self.summary_logger.addHandler(summary_handler)
        self.summary_logger.setLevel(logging.INFO)
        self.summary_logger.propagate = False

    def init_database(self):
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
        return datetime.now(timezone.utc).isoformat()

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        try:
            response = self.session.get(url, params=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP Request failed for {url}: {e}")
            return None

    # --- ID GENERATION METHODS FROM PINNACLE EXAMPLE ---

    def generate_game_id(self, home_team: str, away_team: str, start_time: str) -> str:
        """Generate simple game_id based on the Pinnacle example."""
        def clean_name(name: str) -> str:
            name = re.sub(r"[^\w\s]", "", name)
            name = re.sub(r"\s+", "_", name.strip()).lower()
            return name

        home_clean = clean_name(home_team)
        away_clean = clean_name(away_team)
        sep = ":"

        try:
            if start_time:
                dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y%m%d_%H%M")
            else:
                date_str = "unknown"
        except (ValueError, TypeError):
            date_str = "unknown"

        return f"{home_clean}{sep}{away_clean}{sep}{date_str}"

    def clean_team_name(self, name: str) -> str:
        """Clean team name for use in line IDs, from the Pinnacle example."""
        name = re.sub(
            r"\s+(FC|CF|United|City|Town|Rovers|Wanderers|Athletic|Albion)$",
            "", name, flags=re.IGNORECASE,
        )
        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(r"\s+", "_", name.strip()).lower()
        return name

    def generate_stable_line_id(
        self, home_team: str, away_team: str, bet_type: str,
        designation: str, points: Optional[float] = None, side: str = None
    ) -> str:
        """Generate stable, non-dynamic line IDs, based on the Pinnacle example."""
        home_clean = self.clean_team_name(home_team)
        away_clean = self.clean_team_name(away_team)
        points_str = f"{points}".replace(".", "_") if points is not None else ""
        sep = ":"
        bookmaker = "ballybet" # Changed from 'pinnacle'

        if bet_type == "moneyline":
            if designation.lower() == "home": return f"{home_clean}{sep}moneyline{sep}{bookmaker}"
            elif designation.lower() == "away": return f"{away_clean}{sep}moneyline{sep}{bookmaker}"
            elif designation.lower() in ["draw", "tie"]: return f"draw{sep}moneyline{sep}{bookmaker}"
            else: return f"{designation.lower()}{sep}moneyline{sep}{bookmaker}"
        elif bet_type == "spread":
            if designation.lower() == "home": return f"{home_clean}{sep}spread{sep}{points_str}{sep}{bookmaker}"
            elif designation.lower() == "away": return f"{away_clean}{sep}spread{sep}{points_str}{sep}{bookmaker}"
            else: return f"{designation.lower()}{sep}spread{sep}{points_str}{sep}{bookmaker}"
        elif bet_type == "total":
            designation_clean = designation.lower()
            return f"game_total{sep}{designation_clean}{sep}{points_str}{sep}{bookmaker}"
        else: # Fallback for other types
            return f"{home_clean}{sep}{away_clean}{sep}{bet_type}{sep}{designation.lower()}{sep}{points_str}{sep}{bookmaker}"

    # --- END ID GENERATION METHODS ---

    def discover_leagues(self) -> Iterator[Dict[str, str]]:
        self.logger.info(f"Discovering leagues for configured sports: {self.SPORTS_TO_SCRAPE}")
        discovery_url = f"{self.base_url}/group.json"
        params = {"lang": "en_US", "market": "US-TN"}
        data = self._make_request(discovery_url, params)
        if not data or "group" not in data:
            self.logger.error("Failed to fetch discovery data.")
            return

        for sport_group in data["group"]["groups"]:
            sport_name = sport_group.get("termKey")
            if not sport_name or sport_name not in self.SPORTS_TO_SCRAPE:
                continue

            self.logger.info(f"Found sport: {sport_group.get('name')}. Checking for leagues...")
            (self.data_dir / "json" / sport_name).mkdir(exist_ok=True)
            
            for country in sport_group.get("groups", []):
                for league in country.get("groups", []):
                    if league.get("eventCount", 0) > 0:
                        path = f"/{sport_name}/{country['termKey']}/{league['termKey']}"
                        self.logger.info(f"Discovered league: {league['name']} ({path})")
                        yield {"sport": sport_name, "league_name": league["name"], "path": path}

    def scrape_league(self, league_info: Dict) -> Optional[List[Dict]]:
        sport, path, league_name = league_info["sport"], league_info["path"], league_info["league_name"]
        self.logger.info(f"Scraping league: {league_name} ({sport})")
        url = f"{self.base_url}/listView{path}/all/matches.json"
        params = {"lang": "en_US", "market": "US-TN", "useCombined": "true"}
        data = self._make_request(url, params)
        if not data or "events" not in data:
            self.logger.warning(f"No data or 'events' key found for league: {league_name}")
            return []
        return self.process_events_data(data["events"], sport, league_name)

    def process_events_data(self, events: List[Dict], sport_name: str, league_name: str) -> List[Dict]:
        structured_matches = []
        scraped_at_iso = self.get_current_timestamp()
        for item in events:
            event = item.get("event")
            bet_offers = item.get("betOffers")
            if not event or not bet_offers: continue

            home_team, away_team, start_time = event.get("homeName"), event.get("awayName"), event.get("start")
            if not all([home_team, away_team, start_time]):
                self.logger.warning(f"Skipping event with missing data: {event.get('id')}")
                continue

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
        odds = {"moneyline": {}, "total": {}}
        for offer in bet_offers:
            offer_type = offer.get("betOfferType", {}).get("name")
            outcomes = offer.get("outcomes", [])
            if not outcomes: continue

            # Map the API offer type to the generic type for the ID generator
            if offer_type == "Match":
                bet_type_for_id = "moneyline"
                for outcome in outcomes:
                    if outcome.get("label") == "1": designation = "home"
                    elif outcome.get("label") == "2": designation = "away"
                    elif outcome.get("label") == "X": designation = "draw"
                    else: continue
                    
                    line_id = self.generate_stable_line_id(home_team, away_team, bet_type_for_id, designation)
                    odds["moneyline"][designation] = {"line_id": line_id, "price": outcome.get("odds", 0) / 1000.0, "designation": designation}
            
            elif offer_type == "Over/Under":
                bet_type_for_id = "total"
                if len(outcomes) >= 2:
                    points = outcomes[0].get("line", 0) / 1000.0
                    for outcome in outcomes[:2]:
                        designation = outcome.get("label", "").lower()
                        if designation in ["over", "under"]:
                            line_id = self.generate_stable_line_id(home_team, away_team, bet_type_for_id, designation, points)
                            odds["total"][designation] = {"line_id": line_id, "price": outcome.get("odds", 0) / 1000.0, "points": points, "designation": designation}
        return {"odds": odds}

    def save_data(self, sport_name: str, league_name: str, data: List[Dict]):
        if not data: return
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        clean_league_name = re.sub(r'[^a-zA-Z0-9]', '_', league_name)
        json_file = self.data_dir / "json" / sport_name / f"{clean_league_name}_{timestamp}.json"
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump({"sport": sport_name, "league": league_name, "scraped_at": self.get_current_timestamp(), "match_count": len(data), "matches": data}, f, indent=2, ensure_ascii=False)
        self.logger.info(f"Saved {len(data)} matches from '{league_name}' to {json_file}")
        self.save_to_database(data)

    def save_to_database(self, matches: List[Dict]):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for match in matches:
                game_data = (match["game_id"], match["matchId"], match["sport"], match["homeTeam"], match["awayTeam"], match["startTime"], match["scrapedAt"], match["league"], match["status"])
                cursor.execute("INSERT OR REPLACE INTO games (game_id, match_id, sport_name, home_team, away_team, start_time, scraped_at, league_name, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", game_data)
                for bet_type, bet_group in match.get("odds", {}).items():
                    for odd_data in bet_group.values():
                        odd_record = (odd_data["line_id"], match["game_id"], bet_type, odd_data["designation"], odd_data.get("price"), odd_data.get("points"), match["scrapedAt"])
                        cursor.execute("INSERT OR REPLACE INTO odds (line_id, game_id, bet_type, designation, price, points, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?)", odd_record)
            conn.commit()
        self.logger.info(f"Updated database with {len(matches)} matches.")

    def run(self, max_workers: int = 5):
        self.logger.info("🚀 Starting Bally Bet Multi-Sport Scraper")
        start_time = time.time()
        leagues = list(self.discover_leagues())
        if not leagues:
            self.logger.error("No leagues discovered for the configured sports. Exiting.")
            return

        total_matches_scraped = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_league = {executor.submit(self.scrape_league, league): league for league in leagues}
            for future in concurrent.futures.as_completed(future_to_league):
                league_info = future_to_league[future]
                try:
                    match_data = future.result()
                    if match_data is not None:
                        path, params_str = league_info["path"], "lang=en_US&market=US-TN&useCombined=true"
                        full_endpoint = f"{self.base_url}/listView{path}/all/matches.json?{params_str}"
                        self.summary_logger.info(f"LEAGUE: \"{league_info['league_name']}\" ({league_info['sport']}) | MATCHES: {len(match_data)} | ENDPOINT: {full_endpoint}")
                    if match_data:
                        self.save_data(league_info["sport"], league_info["league_name"], match_data)
                        total_matches_scraped += len(match_data)
                except Exception as e:
                    self.logger.error(f"Error processing league {league_info['league_name']}: {e}")
                    self.summary_logger.error(f"LEAGUE: \"{league_info.get('league_name', 'Unknown')}\" | STATUS: FAILED | ERROR: {e}")

        duration = time.time() - start_time
        self.logger.info("=" * 50)
        self.logger.info("🎉 Scraping Complete!")
        self.logger.info(f"⏱️  Duration: {duration:.2f} seconds")
        self.logger.info(f" leagues processed: {len(leagues)}")
        self.logger.info(f"🎯 Total matches scraped: {total_matches_scraped}")
        self.logger.info(f"💾 Data saved in '{self.data_dir}'")
        self.logger.info(f"📊 Summary log created at 'ballybet_data/logs/league_summary.log'")
        self.logger.info("=" * 50)

if __name__ == "__main__":
    scraper = BallyBetScraper(data_dir="ballybet_data")
    scraper.run(max_workers=5)