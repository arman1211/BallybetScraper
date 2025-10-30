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
    Scrapes detailed market data from the Bally Bet (Kambi) API, including
    Player Props, Team Totals, and Pre-Packaged Parlays. Saves to a 3-table SQLite DB.
    """

    def __init__(self, data_dir: str = "ballybet_data"):
        self.LEAGUES_TO_SCRAPE = {
            # --- Football (Soccer) ---
            "EPL": "football/england/premier_league",
            "La_Liga": "football/spain/la_liga",
            "Serie_A": "football/italy/serie_a",
            "Bundesliga": "football/germany/bundesliga",
            "Ligue_1": "football/france/ligue_1",
            "Champions_League": "football/champions_league",
            "MLS": "football/usa/mls",

            # --- American Sports ---
            "NFL": "american_football/nfl",
            "NBA": "basketball/nba",
            "MLB": "baseball/mlb",
            "NHL": "ice_hockey/nhl",
            
            # --- College Sports (Very popular for props) ---
            "NCAAF": "american_football/ncaaf",
            "NCAAB": "basketball/ncaab",
        }
        self.base_url = "https://eu1.offering-api.kambicdn.com/offering/v2018/bcsustn"
        self.session = requests.Session()
        self.data_dir = Path(data_dir)

        # Directories
        self.props_data_dir = self.data_dir / "json_props"
        self.props_data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "logs").mkdir(exist_ok=True)
        (self.data_dir / "db").mkdir(exist_ok=True)

        self.setup_logging()

        self.headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        self.session.headers.update(self.headers)

        self.db_path = self.data_dir / "db" / "ballybet_props.db"
        self.init_database()

    def setup_logging(self):
        log_file = self.data_dir / "logs" / "props_scraper.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler()],
            encoding='utf-8'
        )
        self.logger = logging.getLogger(__name__)

    def init_database(self):
        """Initializes DB with tables for games, odds (props/totals), and pre-packs."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS player_props_games (
                    game_id TEXT PRIMARY KEY, match_id INTEGER, match_name TEXT, 
                    start_time TEXT, league_name TEXT, sport_name TEXT, scraped_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS player_props_odds (
                    line_id TEXT PRIMARY KEY, game_id TEXT, market_name TEXT, player_name TEXT, 
                    bet_type TEXT, label TEXT, price REAL, line REAL, scraped_at TEXT,
                    FOREIGN KEY (game_id) REFERENCES player_props_games(game_id)
                )
                """
            )
            # --- NEW TABLE FOR PRE-PACKAGED PARLAYS ---
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS player_props_prepacks (
                    prepack_id TEXT PRIMARY KEY,
                    game_id TEXT,
                    legs TEXT,
                    decimal_odds REAL,
                    american_odds TEXT,
                    scraped_at TEXT,
                    FOREIGN KEY (game_id) REFERENCES player_props_games(game_id)
                )
                """
            )
            conn.commit()
            self.logger.info(f"Props database initialized at {self.db_path}")

    # --- ID GENERATION METHODS ---
    def generate_game_id(self, home_team: str, away_team: str, start_time: str) -> str:
        def clean_name(name: str) -> str:
            name = re.sub(r"[^\w\s]", "", name)
            name = re.sub(r"\s+", "_", name.strip()).lower()
            return name
        home_clean, away_clean = clean_name(home_team), clean_name(away_team)
        try:
            dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y%m%d_%H%M")
        except: date_str = "unknown"
        return f"{home_clean}:{away_clean}:{date_str}"

    def clean_name_for_id(self, name: str) -> str:
        name = re.sub(r"[^\w\s]", "", str(name))
        name = re.sub(r"\s+", "_", name.strip()).lower()
        return name

    def generate_line_id(self, **kwargs) -> str:
        parts = [self.clean_name_for_id(v) for k, v in kwargs.items() if v is not None]
        return ":".join(parts)

    # --- NETWORKING & DISCOVERY METHODS ---
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
        data = self._make_request(url, params={"lang": "en_US", "market": "US-TN"})
        if not data or "events" not in data: return []
        event_ids = [event["event"]["id"] for event in data["events"]]
        self.logger.info(f"Found {len(event_ids)} events.")
        return event_ids

    def get_all_markets_for_event(self, event_id: int) -> Optional[Dict]:
        self.logger.debug(f"Fetching all markets for event ID: {event_id}")
        url = f"{self.base_url}/betoffer/event/{event_id}.json"
        return self._make_request(url, params={"lang": "en_US", "market": "US-TN"})

    # --- DATA PROCESSING ---
    def process_event_markets(self, event_json: Dict) -> Optional[Tuple[Dict, List[Dict], List[Dict]]]:
        if not event_json or not event_json.get("events"): return None
        event = event_json["events"][0]
        odds_list, prepacks_list = [], []
        scraped_at = datetime.now(timezone.utc).isoformat()
        
        home_team, away_team, start_time = event.get("homeName"), event.get("awayName"), event.get("start")
        if not all([home_team, away_team, start_time]): return None
        
        game_id = self.generate_game_id(home_team, away_team, start_time)

        # 1. Process BetOffers (Player Props and Team Totals)
        for offer in event_json.get("betOffers", []):
            criterion_label = offer.get("criterion", {}).get("label", "")
            
            # --- Player Prop Identification ---
            offer_type_name = offer.get("betOfferType", {}).get("name", "")
            is_player_prop = "player" in offer_type_name.lower() or "player" in criterion_label.lower() or \
                             re.search(r'(to score|shots|assists|saves|rebounds|yards)', criterion_label, re.I)
            
            # --- Team Total Identification ---
            is_team_total = "total goals by" in criterion_label.lower()

            if not (is_player_prop or is_team_total): continue

            for outcome in offer.get("outcomes", []):
                player_name = outcome.get("participant")
                
                # Determine team for team totals
                if is_team_total and not player_name:
                    if home_team in criterion_label: player_name = home_team
                    elif away_team in criterion_label: player_name = away_team
                    else: continue
                
                if not player_name: continue

                label = outcome.get("label")
                line = outcome.get("line", 0) / 1000.0 if "line" in outcome else None
                
                line_id = self.generate_line_id(player=player_name, market=criterion_label, label=label, line=line, bookmaker="ballybet_prop")
                
                odds_list.append({
                    "line_id": line_id, "game_id": game_id, "market_name": criterion_label,
                    "player_name": player_name, "bet_type": "team_total" if is_team_total else "player_prop",
                    "label": label, "price": outcome.get("odds", 0) / 1000.0 if "odds" in outcome else None,
                    "line": line, "scraped_at": scraped_at
                })

        # 2. Process Pre-Packaged Parlays
        for pack in event_json.get("prePacks", []):
            selection = pack.get("prePackSelections", [{}])[0]
            legs = selection.get("label")
            odds_info = selection.get("combinations", [{}])[0].get("odds", {})
            if not legs or not odds_info: continue

            prepack_id = self.generate_line_id(game=game_id, pack_id=pack.get("id"))
            
            prepacks_list.append({
                "prepack_id": prepack_id, "game_id": game_id, "legs": json.dumps(legs),
                "decimal_odds": odds_info.get("decimal", 0) / 1000.0,
                "american_odds": odds_info.get("american"), "scraped_at": scraped_at
            })

        if not (odds_list or prepacks_list): return None

        game_info = {
            "game_id": game_id, "match_id": event.get("id"), "match_name": event.get("name"),
            "start_time": start_time, "league_name": event.get("group"),
            "sport_name": event.get("sport").lower(), "scraped_at": scraped_at
        }
        return (game_info, odds_list, prepacks_list)
    
    # --- DATA SAVING METHODS ---
    def save_to_database(self, processed_data: List[Tuple[Dict, List[Dict], List[Dict]]]):
        if not processed_data: return
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for game_info, odds_list, prepacks_list in processed_data:
                cursor.execute("INSERT OR REPLACE INTO player_props_games VALUES (:game_id, :match_id, :match_name, :start_time, :league_name, :sport_name, :scraped_at)", game_info)
                if odds_list:
                    cursor.executemany("INSERT OR REPLACE INTO player_props_odds VALUES (:line_id, :game_id, :market_name, :player_name, :bet_type, :label, :price, :line, :scraped_at)", odds_list)
                if prepacks_list:
                    cursor.executemany("INSERT OR REPLACE INTO player_props_prepacks VALUES (:prepack_id, :game_id, :legs, :decimal_odds, :american_odds, :scraped_at)", prepacks_list)
            conn.commit()
        self.logger.info(f"💾 Updated database with props for {len(processed_data)} games.")

    def save_to_json(self, league_name: str, processed_data: List[Tuple[Dict, List[Dict], List[Dict]]]):
        if not processed_data: return
        json_output_games = []
        for game_info, odds_list, prepacks_list in processed_data:
            game_info_copy = game_info.copy()
            game_info_copy["odds"] = odds_list
            game_info_copy["pre_packaged_parlays"] = prepacks_list
            json_output_games.append(game_info_copy)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.props_data_dir / f"playerprops_{league_name}_{timestamp}.json"
        output = {"scraped_at_utc": datetime.now(timezone.utc).isoformat(), "league": league_name, "games_with_props": len(json_output_games), "games": json_output_games}
        with open(file_path, "w", encoding="utf-8") as f: json.dump(output, f, indent=2)
        self.logger.info(f"Saved props & parlays for {len(json_output_games)} games from {league_name} to {file_path}")

    def run(self):
        self.logger.info("🚀 Starting Advanced Props & Parlay Scraper")
        start_time = time.time()
        for league_name, league_path in self.LEAGUES_TO_SCRAPE.items():
            event_ids = self.get_event_ids_for_league(league_path)
            if not event_ids: continue
            
            all_game_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_id = {executor.submit(self.get_all_markets_for_event, eid): eid for eid in event_ids}
                for future in concurrent.futures.as_completed(future_to_id):
                    event_json = future.result()
                    if event_json:
                        processed_data = self.process_event_markets(event_json)
                        if processed_data: all_game_data.append(processed_data)
            
            self.save_to_json(league_name, all_game_data)
            self.save_to_database(all_game_data)

        duration = time.time() - start_time
        self.logger.info("=" * 50)
        self.logger.info(f"🎉 Scraping Complete! Duration: {duration:.2f} seconds.")
        self.logger.info(f"💾 Data saved in '{self.props_data_dir}' and '{self.db_path}'")
        self.logger.info("=" * 50)

if __name__ == "__main__":
    scraper = PlayerPropsScraper()
    scraper.run()