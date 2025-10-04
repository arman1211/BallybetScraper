import requests
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional
import re
import time
from pathlib import Path
import logging
import concurrent.futures


class LiveBallyBetScraper:
    """
    Scrapes LIVE, in-play sports data from the Bally Bet (Kambi) API.

    This script continuously polls the API to find currently live events across
    multiple configured sports, fetches their real-time data (scores, odds),
    and saves periodic snapshots into timestamped JSON files.
    """

    def __init__(self, data_dir: str = "ballybet_data", poll_interval: int = 45):
        # List of sports to scrape for live data.
        self.SPORTS_TO_SCRAPE = [
            "football",
            "basketball",
            "tennis",
            "cricket",
            "ice_hockey",
            "esports",
        ]

        self.base_url = "https://eu1.offering-api.kambicdn.com/offering/v2018/bcsustn"
        self.session = requests.Session()
        self.data_dir = Path(data_dir)
        self.poll_interval_seconds = poll_interval

        self.live_data_dir = self.data_dir / "json_live"
        self.live_data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "logs").mkdir(exist_ok=True)

        self.setup_logging()

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
        }
        self.session.headers.update(self.headers)

    def setup_logging(self):
        """Sets up a dedicated logger for the live scraper."""
        log_file = self.data_dir / "logs" / "live_scraper.log"
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

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Makes an HTTP GET request with error handling."""
        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP Request failed for {url}: {e}")
            return None

    def scrape_live_sport(self, sport: str) -> List[Dict]:
        """Fetches all live events for a single sport."""
        self.logger.info(f"Checking for live events in: {sport.upper()}")

        # This is the new, more efficient endpoint you provided
        url = f"{self.base_url}/listView/{sport}/all/all/all/in-play.json"
        params = {"lang": "en_US", "market": "US-TN", "useCombined": "true"}

        data = self._make_request(url, params)
        if not data or "events" not in data:
            self.logger.info(f"No live events found for {sport.upper()}.")
            return []

        return data["events"]

    def process_live_events(self, events: List[Dict]) -> List[Dict]:
        """Translates the raw API data for a list of live events into our clean format."""
        structured_matches = []
        for item in events:
            event = item.get("event")
            bet_offers = item.get("betOffers")
            live_data = item.get("liveData")

            if not all([event, bet_offers, live_data]):
                continue

            home_team = event.get("homeName")
            away_team = event.get("awayName")
            start_time = event.get("start")

            if not all([home_team, away_team, start_time]):
                continue

            game_id = self.generate_game_id(home_team, away_team, start_time)
            odds_data = self.extract_odds(bet_offers, home_team, away_team)

            structured_match = {
                "game_id": game_id,
                "matchId": event.get("id"),
                "sport": event.get("sport"),
                "league": event.get("group"),
                "homeTeam": home_team,
                "awayTeam": away_team,
                "startTime": start_time,
                "live_data": {
                    "score": live_data.get("score"),
                    "game_clock": live_data.get("matchClock"),
                    "statistics": live_data.get("statistics"),
                },
                "odds": odds_data.get("odds", {}),
            }
            structured_matches.append(structured_match)

        return structured_matches

    def extract_odds(
        self, bet_offers: List[Dict], home_team: str, away_team: str
    ) -> Dict:
        """Extracts and structures odds, handling suspended states."""
        odds = {"moneyline": {}, "total": {}}
        for offer in bet_offers:
            offer_type = offer.get("betOfferType", {}).get("name")
            is_suspended = offer.get("suspended", False)

            if offer_type == "Match":
                for outcome in offer.get("outcomes", []):
                    price = (
                        outcome.get("odds", 0) / 1000.0 if "odds" in outcome else None
                    )
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
                        "status": outcome.get("status", "SUSPENDED"),
                    }
            elif offer_type == "Over/Under":
                outcomes = offer.get("outcomes", [])
                if len(outcomes) >= 2:
                    points = outcomes[0].get("line", 0) / 1000.0
                    for outcome in outcomes[:2]:
                        label = outcome.get("label", "").lower()
                        price = (
                            outcome.get("odds", 0) / 1000.0
                            if "odds" in outcome
                            else None
                        )
                        if label in ["over", "under"]:
                            line_id = self.generate_stable_line_id(
                                home_team, away_team, "total", label, points
                            )
                            odds["total"][label] = {
                                "line_id": line_id,
                                "price": price,
                                "points": points,
                                "designation": label,
                                "status": outcome.get("status", "SUSPENDED"),
                            }
        return {"odds": odds}

    def generate_game_id(self, home_team: str, away_team: str, start_time: str) -> str:
        def clean_name(name: str) -> str:
            return re.sub(r"[^\w]", "", name.lower())

        home_clean, away_clean = clean_name(home_team), clean_name(away_team)
        try:
            date_str = datetime.fromisoformat(
                start_time.replace("Z", "+00:00")
            ).strftime("%Y%m%d")
        except:
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
        def clean_name(name: str) -> str:
            return re.sub(r"[^\w]", "", name.lower())

        home_clean, away_clean = clean_name(home_team), clean_name(away_team)
        points_str = str(points).replace(".", "_") if points is not None else ""
        if bet_type == "moneyline":
            team = clean_name(designation) if designation != "draw" else "draw"
            return f"{team}:{bet_type}:ballybet_live"
        elif bet_type == "total":
            return f"game_total:{designation}:{points_str}:ballybet_live"
        return f"{home_clean}:{away_clean}:{bet_type}:{designation}:{points_str}:ballybet_live"

    def save_snapshot(self, snapshot_data: List[Dict]):
        """Saves the current snapshot of all live games to a timestamped JSON file."""
        if not snapshot_data:
            self.logger.info("Snapshot is empty. Nothing to save.")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = self.live_data_dir / f"live_snapshot_{timestamp}.json"

        snapshot_wrapper = {
            "snapshot_time_utc": datetime.now(timezone.utc).isoformat(),
            "live_events_count": len(snapshot_data),
            "events": snapshot_data,
        }

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(snapshot_wrapper, f, indent=2)

        self.logger.info(
            f"✅ Successfully saved snapshot of {len(snapshot_data)} live events to {file_path}"
        )

    def run(self):
        """The main continuous loop for polling and scraping live data."""
        self.logger.info(
            f"🚀 Starting Live Scraper. Polling every {self.poll_interval_seconds} seconds. Press Ctrl+C to stop."
        )

        try:
            while True:
                all_live_events = []
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=len(self.SPORTS_TO_SCRAPE)
                ) as executor:
                    future_to_sport = {
                        executor.submit(self.scrape_live_sport, sport): sport
                        for sport in self.SPORTS_TO_SCRAPE
                    }
                    for future in concurrent.futures.as_completed(future_to_sport):
                        events = future.result()
                        if events:
                            all_live_events.extend(events)

                if not all_live_events:
                    self.logger.info(
                        f"No live events found across all configured sports. Waiting..."
                    )
                else:
                    processed_data = self.process_live_events(all_live_events)
                    self.save_snapshot(processed_data)

                self.logger.info(
                    f"Next poll in {self.poll_interval_seconds} seconds..."
                )
                time.sleep(self.poll_interval_seconds)

        except KeyboardInterrupt:
            self.logger.info("⏹️ Scraper stopped by user. Exiting gracefully.")
        except Exception as e:
            self.logger.error(
                f"An unexpected error occurred in the main loop: {e}", exc_info=True
            )


if __name__ == "__main__":
    live_scraper = LiveBallyBetScraper(poll_interval=60)  # Poll every 60 seconds
    live_scraper.run()
