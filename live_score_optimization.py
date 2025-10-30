import sqlite3
import json
from datetime import datetime, timezone
from pathlib import Path

def create_optimized_live_db():
    """Create optimized database schema for live scores"""

    db_path = Path("ballybet_data/db/ballybet_live_optimized.db")

    with sqlite3.connect(db_path) as conn:
        # Enable performance optimizations
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA temp_store=MEMORY")

        # Main games table
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

        # Separate live scores table for efficient querying
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

        # Odds table (same as before)
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

    print(f"✅ Created optimized live database at {db_path}")
    return db_path

def demonstrate_efficient_storage():
    """Show how to efficiently store and query live score data"""

    db_path = create_optimized_live_db()

    # Sample live data from API
    sample_live_data = {
        "score": {
            "home": "2",
            "away": "1",
            "who": "HOME",
            "version": 1759912346461
        },
        "matchClock": {
            "minute": 72,
            "second": 30,
            "period": "2nd half",
            "running": True,
            "periodId": "SECOND_HALF"
        },
        "statistics": {
            "football": {
                "home": {"yellowCards": 1, "redCards": 0, "corners": 5},
                "away": {"yellowCards": 2, "redCards": 0, "corners": 3}
            }
        }
    }

    game_id = "sample_game_123"
    scraped_at = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Insert game (if not exists)
        cursor.execute("""
            INSERT OR IGNORE INTO live_games
            (game_id, match_id, sport_name, league_name, home_team, away_team, start_time, scraped_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (game_id, 12345, "football", "Premier League", "Team A", "Team B",
              "2025-10-08T20:00:00Z", scraped_at, "LIVE"))

        # Insert live score
        score_data = sample_live_data["score"]
        clock_data = sample_live_data["matchClock"]

        cursor.execute("""
            INSERT OR REPLACE INTO live_scores
            (game_id, scraped_at, home_score, away_score, last_scorer, score_version,
             period, minute, second, clock_running)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            game_id, scraped_at,
            int(score_data["home"]), int(score_data["away"]),
            score_data["who"], score_data["version"],
            clock_data["period"], clock_data["minute"], clock_data["second"],
            clock_data["running"]
        ))

        # Insert statistics
        stats_data = sample_live_data["statistics"]["football"]
        for stat_type in ["yellowCards", "redCards", "corners"]:
            cursor.execute("""
                INSERT OR REPLACE INTO live_statistics
                (game_id, scraped_at, stat_type, home_value, away_value)
                VALUES (?, ?, ?, ?, ?)
            """, (
                game_id, scraped_at, stat_type,
                stats_data["home"][stat_type],
                stats_data["away"][stat_type]
            ))

        conn.commit()

    print("✅ Sample data inserted")

    # Demonstrate efficient queries
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        print("\n🔍 Efficient Query Examples:")

        # Get latest score for a game
        cursor.execute("""
            SELECT home_score, away_score, minute, second, period
            FROM live_scores
            WHERE game_id = ?
            ORDER BY scraped_at DESC
            LIMIT 1
        """, (game_id,))
        latest_score = cursor.fetchone()
        print(f"Latest score: {latest_score[0]}-{latest_score[1]} at {latest_score[2]}:{latest_score[3]}' ({latest_score[4]})")

        # Get score history (last 5 updates)
        cursor.execute("""
            SELECT scraped_at, home_score, away_score
            FROM live_scores
            WHERE game_id = ?
            ORDER BY scraped_at DESC
            LIMIT 5
        """, (game_id,))
        score_history = cursor.fetchall()
        print(f"Score history (last 5): {len(score_history)} updates")

        # Get current statistics
        cursor.execute("""
            SELECT stat_type, home_value, away_value
            FROM live_statistics
            WHERE game_id = ? AND scraped_at = (
                SELECT MAX(scraped_at) FROM live_statistics WHERE game_id = ?
            )
        """, (game_id, game_id))
        current_stats = cursor.fetchall()
        print("Current stats:")
        for stat in current_stats:
            print(f"  {stat[0]}: {stat[1]}-{stat[2]}")

if __name__ == "__main__":
    demonstrate_efficient_storage()