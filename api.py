from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import sqlite3
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import json

app = FastAPI(title="BallyBet Data API", version="1.0.0")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = Path("ballybet_data/db")

def get_db_connection(db_name: str):
    db_path = DATA_DIR / db_name
    if not db_path.exists():
        raise HTTPException(status_code=404, detail=f"Database {db_name} not found")
    return sqlite3.connect(db_path)

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

@app.get("/")
def root():
    return {
        "message": "BallyBet Data API",
        "endpoints": {
            "overview": "/api/overview",
            "pregame": "/api/pregame/games",
            "live": "/api/live/games",
            "futures": "/api/futures/competitions",
            "props": "/api/props/games"
        }
    }

@app.get("/api/overview")
def get_overview():
    """Get summary statistics from all databases"""
    stats = {}
    
    try:
        # Pregame stats
        conn = get_db_connection("ballybet.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as count FROM games")
        stats['pregame_games'] = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM odds")
        stats['pregame_odds'] = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(DISTINCT sport_name) as count FROM games")
        stats['sports_count'] = cursor.fetchone()['count']
        
        conn.close()
    except:
        stats['pregame_games'] = 0
        stats['pregame_odds'] = 0
        stats['sports_count'] = 0
    
    try:
        # Live stats
        conn = get_db_connection("ballybet_live.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as count FROM live_games WHERE status='LIVE'")
        stats['live_games'] = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM live_odds")
        stats['live_odds'] = cursor.fetchone()['count']
        
        conn.close()
    except:
        stats['live_games'] = 0
        stats['live_odds'] = 0
    
    try:
        # Futures stats
        conn = get_db_connection("ballybet_futures.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as count FROM competitions")
        stats['futures_competitions'] = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM futures_odds")
        stats['futures_odds'] = cursor.fetchone()['count']
        
        conn.close()
    except:
        stats['futures_competitions'] = 0
        stats['futures_odds'] = 0
    
    try:
        # Props stats
        conn = get_db_connection("ballybet_props.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as count FROM player_props_games")
        stats['props_games'] = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM player_props_odds")
        stats['props_odds'] = cursor.fetchone()['count']
        
        conn.close()
    except:
        stats['props_games'] = 0
        stats['props_odds'] = 0
    
    return stats

@app.get("/api/pregame/games")
def get_pregame_games(sport: Optional[str] = None, limit: int = 50):
    """Get pregame games with odds"""
    try:
        conn = get_db_connection("ballybet.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        
        query = """
            SELECT g.*, 
                   GROUP_CONCAT(o.bet_type || ':' || o.designation || ':' || o.price || ':' || COALESCE(o.points, '')) as odds_data
            FROM games g
            LEFT JOIN odds o ON g.game_id = o.game_id
            WHERE g.start_time > datetime('now')
        """
        
        if sport:
            query += f" AND g.sport_name = '{sport}'"
        
        query += " GROUP BY g.game_id ORDER BY g.start_time ASC LIMIT ?"
        
        cursor.execute(query, (limit,))
        games = cursor.fetchall()
        
        # Parse odds data
        for game in games:
            odds = {}
            if game['odds_data']:
                for odd in game['odds_data'].split(','):
                    parts = odd.split(':')
                    if len(parts) >= 3:
                        bet_type = parts[0]
                        if bet_type not in odds:
                            odds[bet_type] = {}
                        odds[bet_type][parts[1]] = {
                            'price': float(parts[2]) if parts[2] else None,
                            'points': float(parts[3]) if len(parts) > 3 and parts[3] else None
                        }
            game['odds'] = odds
            del game['odds_data']
        
        conn.close()
        return games
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pregame/sports")
def get_sports():
    """Get list of available sports"""
    try:
        conn = get_db_connection("ballybet.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT sport_name, COUNT(*) as game_count 
            FROM games 
            WHERE start_time > datetime('now')
            GROUP BY sport_name
            ORDER BY game_count DESC
        """)
        
        sports = cursor.fetchall()
        conn.close()
        return sports
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/live/games")
def get_live_games():
    """Get live games with scores and odds"""
    try:
        conn = get_db_connection("ballybet_live.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT g.*, 
                   s.home_score, s.away_score, s.period, s.minute, s.second,
                   s.clock_running, s.last_scorer
            FROM live_games g
            LEFT JOIN (
                SELECT game_id, home_score, away_score, period, minute, second, 
                       clock_running, last_scorer, MAX(scraped_at) as max_scraped
                FROM live_scores
                GROUP BY game_id
            ) s ON g.game_id = s.game_id
            WHERE g.status = 'LIVE'
            ORDER BY g.scraped_at DESC
        """)
        
        games = cursor.fetchall()
        
        # Get odds for each game
        for game in games:
            cursor.execute("""
                SELECT bet_type, designation, price, points, status
                FROM live_odds
                WHERE game_id = ?
                ORDER BY scraped_at DESC
            """, (game['game_id'],))
            
            odds_rows = cursor.fetchall()
            odds = {}
            for odd in odds_rows:
                bet_type = odd['bet_type']
                if bet_type not in odds:
                    odds[bet_type] = {}
                odds[bet_type][odd['designation']] = {
                    'price': odd['price'],
                    'points': odd['points'],
                    'status': odd['status']
                }
            game['odds'] = odds
            
            # Get stats
            cursor.execute("""
                SELECT stat_type, home_value, away_value
                FROM live_statistics
                WHERE game_id = ?
                ORDER BY scraped_at DESC
                LIMIT 10
            """, (game['game_id'],))
            
            game['statistics'] = cursor.fetchall()
        
        conn.close()
        return games
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/futures/competitions")
def get_futures(sport: Optional[str] = None, limit: int = 20):
    """Get futures/outright markets"""
    try:
        conn = get_db_connection("ballybet_futures.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        
        query = "SELECT * FROM competitions"
        params = []
        
        if sport:
            query += " WHERE sport = ?"
            params.append(sport)
        
        query += " ORDER BY scraped_at DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        competitions = cursor.fetchall()
        
        # Get odds for each competition
        for comp in competitions:
            cursor.execute("""
                SELECT market_name, participant_name, price
                FROM futures_odds
                WHERE competition_id = ?
                ORDER BY price ASC
                LIMIT 20
            """, (comp['competition_id'],))
            
            comp['odds'] = cursor.fetchall()
        
        conn.close()
        return competitions
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/props/games")
def get_props_games(league: Optional[str] = None, limit: int = 20):
    """Get games with player props"""
    try:
        conn = get_db_connection("ballybet_props.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        
        query = "SELECT * FROM player_props_games WHERE start_time > datetime('now')"
        params = []
        
        if league:
            query += " AND league_name LIKE ?"
            params.append(f"%{league}%")
        
        query += " ORDER BY start_time ASC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        games = cursor.fetchall()
        
        # Get props for each game
        for game in games:
            cursor.execute("""
                SELECT market_name, player_name, bet_type, label, price, line
                FROM player_props_odds
                WHERE game_id = ?
                ORDER BY player_name, market_name
            """, (game['game_id'],))
            
            game['props'] = cursor.fetchall()
            
            # Get prepacks
            cursor.execute("""
                SELECT legs, decimal_odds, american_odds
                FROM player_props_prepacks
                WHERE game_id = ?
            """, (game['game_id'],))
            
            game['parlays'] = cursor.fetchall()
        
        conn.close()
        return games
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/props/leagues")
def get_props_leagues():
    """Get available leagues with props"""
    try:
        conn = get_db_connection("ballybet_props.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT league_name, sport_name, COUNT(*) as game_count
            FROM player_props_games
            WHERE start_time > datetime('now')
            GROUP BY league_name, sport_name
            ORDER BY game_count DESC
        """)
        
        leagues = cursor.fetchall()
        conn.close()
        return leagues
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)