import pyodbc
import requests

# SQL Server connection details
SERVER = "DESKTOP-4LC4SRM\\MSSQLSERVER01"
DATABASE = "NBA_Predictions"

# Establish connection to SQL Server
def connect_db():
    conn = pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    )
    return conn

# Fetch NBA game data from API
def fetch_games():
    url = "https://api.sportsdata.io/v3/nba/scores/json/GamesByDate/2024-FEB-10"
    headers = {"Ocp-Apim-Subscription-Key": "007f2c9e0b504f05a9644c3de5f6ac7f"}
    response = requests.get(url, headers=headers)
    return response.json() if response.status_code == 200 else None

# Insert game data into SQL Server
def store_games(games):
    conn = connect_db()
    cursor = conn.cursor()

    for game in games:
        cursor.execute("""
        INSERT INTO Games (date, home_team_id, away_team_id, home_score, away_score, overtime, season)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
    game["Day"],
    game["HomeTeamID"],
    game["AwayTeamID"],
    game["HomeTeamScore"],
    game["AwayTeamScore"],
    game.get("IsOvertime", False),  # Use .get() to avoid KeyError
    game["Season"]
))


    conn.commit()
    cursor.close()
    conn.close()

# Run the pipeline
if __name__ == "__main__":
    games_data = fetch_games()
    if games_data:
        store_games(games_data)
        print("Game data successfully stored in SQL Server!")
    else:
        print("Failed to fetch game data.")
