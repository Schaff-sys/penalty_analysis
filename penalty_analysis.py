import pandas as pd 
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os 
from pathlib import Path
from scipy import stats 


# -------------------
# Environment Setup
# -------------------
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path, override=True)

db_user = os.getenv("db_user")
db_password = os.getenv("db_password")
db_host = os.getenv("db_host")
db_port = os.getenv("db_port")
db_name = os.getenv("db_name")


if not all([db_user, db_password, db_host, db_port, db_name]):
        raise ValueError("Variables de entorno incompletas.")

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

## Loading and adjusting data
# Read the entire table into a DataFrame
df = pd.read_sql_query(text("SELECT * FROM events_dates_merged_ordered_withnames2343;"), con=engine)

# Alter dataframe to include teamId from previous action in every row
df['team_id_last'] = df['team_id'].shift(1)

# Alter dataframe to include game_time_seconds from previous action in every row
df['game_time_seconds_last'] = df['game_time_seconds'].shift(1)

# List of teams for for loops 
teams = df['team_id'].dropna().unique().astype(int).tolist()

teamnames = df[['team_id', 'team_name']]

teamnames = teamnames.drop_duplicates(subset=['team_id'])

penaltyshots = df[df['shot_type'] == 'Penalty_Shootout'] | df[df['shot_type']== 'Penalty']

penaltyshots.to_csv('penaltyshots.csv', index=False)