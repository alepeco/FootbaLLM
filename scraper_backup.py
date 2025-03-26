import pandas as pd
import requests
import time
import random
import os

def collect_team_data(match_logs_url, team_name, max_retries=3):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    for attempt in range(max_retries):
        try:
            # Delay before each attempt
            time.sleep(random.uniform(15, 20))

            # Make a manual request to check for 429 first
            response = requests.get(match_logs_url, headers=headers)
            if response.status_code == 429:
                print("⏳ Rate limited (429). Waiting 5 minutes before retrying...")
                time.sleep(300)
                continue
            elif response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.reason}")

            # If status is OK, read the table using pd.read_html (from the content)
            dfTeam = pd.read_html(response.text, attrs={'id': 'matchlogs_for'})[0]
            break  # Exit retry loop if successful

        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1} failed for {team_name}: {e}")
            if attempt == max_retries - 1:
                raise  # re-raise error if last attempt
            print("🔁 Retrying in 10 seconds...")
            time.sleep(10)

    # Continue processing the table
    dfTeam = dfTeam[dfTeam['Comp'] == 'La Liga']
    dfTeam = dfTeam.iloc[:, :-3]
    dfTeam.dropna(subset=['Result'], inplace=True)

    dfTeam['GF'] = pd.to_numeric(dfTeam['GF'], errors='coerce')
    dfTeam['GA'] = pd.to_numeric(dfTeam['GA'], errors='coerce')

    def result_to_points(result):
        return {'W': 3, 'D': 1, 'L': 0}.get(result, None)

    dfTeam['points'] = dfTeam['Result'].apply(result_to_points)
    dfTeam['GD'] = dfTeam['GF'] - dfTeam['GA']
    dfTeam['cum_points'] = dfTeam['points'].cumsum()
    dfTeam['PpG'] = dfTeam['cum_points'] / (dfTeam.index + 1)
    dfTeam['Venue'] = dfTeam['Venue'].map({'Home': 1, 'Away': 0})
    dfTeam['Team'] = team_name

    return dfTeam

# Define teams
teams_info = [
    ('https://fbref.com/en/squads/206d90db/Barcelona-Stats', 'Barcelona'),
    ('https://fbref.com/en/squads/98e8af82/Rayo-Vallecano-Stats', 'Rayo Vallecano'),
    ('https://fbref.com/en/squads/2aa12281/Mallorca-Stats', 'Mallorca'),
    ('https://fbref.com/en/squads/7848bd64/Getafe-Stats', 'Getafe'),
    ('https://fbref.com/en/squads/ad2be733/Sevilla-Stats', 'Sevilla'),
    ('https://fbref.com/en/squads/ee7c297c/Cadiz-Stats', 'Cádiz'),
    ('https://fbref.com/en/squads/53a2f082/Real-Madrid-Stats', 'Real Madrid'),
    ('https://fbref.com/en/squads/db3b9613/Atletico-Madrid-Stats', 'Atlético Madrid'),
    ('https://fbref.com/en/squads/e31d1cd9/Real-Sociedad-Stats', 'Real Sociedad'),
    ('https://fbref.com/en/squads/9024a00a/Girona-Stats', 'Girona'),
    ('https://fbref.com/en/squads/2b390eca/Athletic-Club-Stats', 'Athletic Club'),
    ('https://fbref.com/en/squads/fc536746/Real-Betis-Stats', 'Betis'),
    ('https://fbref.com/en/squads/dcc91a7b/Valencia-Stats', 'Valencia'),
    ('https://fbref.com/en/squads/2a8183b3/Villarreal-Stats', 'Villareal'),
    ('https://fbref.com/en/squads/0049d422/Las-Palmas-Stats', 'Las Palmas'),
    ('https://fbref.com/en/squads/03c57e2b/Osasuna-Stats', 'Osasuna'),
    ('https://fbref.com/en/squads/8d6fd021/Alaves-Stats', 'Alavés'),
    ('https://fbref.com/en/squads/f25da7fb/Celta-Vigo-Stats', 'Celta Vigo'),
    ('https://fbref.com/en/squads/a0435291/Granada-Stats', 'Granada'),
    ('https://fbref.com/en/squads/78ecf4bb/Almeria-Stats', 'Almería')
]

# Collect data
dataframes = []

for url, team in teams_info:
    try:
        print(f"Fetching data for {team}...")
        df = collect_team_data(url, team)
        dataframes.append(df)
    except Exception as e:
        print(f"❌ Failed to collect data for {team}: {e}")

# Combine all teams
combined_df = pd.concat(dataframes, ignore_index=True)

# Fix warning with raw string
combined_df['Round'] = combined_df['Round'].str.extract(r'(\d+)')
combined_df['Round'] = pd.to_numeric(combined_df['Round'], errors='coerce')
combined_df.sort_values(by=['Round', 'cum_points'], ascending=[True, False], inplace=True)
combined_df['League Position'] = combined_df.groupby('Round')['cum_points'].rank(method='first', ascending=False)

combined_df.sort_values(by=['Team', 'Date'], inplace=True)
combined_df['Total_GF'] = combined_df.groupby('Team')['GF'].cumsum()
combined_df['Total_GA'] = combined_df.groupby('Team')['GA'].cumsum()
combined_df['pointsLast3'] = combined_df.groupby('Team')['points'].transform(lambda x: x.rolling(window=3, min_periods=1).sum().shift())
combined_df['match_count'] = combined_df.groupby('Team').cumcount() + 1
combined_df['avgGF'] = combined_df['GF'].cumsum() / combined_df['match_count']
combined_df['avgGA'] = combined_df['GA'].cumsum() / combined_df['match_count']
combined_df.drop(['match_count'], axis=1, inplace=True)

combined_df['pointsLastGame'] = combined_df.groupby('Team')['points'].shift()
combined_df['GD'] = combined_df['GF'] - combined_df['GA']
combined_df['GDlastGame'] = combined_df.groupby('Team')['GD'].shift()
combined_df['pointsLastGame'].fillna(0, inplace=True)
combined_df['GDlastGame'].fillna(0, inplace=True)

combined_df.sort_values(by=['Date', 'Round'], inplace=True)

# Merge opponent features
opponent_features = combined_df[['Date', 'Team', 'Total_GF', 'Total_GA', 'PpG', 'cum_points', 'League Position',
                                 'pointsLast3', 'avgGF', 'avgGA', 'pointsLastGame', 'GDlastGame']].copy()
opponent_features.columns = ['Date', 'Opponent', 'Opponent_TotalGF', 'Opponent_TotalGA', 'OpponentPpG',
                             'Opponent_cum_points', 'Opponent_League Position', 'Opponent_pointsLast3',
                             'Opponent_avgGF', 'Opponent_avgGA', 'Opponent_pointsLastGame', 'Opponent_GDlastGame']

combined_df = pd.merge(combined_df, opponent_features, on=['Date', 'Opponent'])

# Save to CSV
os.makedirs('data', exist_ok=True)
combined_df.to_csv('data/Laliga_Fixtures.csv', index=False)
print("✅ CSV saved at data/Laliga_Fixtures.csv")
