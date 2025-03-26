import requests
import pandas as pd
import warnings
from bs4 import BeautifulSoup

# Hide FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# URL for Leeds 
team_url = "https://fbref.com/en/squads/206d90db/Barcelona-Stats"

# Sendeing HTTP GET-request to webpage
data = requests.get(team_url)

# check for success response (statuscode 200)
if data.status_code == 200:
    print("Successfully retrieved page")
else:
    print(f"Failed to retrieve page. Status code: {data.status_code}")

# Create a BeautifulSoup-objekt for navigating in HTML
soup = BeautifulSoup(data.text, "html.parser")

# finding all tables in page
tables = soup.find_all("table") # finding table-elements
# print all ids for each table
for table in tables:
    print(table.get("id")) #print all ids 
