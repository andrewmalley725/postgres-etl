from bs4 import BeautifulSoup
import psycopg2
import requests
import pandas as pd
import creds

url = 'https://www.espn.com/nba/stats'

res = requests.get(url)

html = res.text

soup = BeautifulSoup(html, 'html.parser')

offLeads = soup.find('div', {'class' : 'mb1'})

categories = offLeads.select('th', {'class' : 'TABLE_TH', 'title': ''})

categories = [i.text for i in categories if categories.index(i) % 2 == 0]

players = offLeads.select('a', {'class': 'AnchorLink flex items-center'})

points = offLeads.select('td', {'class': 'Table_TD'})

players = [i['href'] for i in players if 'https' in i['href']]

points = [i.text for i in points if '.' in i.text]

player_categories = []

for i in categories:
    obj = {
        'category': i,
        'players': []
    }
    for count in range(5):
        player_url = players[0]
        index = player_url.rfind('/') + 1
        name = player_url[index:].split('-', 1)
        player = {
            'first_name': name[0],
            'last_name': name[1],
            i: points[0]
        }
        obj['players'].append(player)
        players.pop(0)
        points.pop(0)
    player_categories.append(obj)

players_df = pd.DataFrame(columns=['first_name', 'last_name'])

categories_df = pd.DataFrame(columns=['category'])

for cat in player_categories:
    categories_df = categories_df.append({'category': cat['category']}, ignore_index=True)
    for player in cat['players']:
        new_record = {'first_name':player['first_name'], 'last_name': player['last_name']}
        players_df = players_df.append(new_record, ignore_index=True)

columns = ['player_id','category_id','stat']

stats_df = pd.DataFrame(columns=columns)

for i in player_categories:
    cat = i['category']
    cat_id = categories_df.index.get_loc(categories_df[categories_df.category == cat].index[0])
    for p in i['players']:
        new_record = {}
        new_record['category_id'] = cat_id
        play_id = players_df.index.get_loc(players_df[(players_df.first_name == p['first_name']) & (players_df.last_name == p['last_name'])].index[0])
        new_record['player_id'] = play_id
        new_record['stat'] = p['stat']
        stats_df = stats_df.append(new_record, ignore_index=True)

conn = psycopg2.connect(
    host=creds.dbhost,
    database=creds.db,
    user=creds.dbUser,
    password=creds.dbPassword
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS players (
        id SERIAL PRIMARY KEY,
        firstname TEXT,
        lastname TEXT
    );
""")

for i, row in players_df.iterrows():
    cur.execute("""
        INSERT INTO players (firstname, lastname)
        VALUES (%s, %s);
    """, (row['first_name'], row['last_name']))

conn.commit()
