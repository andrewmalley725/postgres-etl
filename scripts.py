from bs4 import BeautifulSoup
import psycopg2
import requests
import pandas as pd
import creds

conn = psycopg2.connect(
    host=creds.dbhost,
    database=creds.db,
    user=creds.dbUser,
    password=creds.dbPassword
)

cur = conn.cursor()

cur.execute('drop table player_stats')
cur.execute('drop table players')
cur.execute('drop table positions')

cur.execute('''create table if not exists positions (
    id serial primary key,
    position_name text
);''')

cur.execute('''create table if not exists players (
    id serial primary key,
    firstname text,
    lastname text,
    position_id integer references positions(id)
);''')
            
cur.execute('''CREATE TABLE IF NOT EXISTS player_stats (
    id serial PRIMARY KEY,
    player_id bigint REFERENCES players(id),
    GP double precision NULL,
    MIN double precision NULL,
    PTS double precision NULL,
    FGM double precision NULL,
    FGA double precision NULL,
    FG_percentage double precision NULL,
    TPM double precision NULL,
    TPA double precision NULL,
    TP_percentage double precision NULL,
    FTM double precision NULL,
    FTA double precision NULL,
    FT_percentage double precision NULL,
    REB double precision NULL,
    AST double precision NULL,
    STL double precision NULL,
    BLK double precision NULL,
    T_O double precision NULL,
    DD double precision NULL,
    TD double precision NULL
);''')


            
url = 'https://www.espn.com/nba/stats'

res = requests.get(url)

html = res.text

soup = BeautifulSoup(html, 'html.parser')

offLeads = soup.find('div', {'class' : 'mb1'})

categories = offLeads.find_all('tr', {'class' : 'Table__TR Table__even'})

categories = [i.find('th').text for i in categories]

complete_link = offLeads.find('a', {'class': 'AnchorLink leadersTable__complete-list'})['href']

stats_url = 'https://www.espn.com' + complete_link

req = requests.get(stats_url)

stats_html = req.text

stats_soup = BeautifulSoup(stats_html, 'html.parser')

player_stats = stats_soup.select('tr',{'class':'Table__TR Table__TR--sm Table__even'})

stats = [i for i in player_stats if i.find('td',{'class':'position Table__TD'})]

player_names = []

stat_order = ['GP', 'MIN', 'PTS', 'FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%', 'FTM', 'FTA', 'FT%', 'REB', 'AST', 'STL', 'BLK', 'TO', 'DD2', 'TD3']

for i in player_stats:
    if i.find('a',{'class':'AnchorLink'}) and i.get('data-idx'):
        obj = {}
        first_name = i.find('a',{'class':'AnchorLink'}).text.split(' ')[0]
        last_name = i.find('a',{'class':'AnchorLink'}).text.split(' ')[1]
        url = i.find('a',{'class':'AnchorLink'})['href']
        obj['firstname'] = first_name
        obj['lastname'] = last_name
        obj['playerinfo'] = url
        obj['index'] = i.get('data-idx')
        player_names.append(obj)

positions = []

for i in stats:
    index = i['data-idx']
    for p in player_names:
        if p.get('index') == index:
            p['positionid'] = i.find('div', {'class': 'position'}).text
            if i.find('div', {'class': 'position'}).text not in positions:
                positions.append(i.find('div', {'class': 'position'}).text)
            p_stats = i.find_all('td',{'class','Table__TD'})[1:]
            for stat in p_stats:
                number = stat.text
                p[stat_order[p_stats.index(stat)]] = float(number)

positions_table = []

for i in range(1, len(positions) + 1):
    obj = {
        'pos_id': i,
        'position':positions[i - 1]
    }
    positions_table.append(obj)

for player in player_names:
    for pos in positions_table:
        if player['positionid'] == pos['position']:
            player['positionid'] = pos['pos_id']

df = pd.DataFrame.from_records(player_names)

df_pos = pd.DataFrame.from_records(positions_table, index='pos_id')

df_player = df[['firstname', 'lastname', 'positionid']]

df_player.index += 1

df_stats = df[['GP', 'MIN', 'PTS', 'FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%', 'FTM', 'FTA', 'FT%', 'REB', 'AST', 'STL', 'BLK', 'TO', 'DD2', 'TD3']]
# df_stats['player_id'] = df_player.index
# df_stats = df_stats[['player_id','GP', 'MIN', 'PTS', 'FGM', 'FGA', 'FG%', '3PM', '3PA', '3P%', 'FTM', 'FTA', 'FT%', 'REB', 'AST', 'STL', 'BLK', 'TO', 'DD2', 'TD3']]
df_stats.insert(0, 'player_id', df_player.index)

for _, row in df_pos.iterrows():
    cur.execute(
        '''insert into positions (position_name)
            values (%s);
        ''', (row['position'],)
    )

for _, row in df_player.iterrows():
    cur.execute(
        '''insert into players (firstname, lastname, position_id)
            values (%s, %s, %s);
        ''', (row['firstname'], row['lastname'], row['positionid'])
    )

for i in range(len(df_stats)):
    cur.execute('''INSERT INTO player_stats (player_id, GP, MIN, PTS, FGM, FGA, FG_percentage, TPM, TPA, TP_percentage, FTM, FTA, FT_percentage, REB, AST, STL, BLK, T_O, DD, TD)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                   (df_stats.iloc[i]['player_id'], df_stats.iloc[i]['GP'], df_stats.iloc[i]['MIN'], df_stats.iloc[i]['PTS'], df_stats.iloc[i]['FGM'], df_stats.iloc[i]['FGA'], df_stats.iloc[i]['FG%'], df_stats.iloc[i]['3PM'], df_stats.iloc[i]['3PA'], df_stats.iloc[i]['3P%'], df_stats.iloc[i]['FTM'], df_stats.iloc[i]['FTA'], df_stats.iloc[i]['FT%'], df_stats.iloc[i]['REB'], df_stats.iloc[i]['AST'], df_stats.iloc[i]['STL'], df_stats.iloc[i]['BLK'], df_stats.iloc[i]['TO'], df_stats.iloc[i]['DD2'], df_stats.iloc[i]['TD3']))

    
conn.commit()
conn.close()