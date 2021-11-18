import twitter
import mysql.connector
import json
import os

def collect_data_about(name, twitter_api, cursor):  
  MAX_ENTRY_PER_PERSON = 1000

  print(f'Quering twitter API for: {name}')
  search_results = twitter_api.search.tweets(q=name, count=100, lang='en')
  processStatusCounter = 0
  
  while True:
    try:
      statuses = search_results['statuses']
    except KeyError:
      return
    
    for status in statuses:
      tweet = status['text']      
      username = status['user']['screen_name']
      created_at = status['created_at']
      retweet_count = status['retweet_count']
      country = status['user']['location']
      tweet_id = status['id_str']

      query = '''
INSERT INTO tweets (character_id, `name`, tweet, tweet_id, username, created_at, retweet_count, country)
  VALUES (
    (SELECT id from characters WHERE %s = hero OR %s = alterego OR %s = actor),
    %s, %s, %s, %s, %s, %s, %s
  )
'''
      cursor.execute(query, (name, name, name, name, tweet, tweet_id, username, created_at, retweet_count, country))

      processStatusCounter += 1
      if processStatusCounter >= MAX_ENTRY_PER_PERSON:
        print(f'Received tweets for {name}: {processStatusCounter}')
        return
    
    try:
      next_results = search_results['search_metadata']['next_results']
    except KeyError:
      print(f'Received tweets for {name}: {processStatusCounter}')
      return

    kwargs = dict([ kv.split('=') for kv in next_results[1:].split("&") ])
    search_results = twitter_api.search.tweets(**kwargs)

def init_db(cursor):
  characters = [
    {
      'hero': 'Iron Man',
      'alterego': 'Tony Stark',
      'actor': 'Robert Downey Jr.'
    },
    {
      'hero': 'Hulk',
      'alterego': 'Bruce Banner',
      'actor': 'Mark Ruffalo'
    },
    {
      'hero': 'Spider-Man',
      'alterego': 'Peter Parker',
      'actor': 'Tom Holland'
    },
    {
      'hero': 'Thor',
      'actor': 'Tom Hemsworth'
    },
    {
      'hero': 'Loki',
      'actor': 'Tom Hiddleston'
    },
    {
      'hero': 'Captain America',
      'alterego': 'Steve Rogers',
      'actor': 'Chris Evans'
    },
    {
      'hero': 'Deadpool',
      'alterego': 'Wade Wilson',
      'actor': 'Ryan Reynolds'
    },
    {
      'hero': 'Winter Soldier',
      'alterego': 'Bucky Barnes',
      'actor': 'Sebastian Stan'
    },
    {
      'hero': 'Doctor Strange',
      'alterego': 'Doctor Strange',
      'actor': 'Benedict Cumberbatch'
    },
    {
      'hero': 'Black Panther',
      'alterego': "T'Challa",
      'actor': 'Chadwick Boseman'
    },
    {
      'hero': 'Hawkeye',
      'alterego': 'Clint Barton',
      'actor': 'Jeremy Renner'
    },
    {
      'hero': 'Captain Marvel',
      'alterego': 'Carol Danvers',
      'actor': 'Brie Larson'
    },
    {
      'hero': 'Vision',
      'alterego': 'Jarvis',
      'actor': 'Paul Bettany'
    },
    {
      'hero': 'Ant-Man',
      'alterego': 'Scott Lang',
      'actor': 'Paul Rudd'
    },
    {
      'hero': 'Thanos',
      'actor': 'Josh Brolin'
    },
    {
      'hero': 'Star Lord',
      'alterego': 'Peter Quill',
      'actor': 'Chris Pratt'
    },
    {
      'hero': 'Groot',
      'actor': 'Vin Diesel'
    },
    {
      'hero': 'Rocket Raccoon',
      'actor': 'Bradley Cooper'
    },
    {
      'hero': 'Gamora',
      'actor': 'Zoe Saldana'
    },
    {
      'hero': 'Nebula',
      'actor': 'Karen Gillan'
    }
]

  cursor.execute('DROP TABLE IF EXISTS tweets')
  cursor.execute('DROP TABLE IF EXISTS characters')

  cursor.execute('''
CREATE TABLE characters (
  id INT AUTO_INCREMENT PRIMARY KEY,
  hero VARCHAR(255),
  alterego VARCHAR(255),
  actor VARCHAR(255)
)
  ''')

  for character in characters:
    query = 'INSERT INTO characters (hero, alterego, actor) VALUES (%s, %s, %s)'
    cursor.execute(query, (character.get('hero', ''), character.get('alterego', ''), character.get('actor', '')))

  cursor.execute('''
CREATE TABLE tweets (
  id INT AUTO_INCREMENT PRIMARY KEY,
  character_id INT NOT NULL,
  name VARCHAR(255),
  tweet VARCHAR(255),
  tweet_id VARCHAR(255),
  username VARCHAR(255),
  created_at VARCHAR(255),
  retweet_count INT,
  country VARCHAR(255) CHARACTER SET utf8mb4,
  FOREIGN KEY (character_id) REFERENCES characters(id)
)
  ''')

def fill_db(twitter_api, cursor):
  cursor.execute('SELECT hero, alterego, actor FROM characters')
  rows = cursor.fetchall()
  for row in rows:
    for col in row:
      if col != '':
        collect_data_about(col, twitter_api, cursor)

def get_stats(cursor):
  stats = {}
  cursor.execute('''
SELECT characters.hero AS hero, COUNT(DISTINCT tweets.tweet_id)
  AS tweet_count FROM characters
  INNER JOIN tweets ON characters.id = tweets.character_id
  GROUP BY hero
''')
  rows = cursor.fetchall()
  for row in rows:
    stats[row[0]] = row[1]
  return stats

if __name__ == "__main__":
  marvelDB = mysql.connector.connect(
    host = "localhost",
    user = "marietamarvel",
    passwd = "4516",
    database = "marvelDB"
  )
  cursor = marvelDB.cursor()

  CONSUMER_KEY = 'aaaaaaaaaa'
  CONSUMER_SECRET = 'bbbbbbbbbbbbb'
  OAUTH_TOKEN = 'ccccccccccccccc'
  OAUTH_TOKEN_SECRET = 'dddddddddddddd'
  twitter_auth = twitter.oauth.OAuth(
    OAUTH_TOKEN,
    OAUTH_TOKEN_SECRET,
    CONSUMER_KEY,
    CONSUMER_SECRET
  )
  twitter_api = twitter.Twitter(auth = twitter_auth)

  init_db(cursor)
  marvelDB.commit()

  fill_db(twitter_api, cursor)
  marvelDB.commit()

  stats = get_stats(cursor)

  print(f'Total unique tweets for each hero (and their alter ego or actor) in last week:')
  for name in stats:
    print(f'{name}: {stats[name]}')
