import json
import sqlite3
from time import sleep
import requests

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0",})

DELAY = 0.5

def get_url(url):
    while True:
        sleep(DELAY)
        try:
            resp = S.get(url, timeout=10)
            print(resp.status_code, resp.elapsed, resp.url)
            if resp.status_code in (200, 403, 404):          
                return resp
            print(f'ERROR {resp.status_code}')
            sleep(3)
        except:
            print('ERROR')
            sleep(3)

def parse_habr_json(id):
    url = f'https://habr.com/kek/v2/articles/{id}/?fl=ru&hl=ru'
    page = get_url(url)
    if page.status_code in (403, 404):             
        return None
    else:
        data = json.loads(page.content)
        timestamp = data['timePublished']
        title = data['titleHtml']
        content = data['textHtml']    
        author = data['author']['alias']
        lang = data['lang']
        is_tutorial = 1 if len(data['postLabels']) > 0 else 0
        rating = data['statistics']['score']
        views = data['statistics']['readingCount']    
        comments = data['statistics']['commentsCount']
        tags = ' ,'.join(tag['titleHtml'] for tag in data['tags'])
    return ((id, timestamp, title, content, author, lang, is_tutorial, rating, views, comments, tags))

def save_sqlite(results):
    conn = sqlite3.connect('habromir.sqlite3', check_same_thread = False)
    with conn:
        conn.executescript('PRAGMA journal_mode = wal; PRAGMA synchronous = 1;')
        conn.execute('''CREATE TABLE IF NOT EXISTS habr(id INTEGER PRIMARY KEY, timestamp TEXT, title TEXT,
                        content TEXT, author TEXT, lang TEXT, is_tutorial INTEGER, rating INTEGER, views INTEGER,
                        comments INTEGER, tags TEXT)''')
        conn.executemany('''insert into habr values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', results)
        conn.executescript('PRAGMA optimize;')
    print(f'SQLite3 commit {len(results)} pages')

if __name__ == '__main__': 
    start, end = 1, 646000
    results = []
    for  i in range(start, end+1):
        r = parse_habr_json(i)
        if r:
            results.append(r)
        if i % 10 == 0:
            save_sqlite(results)
            results.clear()
