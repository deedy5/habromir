import json
import sqlite3
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

S = requests.Session()
S.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0",})

def get_url(url):
    while True:
        try:
            resp = S.get(url, timeout=10)
            elapsed = resp.elapsed.total_seconds()
            print(resp.status_code, elapsed, resp.url)
            sleep(elapsed)
            if resp.status_code in (200, 403, 404, 500):                
                return resp
            print(f'ERROR {resp.status_code}. Sleep 3 sec.')
            sleep(3)
        except:
            print('ERROR. Sleep 3 sec.')
            sleep(3)

def parse_habr_json(_id):
    url = f'https://habr.com/kek/v2/articles/{_id}/?fl=ru&hl=ru'
    page = get_url(url)
    if page.status_code in (403, 404, 500):             
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
    return ((_id, timestamp, title, content, author, lang, is_tutorial, rating, views, comments, tags))

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
    start, end, step = 1, 660600, 100
    with ThreadPoolExecutor(4) as executor:
        while start < end:
            futures = [executor.submit(parse_habr_json, start+i) for i in range(step)]
            save_sqlite([r.result() for r in as_completed(futures) if r.result()])
            print(f'saved {start}-{start+step-1}')
            start += step

 
    
