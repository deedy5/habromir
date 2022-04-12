import asyncio
import json
import sqlite3
import httpx

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0",}

async def get_url(client, url):
    while True:
        try:
            resp = await client.get(url, headers=HEADERS, timeout=10)
            print(resp.status_code, resp.elapsed.total_seconds(), resp.url)
            if resp.status_code in (200, 403, 404, 500):                
                return resp
            print(f'ERROR {resp.status_code}. Sleep 3 sec.')
            await asyncio.sleep(3)
        except:
            print('ERROR. Sleep 3 sec.')
            await asyncio.sleep(3)

async def parse_habr_json(client, id):
    url = f'https://habr.com/kek/v2/articles/{id}/?fl=ru&hl=ru'
    page = await get_url(client, url)
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

async def main():
    start, end, step = 1, 660600, 10
    async with httpx.AsyncClient() as client:
        while start <= end:
            tasks = [asyncio.create_task(parse_habr_json(client, start+i)) for i in range(step)]
            await asyncio.gather(*tasks)
            save_sqlite([task.result() for task in tasks if task.result()])
            start += step
            sleep(1)

if __name__ == '__main__':    
    asyncio.run(main())
