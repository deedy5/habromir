import json
import asyncio
import sqlite3
import time
from random import uniform, choice
from collections import deque
import aiohttp
from aiohttp_socks import ProxyConnector
from stem import Signal
from stem.control import Controller

async def tor_new_ip():
    with Controller.from_port(port = 9051) as controller:
        controller.authenticate(password = "Unhashed_Password")
        controller.signal(Signal.NEWNYM)
##        print(controller.is_authenticated())
##        ip_api_list = ['https://api.my-ip.io/ip', 'https://api.ipify.org', 'https://ifconfig.me/ip', 
##                       'https://httpbin.org/ip', 'https://icanhazip.com/', 'https://ipinfo.io/ip',
##                       'https://wtfismyip.com/text', 'https://checkip.amazonaws.com/', 
##                       'https://bot.whatismyipaddress.com', 'https://whoer.net/ip',]
##        ip = await fetch(choice(ip_api_list))
##        print(f'Tor NEWNYM === ip: {ip}')

async def fetch(url):
    sleeptime = uniform(0, 2)
    await asyncio.sleep(sleeptime)
    for _ in range(5):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:68.0) Gecko/20100101 Firefox/68.0',
                       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                       'Accept-Language': 'en-US,en;q=0.5',
                       'Accept-Encoding': 'gzip, deflate, br',
                       'Connection': 'keep-alive',
                       'Upgrade-Insecure-Requests': '1',}
            rproxy = proxies[0]
            proxies.rotate()            
            connector = ProxyConnector.from_url(rproxy, rdns = True)
            async with aiohttp.request('get', url, connector=connector, headers=headers) as response:   
                r = await response.text()                
                print(f'{response.status} {url} {rproxy} {sleeptime:.2f}')
                if response.status == 200:
                    pass                 
                else:
                    r = response.status
                await connector.close()                
                return r
        except:
            print('Error, wait... and repeat')
            await tor_new_ip()
            await asyncio.sleep(3)
            continue    
    return 'Error'

async def parse_habr_json(id):
    url = f'https://m.habr.com/kek/v2/articles/{id}/?fl=ru&hl=ru'
    page = await fetch(url)
    if page == 403:        
        return ((id, 403, 403, 403, 403, 403, 403, 403, 403, 403, 403))
    if page == 404:        
        return ((id, 404, 404, 404, 404, 404, 404, 404, 404, 404, 404))
    else:
        data = json.loads(page)    
        timestamp = data['timePublished']
        title = data['titleHtml']
        content = data['textHtml']    
        author = data['author']['login']
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
    conn.close()
    print(f'SQLite3 commit {len(results)} pages. Time: {time.ctime()}')

async def main():    
    while start < end:    
        tasks = [asyncio.create_task(parse_habr_json(start + i)) for  i in range(step)]     
        results = await asyncio.gather(*tasks)
        save_sqlite(results)
        await tor_new_ip()
        start += step
        time.sleep(2)

if __name__ == '__main__':
    start, end, step = 512000, 513000, 10
    proxies = deque(['socks5://127.0.0.1:9000', 'socks5://127.0.0.1:9001', 'socks5://127.0.0.1:9002',])    
    asyncio.run(main())
