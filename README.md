# habromir
Зеркало Хабра (habr.com)

База sqlite3 без комментариев по состоянию на июль 2020
[Link: mega.nz](https://mega.nz/file/nwM2HCSQ#YsupTufeNoMTH_TgirQndDbnLCFLIeQzHGiNvXsDUnQ)


Парсер асинхронный и работает через Tor, в файл torrc необходимо добавить порты и хешированный пароль
для stem (tor.exe --hash-password PASSWORD):
```
ControlPort 9051
HashedControlPassword Your_hashed_password
SocksPort 9000 IsolateDestAddr
SocksPort 9001 IsolateDestAddr
SocksPort 9002 IsolateDestAddr
```
stem: https://stem.torproject.org,

tor: https://www.torproject.org/docs/tor-manual.html.en
