import aiohttp
import asyncio
import csv
import random
import time
from asyncio import Semaphore

# ==============================
# PARAMETRY
# ==============================
PER_PAGE = 50
MEMORY_BATCH = 100
BASE_URL = "https://api.jbzd.com.pl/ranking/get"
CONCURRENT_REQUESTS = 10  # liczba równoczesnych requestów
CSV_FILE = "jbzd_users_colors.csv"
LOG_FILE = "jbzd_scraper_log.txt"

# Przykładowe proxy (możesz zmienić/uzupełnić)
PROXIES = [
    None,  # brak proxy – użycie własnego IP
    # "http://user:pass@proxy1:port",
    # "http://user:pass@proxy2:port",
]

# Rotacja User-Agent
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.96 Safari/537.36",
]

# ==============================
# FUNKCJE
# ==============================
def log(message):
    print(message)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")

def format_time(seconds):
    h, m = divmod(int(seconds)//60, 60)
    s = int(seconds) % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def save_csv(data):
    with open(CSV_FILE, 'a', newline='', encoding='utf-8') as f:
        csv.writer(f).writerows(data)

async def fetch_page(session, page):
    retry = 0
    url = f"{BASE_URL}?page={page}&per_page={PER_PAGE}"
    while True:
        proxy = random.choice(PROXIES)
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        try:
            async with session.get(url, proxy=proxy, headers=headers, timeout=10) as resp:
                resp.raise_for_status()
                json_data = await resp.json()
                return page, [[u['id'], u['model']['color']] for u in json_data['rankings']['data']]
        except Exception as e:
            retry += 1
            wait = 2 ** min(retry, 5) + random.uniform(0.5,1.5)
            log(f"Błąd przy stronie {page} z proxy {proxy}: {e}. Retry za {wait:.1f}s (próba {retry})")
            await asyncio.sleep(wait)

async def run_scraper():
    start = time.time()

    async with aiohttp.ClientSession() as session:
        # Pobranie liczby wszystkich stron
        async with session.get(f"{BASE_URL}?page=1&per_page={PER_PAGE}") as r:
            last_page = (await r.json())['rankings']['last_page']
    total_pages = last_page
    log(f"Łącznie stron do pobrania: {total_pages}")

    pages = list(range(1, total_pages+1))
    buffer, done = [], 0
    sem = Semaphore(CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        async def worker(page):
            async with sem:
                return await fetch_page(session, page)

        while pages:
            batch, pages = pages[:CONCURRENT_REQUESTS], pages[CONCURRENT_REQUESTS:]
            results = await asyncio.gather(*(worker(p) for p in batch))
            for _, data in results:
                done += 1
                buffer.extend(data)

            if len(buffer) >= MEMORY_BATCH * PER_PAGE:
                save_csv(buffer)
                log(f"Zapisano dane z {MEMORY_BATCH} stron do CSV")
                buffer.clear()

            elapsed = time.time() - start
            percent = done / total_pages
            eta = format_time(elapsed * (1 - percent) / percent) if percent > 0 else "N/A"
            log(f"Stron pobrano: {done}/{total_pages} ({percent*100:.2f}%) | ETA: {eta}")

    if buffer:
        save_csv(buffer)
        log(f"Zapisano dane z ostatnich {len(buffer)//PER_PAGE} stron do CSV")

    log(f"✅ Całkowity czas: {format_time(time.time()-start)}\n")

# ==============================
# URUCHOMIENIE
# ==============================
if __name__ == "__main__":
    # Tworzymy plik CSV z nagłówkiem
    with open(CSV_FILE,'w',newline='',encoding='utf-8') as f:
        csv.writer(f).writerow(['id','color'])
    with open(LOG_FILE,'w',encoding='utf-8') as f:
        f.write("=== Start pobierania JBZD ===\n")

    asyncio.run(run_scraper())
    log("✅ Wszystkie strony pobrane")
