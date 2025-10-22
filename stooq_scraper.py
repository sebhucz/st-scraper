#!/usr/bin/env python3
"""
Stooq Scraper v4
Autor: Sebastian Huczek
Opis:
  - Pobiera https://stooq.pl/q/p/?s=<symbol>
  - Radzi sobie z kodowaniem (ISO-8859-2/Windows-1250)
  - Próbuje parsować tabelę via lxml, a potem html5lib
  - Zapisuje CSV + debug.html przy problemach
"""

import time
import logging
import requests
import pandas as pd
from datetime import datetime
from io import StringIO
from pathlib import Path

# === KONFIG ===
STOOQ_SYMBOL = "wod"
OUTPUT_DIR = Path("data")
USER_AGENT = "stooq-scraper/1.1 (+https://github.com/yourusername/stooq-scraper)"
THROTTLE_SECONDS = 2

# === LOGGING ===
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

def _decode_html(resp: requests.Response) -> str:
    """
    Dekoduje bajty na tekst:
    1) Spróbuj nagłówek/requests
    2) Jeśli wynik pusty lub nieczytelny -> spróbuj ISO-8859-2
    3) Ostatecznie Windows-1250
    """
    # 1) Użyj bajtów, nie resp.text
    raw = resp.content or b""
    if not raw:
        return ""

    candidates = []

    # a) deklarowane przez serwer / heurystyka
    if resp.encoding:
        candidates.append(resp.encoding)
    if getattr(resp, "apparent_encoding", None):
        candidates.append(resp.apparent_encoding)

    # b) polskie legacy
    candidates.extend(["iso-8859-2", "windows-1250", "cp1250", "utf-8"])

    tried = set()
    for enc in candidates:
        enc = (enc or "").lower()
        if not enc or enc in tried:
            continue
        tried.add(enc)
        try:
            text = raw.decode(enc, errors="replace")
            if text and "<html" in text.lower():
                logging.info(f"HTML decoded using encoding='{enc}' (len={len(text)})")
                return text
        except Exception:
            continue

    # Ostatnia próba: „replace” w utf-8
    text = raw.decode("utf-8", errors="replace")
    logging.info(f"HTML decoded using fallback utf-8 (len={len(text)})")
    return text

def fetch_stooq_data(symbol: str) -> pd.DataFrame:
    url = f"https://stooq.pl/q/p/?s={symbol}"
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://stooq.pl/",
        "Cache-Control": "no-cache",
    }

    logging.info(f"Pobieram dane z {url}")
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()

    html = _decode_html(r)

    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "debug.html").write_text(html or "", encoding="utf-8")

    if not html or "<table" not in html.lower():
        raise ValueError("Pusta odpowiedź lub brak <table> w HTML — sprawdź data/debug.html")

    # 1. Próba: lxml
    try:
        tables = pd.read_html(StringIO(html), flavor="lxml")
        if tables:
            logging.info(f"Znaleziono {len(tables)} tabel (lxml)")
            return tables[0]
    except Exception as e:
        logging.warning(f"lxml parsowanie nie powiodło się: {e}")

    # 2. Próba: html5lib (bardziej tolerancyjny)
    try:
        tables = pd.read_html(StringIO(html), flavor="html5lib")
        if tables:
            logging.info(f"Znaleziono {len(tables)} tabel (html5lib)")
            return tables[0]
    except Exception as e:
        logging.error("Nie udało się sparsować HTML także html5lib.")
        raise e

    raise ValueError("Nie znaleziono tabel do zparsowania – sprawdź data/debug.html.")

def save_to_csv(df: pd.DataFrame, symbol: str):
    OUTPUT_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    fn = OUTPUT_DIR / f"{symbol}_{date_str}.csv"
    df.to_csv(fn, index=False)
    logging.info(f"Zapisano: {fn}")

def main():
    try:
        df = fetch_stooq_data(STOOQ_SYMBOL)
        logging.info(f"Shape: {df.shape}")
        save_to_csv(df, STOOQ_SYMBOL)
    except requests.HTTPError as e:
        logging.error(f"Błąd HTTP: {e}")
    except Exception as e:
        logging.exception(f"Wystąpił nieoczekiwany błąd: {e}")
    finally:
        logging.info("Zakończono działanie.")
        time.sleep(THROTTLE_SECONDS)

if __name__ == "__main__":
    main()
