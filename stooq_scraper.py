#!/usr/bin/env python3
"""
Stooq Scraper - pobiera dane notowań z serwisu stooq.pl i zapisuje do CSV.
Autor: Sebastian Huczek
Licencja: MIT
"""

import time
import logging
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path


# === KONFIGURACJA ===
STOOQ_SYMBOL = "wod"  # np. WIG20: wig20, Orlen: pcl, Allegro: ale, itp.
OUTPUT_DIR = Path("data")
USER_AGENT = "stooq-scraper/1.0 (github.com/yourusername/stooq-scraper)"
THROTTLE_SECONDS = 3  # opóźnienie między zapytaniami, by nie przeciążać serwisu


# === LOGOWANIE ===
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)


def fetch_stooq_data(symbol: str) -> pd.DataFrame:
    """Pobiera dane z serwisu stooq.pl i zwraca je jako DataFrame."""
    url = f"https://stooq.pl/q/d/?s={symbol}&c=0"
    headers = {"User-Agent": USER_AGENT}

    logging.info(f"Pobieram dane z {url}")
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()

    # Spróbuj wczytać jako CSV
    try:
        df = pd.read_csv(pd.compat.StringIO(response.text))
    except Exception:
        # Niektóre wersje Pandas nie mają już pd.compat
        from io import StringIO
        df = pd.read_csv(StringIO(response.text))

    logging.info(f"Pobrano {len(df)} wierszy dla {symbol}")
    return df


def save_to_csv(df: pd.DataFrame, symbol: str):
    """Zapisuje dane do pliku CSV w folderze data/."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"{symbol}_{date_str}.csv"
    df.to_csv(path, index=False)
    logging.info(f"Zapisano dane do: {path}")


def main():
    try:
        df = fetch_stooq_data(STOOQ_SYMBOL)
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
