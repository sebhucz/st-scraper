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


import pandas as pd
from io import StringIO

def fetch_stooq_data(symbol: str) -> pd.DataFrame:
    """
    Pobiera dane z głównej strony notowania (q/p/?s=...) serwisu stooq.pl.
    Zwraca tabelę z cenami i wskaźnikami jako DataFrame.
    """
    url = f"https://stooq.pl/q/p/?s={symbol}"
    headers = {"User-Agent": USER_AGENT}

    logging.info(f"Pobieram dane z {url}")
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()

    # Zapisz HTML do debugowania
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "debug.html").write_text(response.text)

    # Spróbuj odczytać wszystkie tabele z HTML
    tables = pd.read_html(response.text)
    if not tables:
        raise ValueError("Nie znaleziono żadnych tabel w HTML — możliwa zmiana struktury strony.")

    # Na stronie są zwykle dwie tabele: notowania i wskaźniki
    logging.info(f"Znaleziono {len(tables)} tabel, pierwsza ma {len(tables[0])} wierszy.")

    # Możesz wybrać np. pierwszą tabelę (główne dane)
    df = tables[0]

    logging.info(f"Pobrano dane dla {symbol}: {df.shape[0]} wierszy, {df.shape[1]} kolumn.")
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
