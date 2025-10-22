#!/usr/bin/env python3
"""
Stooq Scraper v3
Autor: Sebastian Huczek
Repo: github.com/yourusername/stooq-scraper
Opis:
  - Pobiera dane ze strony notowań (np. https://stooq.pl/q/p/?s=wod)
  - Parsuje tabele HTML do DataFrame
  - Zapisuje dane do pliku CSV
  - Obsługuje błędy i zapisuje debug.html przy problemach
"""

import time
import logging
import requests
import pandas as pd
from datetime import datetime
from io import StringIO
from pathlib import Path

# === KONFIGURACJA ===
STOOQ_SYMBOL = "wod"  # np. WIG20: wig20, Allegro: ale, Orlen: pcl
OUTPUT_DIR = Path("data")
USER_AGENT = "stooq-scraper/1.0 (github.com/yourusername/stooq-scraper)"
THROTTLE_SECONDS = 3  # opóźnienie, żeby nie przeciążać serwisu


# === LOGOWANIE ===
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)


def fetch_stooq_data(symbol: str) -> pd.DataFrame:
    """
    Pobiera dane z głównej strony notowania (q/p/?s=...) serwisu stooq.pl.
    Zwraca tabelę z danymi jako DataFrame.
    """
    url = f"https://stooq.pl/q/p/?s={symbol}"
    headers = {"User-Agent": USER_AGENT}

    logging.info(f"Pobieram dane z {url}")
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()

    # Zapisz HTML do debugowania
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "debug.html").write_text(response.text, encoding="utf-8")

    # Parsowanie HTML -> DataFrame
    try:
        tables = pd.read_html(StringIO(response.text))
    except Exception as e:
        logging.error("Nie udało się sparsować HTML (prawdopodobnie zmiana struktury strony).")
        raise e

    if not tables:
        raise ValueError("Nie znaleziono żadnych tabel w HTML — sprawdź debug.html.")

    logging.info(f"Znaleziono {len(tables)} tabel w HTML.")
    df = tables[0]  # zwykle pierwsza tabela to notowania

    logging.info(f"Pobrano dane dla {symbol}: {df.shape[0]} wierszy, {df.shape[1]} kolumn.")
    return df


def save_to_csv(df: pd.DataFrame, symbol: str):
    """Zapisuje DataFrame do pliku CSV w katalogu data/."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = OUTPUT_DIR / f"{symbol}_{date_str}.csv"
    df.to_csv(filename, index=False)
    logging.info(f"Zapisano dane do: {filename}")


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
