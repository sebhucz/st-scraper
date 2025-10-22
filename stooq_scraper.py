#!/usr/bin/env python3
"""
Stooq Profile Scraper v6
Autor: Sebastian Huczek
Cel:
  - Pobiera opis spółki (tekst przed słowem "Źródło:") ze strony https://stooq.pl/q/p2/?s=<symbol>
  - Działa w GitHub Actions bez Selenium
"""

import logging
import re
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import time

# === KONFIGURACJA ===
STOOQ_SYMBOL = "wod"
OUTPUT_DIR = Path("data")
USER_AGENT = "stooq-profile-scraper/1.3 (+https://github.com/yourusername/stooq-scraper)"
THROTTLE_SECONDS = 2

# === LOGOWANIE ===
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

def fetch_company_profile(symbol: str) -> str:
    """
    Pobiera opis spółki ze strony p2/?s=<symbol>,
    biorąc tekst poprzedzający słowo 'Źródło:'.
    """
    url = f"https://stooq.pl/q/p/?s={symbol}"
    headers = {"User-Agent": USER_AGENT}

    logging.info(f"Pobieram stronę {url}")
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()

    html = r.content.decode("iso-8859-2", errors="replace")

    # Parsujemy HTML
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    # Znajdź fragment przed słowem "Źródło:"
    match = re.search(r'(.+?)Źródło', text, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        # zapisz do debugowania
        OUTPUT_DIR.mkdir(exist_ok=True)
        (OUTPUT_DIR / "debug_profile.html").write_text(html, encoding="utf-8")
        raise ValueError("Nie udało się znaleźć fragmentu przed 'Źródło:' — sprawdź debug_profile.html")

    profile_text = match.group(1).strip()

    # Opcjonalne czyszczenie z resztek whitespace
    profile_text = re.sub(r'\s+', ' ', profile_text)
    logging.info(f"Znaleziono profil spółki ({len(profile_text)} znaków).")
    return profile_text

def save_profile(symbol: str, text: str):
    """Zapisuje opis spółki do pliku .txt"""
    OUTPUT_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"{symbol}_profil_{date_str}.txt"
    path.write_text(text, encoding="utf-8")
    logging.info(f"Zapisano opis spółki do pliku: {path}")

def main():
    try:
        profile = fetch_company_profile(STOOQ_SYMBOL)
        print(f"\n=== PROFIL SPÓŁKI {STOOQ_SYMBOL.upper()} ===\n{profile}\n")
        save_profile(STOOQ_SYMBOL, profile)
    except Exception as e:
        logging.exception(f"Błąd: {e}")
    finally:
        logging.info("Zakończono działanie.")
        time.sleep(THROTTLE_SECONDS)

if __name__ == "__main__":
    main()
