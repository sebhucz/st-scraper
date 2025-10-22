#!/usr/bin/env python3
"""
Stooq Profile Scraper v7
Autor: Sebastian Huczek
Cel:
  - Pobiera opis spółki ze strony https://stooq.pl/q/p2/?s=<symbol>
  - Używa XPath do znalezienia tekstu po tabeli z <b>Profil</b>
  - W razie braku wyniku używa metody zapasowej (tekst przed "Źródło:")
"""

import logging
import re
import requests
from lxml import html
from pathlib import Path
from datetime import datetime
import time

# === KONFIGURACJA ===
STOOQ_SYMBOL = "wod"
OUTPUT_DIR = Path("data")
USER_AGENT = "stooq-profile-scraper/1.5 (+https://github.com/yourusername/stooq-scraper)"
THROTTLE_SECONDS = 2

# === LOGOWANIE ===
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

def fetch_company_profile(symbol: str) -> str:
    """
    Pobiera opis spółki za pomocą XPath (główna metoda)
    i fallback regex (tekst przed 'Źródło:').
    """
    url = f"https://stooq.pl/q/p/?s={symbol}"
    headers = {"User-Agent": USER_AGENT}

    logging.info(f"Pobieram stronę {url}")
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()

    # Dekodowanie HTML (Stooq używa ISO-8859-2 / Windows-1250)
    html_text = r.content.decode("iso-8859-2", errors="replace")
    tree = html.fromstring(html_text)

    # --- METODA 1: XPath (dokładna) ---
    xpath_expr = "//table[.//b[text()='Profil']]/following-sibling::text()[normalize-space()]"
    profile_nodes = tree.xpath(xpath_expr)

    if profile_nodes:
        profile_text = " ".join(t.strip() for t in profile_nodes)
        logging.info(f"Znaleziono opis spółki za pomocą XPath ({len(profile_text)} znaków).")
        return profile_text.strip()

    # --- METODA 2: Regex (fallback) ---
    logging.warning("XPath nie zwrócił wyniku — używam fallbacku 'Źródło:'.")
    match = re.search(r"(.+?)Źródło", html_text, flags=re.DOTALL | re.IGNORECASE)
    if match:
        profile_text = re.sub(r"\s+", " ", match.group(1).strip())
        logging.info(f"Znaleziono opis spółki metodą fallback ({len(profile_text)} znaków).")
        return profile_text

    # --- Jeśli brak wyników, zapisz debug HTML ---
    OUTPUT_DIR.mkdir(exist_ok=True)
    debug_path = OUTPUT_DIR / "debug_profile.html"
    debug_path.write_text(html_text, encoding="utf-8")
    raise ValueError(f"Nie znaleziono opisu spółki — zapisano debug HTML: {debug_path}")

def save_profile(symbol: str, text: str):
    """Zapisuje opis spółki do pliku .txt"""
    OUTPUT_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = OUTPUT_DIR / f"{symbol}_profil_{date_str}.txt"
    path.write_text(text, encoding="utf-8")
    logging.info(f"Zapisano opis spółki do pliku: {path}")

def main():
    try:
        text = fetch_company_profile(STOOQ_SYMBOL)
        print(f"\n=== PROFIL SPÓŁKI {STOOQ_SYMBOL.upper()} ===\n{text}\n")
        save_profile(STOOQ_SYMBOL, text)
    except Exception as e:
        logging.exception(f"Błąd: {e}")
    finally:
        logging.info("Zakończono działanie.")
        time.sleep(THROTTLE_SECONDS)

if __name__ == "__main__":
    main()
