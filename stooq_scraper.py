#!/usr/bin/env python3
"""
Stooq Profile Scraper
Autor: Sebastian Huczek
Cel:
  - Pobiera opis spółki ("Profil") ze strony https://stooq.pl/q/p/?s=<symbol>
  - Zwraca czysty tekst z sekcji Profil
"""

import time
import logging
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

# === KONFIGURACJA ===
STOOQ_SYMBOL = "wod"
OUTPUT_DIR = Path("data")
USER_AGENT = "stooq-profile-scraper/1.0 (github.com/yourusername/stooq-scraper)"
THROTTLE_SECONDS = 2

# === LOGOWANIE ===
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

def fetch_profile(symbol: str) -> str:
    """Pobiera i zwraca tekst sekcji 'Profil' ze strony spółki."""
    url = f"https://stooq.pl/q/p/?s={symbol}"
    headers = {"User-Agent": USER_AGENT}

    logging.info(f"Pobieram stronę {url}")
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()

    # Dekoduj w odpowiednim kodowaniu (ISO-8859-2 -> Windows-1250)
    html = response.content.decode("iso-8859-2", errors="replace")
    soup = BeautifulSoup(html, "lxml")

    # Znajdź komórkę z napisem "Profil"
    profile_label = soup.find("td", string=lambda s: s and "Profil" in s)
    if not profile_label:
        raise ValueError("Nie znaleziono sekcji 'Profil' na stronie.")

    # Następny element (opis)
    profile_text_td = profile_label.find_next("td")
    if not profile_text_td:
        raise ValueError("Nie znaleziono tekstu opisu pod etykietą 'Profil'.")

    # Usuń nadmiarowe spacje i formatowanie
    text = " ".join(profile_text_td.get_text(strip=True).split())
    logging.info(f"Znaleziono opis profilu ({len(text)} znaków).")
    return text

def save_profile(symbol: str, text: str):
    """Zapisuje tekst profilu do pliku."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = OUTPUT_DIR / f"{symbol}_profil_{date_str}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(text)
    logging.info(f"Zapisano profil do: {filename}")

def main():
    try:
        profile_text = fetch_profile(STOOQ_SYMBOL)
        print(f"\n=== PROFIL SPÓŁKI {STOOQ_SYMBOL.upper()} ===\n{profile_text}\n")
        save_profile(STOOQ_SYMBOL, profile_text)
    except Exception as e:
        logging.exception(f"Błąd: {e}")
    finally:
        time.sleep(THROTTLE_SECONDS)

if __name__ == "__main__":
    main()
