#!/usr/bin/env python3
"""Same-origin dashboard API for market quotes and linked news headlines."""
from __future__ import annotations

import copy
import json
import math
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parent
PORT = int(os.environ.get("PORT", "4173"))
IST = ZoneInfo("Asia/Kolkata")
CACHE_TTL = 10
HTTP_TIMEOUT = (4, 9)

NSE_API = "https://www.nseindia.com/api"
BSE_API = "https://api.bseindia.com/BseIndiaAPI/api"
BSE_REALTIME_API = "https://api.bseindia.com/RealTimeBseIndiaAPI/api"
IPO_URL = "https://www.investorgain.com/report/ipo-gmp-live/331/"
IPO_CACHE_TTL = 120
ETF_URL = "https://www.nseindia.com/market-data/exchange-traded-funds-etf"
ETF_DATA_URL = "https://www.nseindia.com/api/NextApi/apiClient/marketWatchApi?functionName=getETFData&parent=all&child="
ETF_CACHE_TTL = 30
GOODRETURNS_GOLD_URL = "https://www.goodreturns.in/gold-rates/"
GROWW_SILVER_URL = "https://groww.in/commodities/futures/mcx_silver"
COINGECKO_BTC_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=inr&ids=bitcoin&price_change_percentage=24h&precision=full"
BENCHMARK_CACHE_TTL = 300
SILVER_CACHE_TTL = 5
NEWS_CACHE_TTL = 120
MONEYCONTROL_SECTIONS = {
    "news": {"title": "News", "url": "https://www.moneycontrol.com/news/news-all/", "sourceName": "Moneycontrol"},
    "business": {"title": "Business", "url": "https://www.moneycontrol.com/news/business/", "sourceName": "Moneycontrol"},
    "market": {"title": "Market", "url": "https://www.moneycontrol.com/news/business/markets/", "sourceName": "Moneycontrol"},
    "hindi": {"title": "News (Hindi)", "url": "https://hindi.moneycontrol.com/news/latest-news", "sourceName": "Moneycontrol Hindi", "parser": "hindi_latest"},
}

IPO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.investorgain.com/",
    "Cache-Control": "no-cache",
}

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/live-equity-market",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
ETF_HEADERS = {
    **NSE_HEADERS,
    "Referer": ETF_URL,
}
GOODRETURNS_HEADERS = {
    "User-Agent": IPO_HEADERS["User-Agent"],
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.goodreturns.in/",
    "Cache-Control": "no-cache",
}
GROWW_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://groww.in/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
MONEYCONTROL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "hi-IN,hi;q=0.9,en-IN;q=0.8,en;q=0.7",
    "Referer": "https://www.moneycontrol.com/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
COINGECKO_HEADERS = {
    "User-Agent": "GandhiBrosCo/1.1 (market dashboard)",
    "Accept": "application/json",
}
BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bseindia.com/",
    "Origin": "https://www.bseindia.com",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# BSE remains the source for the SENSEX benchmark card. The index table is
# populated dynamically with every index returned by NSE's allIndices feed.
BSE_WATCHLIST = [
    ("bse-sensex", "BSE Sensex", 16),
    ("bse-500", "BSE-500", 17),
    ("bse-200", "BSE-200", 23),
    ("bse-100", "BSE-100", 22),
]
BSE_ORDER = {
    "bse-sensex": 1,
    "bse-500": 2,
    "bse-200": 3,
    "bse-100": 4,
}

BUILD_LOCK = threading.Lock()
CACHE_LOCK = threading.Lock()
BSE_WARM_LOCK = threading.Lock()
CACHE = {"value": None, "built_at": 0.0}
LAST_GOOD = {"nse": None, "bse": None}
BSE_SESSION = requests.Session()
BSE_SESSION.headers.update(BSE_HEADERS)
BSE_WARMED_AT = 0.0
IPO_BUILD_LOCK = threading.Lock()
IPO_CACHE = {"value": None, "built_at": 0.0}
ETF_BUILD_LOCK = threading.Lock()
ETF_CACHE = {"value": None, "built_at": 0.0}
BENCHMARK_BUILD_LOCK = threading.Lock()
BENCHMARK_CACHE = {"value": None, "built_at": 0.0}
LAST_GOOD_BENCHMARKS = {"gold": None, "bitcoin": None}
SILVER_BUILD_LOCK = threading.Lock()
SILVER_CACHE = {"value": None, "built_at": 0.0}
LAST_GOOD_SILVER = None
NEWS_BUILD_LOCK = threading.Lock()
NEWS_CACHE = {"value": None, "built_at": 0.0}
LAST_GOOD_NEWS = {key: None for key in MONEYCONTROL_SECTIONS}


def now_ist() -> datetime:
    return datetime.now(IST)


def stamp_ist(value: datetime | None = None) -> str:
    return (value or now_ist()).isoformat(timespec="seconds")


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        text = str(value).replace(",", "").replace("%", "").strip()
        if not text or text in {"-", "--", "N/A", "NA", "null"}:
            return None
        result = float(text)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def time_part(value, fallback: str | None = None) -> str:
    if value:
        match = re.search(r"(\d{1,2}:\d{2}(?::\d{2})?)", str(value))
        if match:
            found = match.group(1)
            return found if found.count(":") == 2 else found + ":00"
    return fallback or now_ist().strftime("%H:%M:%S")


def get_json(url: str, headers: dict, timeout=HTTP_TIMEOUT):
    """Fetch a public JSON endpoint with a light retry and reject HTML error pages."""
    last_error = None
    for attempt in range(2):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code != 200:
                raise RuntimeError(f"HTTP {response.status_code}")
            text = response.text.strip()
            if not text or text.startswith("<") or "Access Denied" in text[:700]:
                raise RuntimeError("The exchange returned a non-JSON response")
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt == 0:
                time.sleep(0.18)
    raise RuntimeError(str(last_error or "Feed request failed"))


def warm_bse(force=False):
    """Establish the BSE site session used by its public API endpoints."""
    global BSE_WARMED_AT
    with BSE_WARM_LOCK:
        now = time.time()
        if BSE_WARMED_AT and now - BSE_WARMED_AT < (3 if force else 300):
            return
        try:
            landing_headers = {
                "User-Agent": BSE_HEADERS["User-Agent"],
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": BSE_HEADERS["Accept-Language"],
                "Referer": "https://www.bseindia.com/",
            }
            # Touch both the public frontend and its beta landing page. BSE's
            # CDN sometimes requires this warm-up before the API host responds.
            for landing in ("https://www.bseindia.com/", "https://beta.bseindia.com/"):
                BSE_SESSION.get(landing, headers=landing_headers, timeout=HTTP_TIMEOUT)
            BSE_WARMED_AT = time.time()
        except requests.RequestException:
            # Continue to the JSON endpoint; the public API occasionally works
            # without the page preflight, and the failure is surfaced if not.
            BSE_WARMED_AT = time.time()


def get_bse_response(url: str, timeout=HTTP_TIMEOUT):
    warm_bse()
    last_error = None
    for attempt in range(2):
        try:
            response = BSE_SESSION.get(url, headers=BSE_HEADERS, timeout=timeout)
            if response.status_code == 403 and attempt == 0:
                warm_bse(force=True)
                continue
            if response.status_code != 200:
                raise RuntimeError(f"HTTP {response.status_code}")
            text = response.text.strip()
            if not text or text.startswith("<") or "Access Denied" in text[:700]:
                raise RuntimeError("BSE returned a non-JSON response")
            return response
        except Exception as exc:
            last_error = exc
            if attempt == 0:
                time.sleep(0.18)
    raise RuntimeError(str(last_error or "BSE feed request failed"))


def get_bse_json(url: str):
    try:
        return get_bse_response(url).json()
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc


def fetch_nse():
    """Load the NSE broad-indices payload and market-status timestamp."""
    urls = [f"{NSE_API}/allIndices", f"{NSE_API}/marketStatus"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(get_json, url, NSE_HEADERS) for url in urls]
        indices_json = futures[0].result()
        try:
            status_json = futures[1].result()
        except Exception:
            status_json = {}

    raw_items = indices_json.get("data", []) if isinstance(indices_json, dict) else []
    market_states = status_json.get("marketState", []) if isinstance(status_json, dict) else []
    capital = next(
        (item for item in market_states if str(item.get("market", "")).lower() == "capital market"),
        {},
    )
    nse_time = capital.get("tradeDate") or ""
    fetched_at = stamp_ist()
    rows = []
    used_ids = set()
    for ordinal, item in enumerate(raw_items, start=1):
        if not isinstance(item, dict):
            continue
        name = str(item.get("index", "")).strip()
        if not name:
            continue
        row_id = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or f"nse-index-{ordinal}"
        if row_id in used_ids:
            row_id = f"{row_id}-{ordinal}"
        used_ids.add(row_id)
        indicative_close = number(item.get("indicativeClose"))
        if indicative_close == 0:
            indicative_close = None
        rows.append({
            "id": row_id,
            "name": name,
            "category": str(item.get("key") or "OTHER INDICES"),
            "exchange": "NSE",
            "last": number(item.get("last")),
            "high": number(item.get("high")),
            "low": number(item.get("low")),
            "change": number(item.get("variation")),
            "changePct": number(item.get("percentChange")),
            "open": number(item.get("open")),
            "indicativeClose": indicative_close,
            "previousClose": number(item.get("previousClose")),
            "previousDayVal": number(item.get("previousDayVal")),
            "oneWeekAgoVal": number(item.get("oneWeekAgoVal")),
            "oneMonthAgoVal": number(item.get("oneMonthAgoVal")),
            "oneYearAgoVal": number(item.get("oneYearAgoVal")),
            "yearHigh": number(item.get("yearHigh")),
            "yearLow": number(item.get("yearLow")),
            "datePreviousDay": str(item.get("previousDay") or ""),
            "dateOneWeekAgo": str(item.get("oneWeekAgo") or ""),
            "dateOneMonthAgo": str(item.get("date30dAgo") or ""),
            "dateOneYearAgo": str(item.get("date365dAgo") or ""),
            "time": time_part(nse_time, time_part(fetched_at)),
            "order": ordinal,
            "precision": 4 if name.strip().upper() == "INDIA VIX" else 2,
        })

    if not rows:
        raise RuntimeError("NSE returned no rows from its all-indices feed")
    gift_raw = status_json.get("giftnifty", {}) if isinstance(status_json, dict) else {}
    gift_raw = gift_raw if isinstance(gift_raw, dict) else {}
    gift_nifty = {
        "id": "gift-nifty",
        "name": "GIFT NIFTY",
        "last": number(gift_raw.get("LASTPRICE")),
        "change": number(gift_raw.get("DAYCHANGE")),
        "changePct": number(gift_raw.get("PERCHANGE")),
        "expiry": str(gift_raw.get("EXPIRYDATE") or ""),
        "dateTime": str(gift_raw.get("TIMESTMP") or ""),
        "time": time_part(gift_raw.get("TIMESTMP"), ""),
        "precision": 2,
        "source": "NSE International Exchange",
    }
    market = {
        "status": str(capital.get("marketStatus") or "Unknown"),
        "message": str(capital.get("marketStatusMessage") or ""),
        "tradeDate": str(nse_time),
        "index": str(capital.get("index") or "NIFTY 50"),
        "giftNifty": gift_nifty,
    }
    return {
        "rows": rows,
        "market": market,
        "fetchedAt": fetched_at,
        "sourceTime": time_part(nse_time, time_part(fetched_at)),
    }


def _goodreturns_page(url: str):
    response = requests.get(url, headers=GOODRETURNS_HEADERS, timeout=HTTP_TIMEOUT)
    if response.status_code != 200:
        raise RuntimeError(f"Goodreturns HTTP {response.status_code}")
    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    if not title or "goodreturns" not in title.lower():
        raise RuntimeError("Goodreturns returned an unexpected page")
    return soup, title


def _amounts_in_text(value: str) -> list[float]:
    text = str(value or "").replace("\xa0", " ").replace("−", "-").replace("–", "-")
    matches = re.findall(r"[-+]?\s*₹?\s*\d[\d,]*(?:\.\d+)?", text)
    return [parsed for match in matches if (parsed := number(match.replace("₹", "").replace(" ", ""))) is not None]


def _goodreturns_date(title: str) -> str:
    match = re.search(r"\((\d{1,2}\s+[A-Za-z]+\s+\d{4})\)", title)
    return match.group(1) if match else ""


def fetch_india_gold():
    """Fetch Goodreturns' India 24K gold reference rate, quoted per 10 grams."""
    soup, title = _goodreturns_page(GOODRETURNS_GOLD_URL)
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError("Gold rate table was not found")
    table = tables[0]
    data_row = None
    for tr in table.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
        if len(cells) >= 2 and cells[0].strip() == "10":
            data_row = cells
            break
    if not data_row:
        raise RuntimeError("India 24K gold 10-gram row was not found")
    amounts = _amounts_in_text(data_row[1])
    if not amounts:
        raise RuntimeError("India 24K gold price could not be parsed")
    last = amounts[0]
    change = amounts[1] if len(amounts) > 1 else None
    previous = last - change if change is not None else None
    change_pct = (change / previous * 100) if change is not None and previous else None
    return {
        "id": "india-gold",
        "name": "India Gold 24K",
        "last": last,
        "change": change,
        "changePct": change_pct,
        "high": None,
        "low": None,
        "precision": 0,
        "unit": "₹/10g",
        "date": _goodreturns_date(title),
        "source": "Goodreturns India rates",
        "sourceUrl": GOODRETURNS_GOLD_URL,
    }


def fetch_india_silver():
    """Fetch the live MCX Silver futures quote embedded in Groww's public quote page."""
    request_url = f"{GROWW_SILVER_URL}?_={int(time.time() * 1000)}"
    response = requests.get(request_url, headers=GROWW_HEADERS, timeout=(5, 12))
    if response.status_code != 200:
        raise RuntimeError(f"Groww returned HTTP {response.status_code}")
    soup = BeautifulSoup(response.text, "html.parser")
    next_data = soup.select_one("script#__NEXT_DATA__")
    if next_data is None:
        raise RuntimeError("Groww Silver quote data was not found")
    try:
        page_data = json.loads(next_data.string or next_data.get_text())
        static_data = page_data.get("props", {}).get("pageProps", {}).get("staticData", {})
        details = static_data.get("livePriceDetails", {})
        contract = static_data.get("contractDetails", {})
    except (TypeError, ValueError, AttributeError):
        raise RuntimeError("Groww Silver quote data could not be parsed")
    if not isinstance(details, dict) or not details:
        raise RuntimeError("Groww did not return a live MCX Silver quote")

    last = number(details.get("ltp"))
    if last is None:
        raise RuntimeError("Groww MCX Silver last-traded price was empty")
    previous_close = number(details.get("close"))
    change = number(details.get("dayChange"))
    if change is None and previous_close is not None:
        change = last - previous_close
    change_pct = change / previous_close * 100 if change is not None and previous_close else None

    timestamp = number(details.get("tsInMillis"))
    if timestamp is None or timestamp <= 0:
        raise RuntimeError("Groww MCX Silver quote timestamp was missing")
    epoch = timestamp / 1000 if timestamp > 1_000_000_000_000 else timestamp
    try:
        source_datetime = datetime.fromtimestamp(epoch, tz=IST)
    except (OverflowError, OSError, ValueError):
        raise RuntimeError("Groww MCX Silver quote timestamp was invalid")

    return {
        "id": "india-silver",
        "name": "MCX Silver Futures",
        "last": last,
        "change": change,
        "changePct": change_pct,
        "high": number(details.get("high")),
        "low": number(details.get("low")),
        "open": number(details.get("open")),
        "previousClose": previous_close,
        "precision": 0,
        "unit": "₹/kg",
        "date": source_datetime.strftime("%d %b %Y"),
        "time": source_datetime.strftime("%H:%M:%S"),
        "dateTime": source_datetime.isoformat(timespec="seconds"),
        "contract": str(contract.get("companyShortName") or contract.get("displayName") or "MCX Silver futures"),
        "source": "Groww · live MCX Silver futures",
        "sourceUrl": GROWW_SILVER_URL,
    }


def fetch_bitcoin_inr():
    """Fetch BTC/INR and rolling 24-hour change/range from CoinGecko."""
    payload = get_json(COINGECKO_BTC_URL, COINGECKO_HEADERS, timeout=(4, 12))
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
        raise RuntimeError("CoinGecko did not return a Bitcoin quote")
    item = payload[0]
    return {
        "id": "bitcoin",
        "name": "Bitcoin",
        "last": number(item.get("current_price")),
        "change": number(item.get("price_change_24h")),
        "changePct": number(item.get("price_change_percentage_24h_in_currency", item.get("price_change_percentage_24h"))),
        "high": number(item.get("high_24h")),
        "low": number(item.get("low_24h")),
        "precision": 0,
        "unit": "₹/BTC",
        "dateTime": str(item.get("last_updated") or ""),
        "source": "CoinGecko",
        "sourceUrl": "https://www.coingecko.com/en/coins/bitcoin",
    }


def build_benchmark_snapshot():
    errors = {}
    quotes = {}
    fetchers = {"gold": fetch_india_gold, "bitcoin": fetch_bitcoin_inr}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {key: pool.submit(fetcher) for key, fetcher in fetchers.items()}
        for key, future in futures.items():
            try:
                quote = future.result()
                if quote.get("last") is None:
                    raise RuntimeError("The source returned an empty price")
                quote["sourceState"] = "live"
                LAST_GOOD_BENCHMARKS[key] = copy.deepcopy(quote)
                quotes[key] = quote
            except Exception as exc:
                errors[key] = str(exc)
                previous = LAST_GOOD_BENCHMARKS.get(key)
                if previous:
                    quotes[key] = {**copy.deepcopy(previous), "sourceState": "stale"}
                else:
                    quotes[key] = None
    return {
        "ok": any(quote and quote.get("last") is not None for quote in quotes.values()),
        "benchmarks": quotes,
        "checkedAt": stamp_ist(),
        "sources": {key: quote.get("sourceState", "offline") if quote else "offline" for key, quote in quotes.items()},
        "errors": errors,
        "refreshSeconds": BENCHMARK_CACHE_TTL,
        "timezone": "Asia/Kolkata",
    }


def get_benchmark_snapshot(force=False):
    with BENCHMARK_BUILD_LOCK:
        with CACHE_LOCK:
            if not force and BENCHMARK_CACHE["value"] is not None and time.time() - BENCHMARK_CACHE["built_at"] < BENCHMARK_CACHE_TTL:
                return copy.deepcopy(BENCHMARK_CACHE["value"])
        snapshot = build_benchmark_snapshot()
        with CACHE_LOCK:
            BENCHMARK_CACHE["value"] = snapshot
            BENCHMARK_CACHE["built_at"] = time.time()
        return copy.deepcopy(snapshot)


def build_silver_snapshot():
    global LAST_GOOD_SILVER
    try:
        quote = fetch_india_silver()
        if quote.get("last") is None:
            raise RuntimeError("Groww returned an empty Silver futures price")
        previous_time = LAST_GOOD_SILVER.get("dateTime") if LAST_GOOD_SILVER else None
        if previous_time and quote.get("dateTime"):
            previous_datetime = datetime.fromisoformat(previous_time)
            current_datetime = datetime.fromisoformat(quote["dateTime"])
            if current_datetime < previous_datetime:
                raise RuntimeError("Groww returned an older Silver quote timestamp")
        quote["sourceState"] = "live"
        LAST_GOOD_SILVER = copy.deepcopy(quote)
        return {
            "ok": True,
            "benchmark": quote,
            "sourceState": "live",
            "checkedAt": stamp_ist(),
            "refreshSeconds": SILVER_CACHE_TTL,
            "timezone": "Asia/Kolkata",
        }
    except Exception as exc:
        previous = copy.deepcopy(LAST_GOOD_SILVER)
        if previous:
            previous["sourceState"] = "stale"
        return {
            "ok": bool(previous),
            "benchmark": previous,
            "sourceState": "stale" if previous else "offline",
            "checkedAt": stamp_ist(),
            "refreshSeconds": SILVER_CACHE_TTL,
            "timezone": "Asia/Kolkata",
            "error": str(exc),
        }


def get_silver_snapshot(force=False):
    with SILVER_BUILD_LOCK:
        if not force and SILVER_CACHE["value"] is not None and time.time() - SILVER_CACHE["built_at"] < SILVER_CACHE_TTL:
            return copy.deepcopy(SILVER_CACHE["value"])
        snapshot = build_silver_snapshot()
        SILVER_CACHE["value"] = snapshot
        SILVER_CACHE["built_at"] = time.time()
        return copy.deepcopy(snapshot)


def fetch_moneycontrol_section(section_key: str):
    """Fetch the latest five headlines shown in a Moneycontrol category page."""
    config = MONEYCONTROL_SECTIONS.get(section_key)
    if not config:
        raise ValueError(f"Unknown Moneycontrol news section: {section_key}")
    source_host = urlparse(config["url"]).netloc.lower()
    headers = {**MONEYCONTROL_HEADERS, "Referer": f"https://{source_host}/"}
    response = requests.get(config["url"], headers=headers, timeout=(5, 15))
    if response.status_code != 200:
        raise RuntimeError(f"{config['sourceName']} {config['title']} page returned HTTP {response.status_code}")
    soup = BeautifulSoup(response.text, "html.parser")
    if config.get("parser") == "hindi_latest":
        # The Hindi category page leads with a featured H2, then its latest-news H3 list.
        featured = soup.select_one('h2[class*="topNews_h2"]')
        headline_nodes = ([featured] if featured else []) + soup.find_all("h3")
    else:
        headline_nodes = soup.select('li[id^="newslist-"]')

    stories = []
    seen = set()
    for node in headline_nodes:
        if config.get("parser") == "hindi_latest":
            title_node = node
            link = node.find("a", href=True) or node.find_parent("a", href=True)
        else:
            title_node = node.find(["h2", "h3"])
            link = node.find("a", href=True)
        if link is None:
            continue
        title = " ".join((title_node.get_text(" ", strip=True) if title_node else link.get("title", "")).split())
        if not title:
            title = " ".join(link.get_text(" ", strip=True).split())
        if not title:
            continue
        url = urljoin(config["url"], link.get("href", ""))
        parsed = urlparse(url)
        allowed_hosts = {"hindi.moneycontrol.com"} if section_key == "hindi" else {"www.moneycontrol.com", "moneycontrol.com"}
        if parsed.scheme != "https" or parsed.netloc.lower() not in allowed_hosts:
            continue
        if url in seen:
            continue
        seen.add(url)
        stories.append({
            "title": title,
            "url": url,
            "source": config["sourceName"],
            "section": section_key,
        })
        if len(stories) == 5:
            break
    if len(stories) < 5:
        raise RuntimeError(f"{config['sourceName']} {config['title']} page returned only {len(stories)} usable headlines")
    return stories


def fetch_moneycontrol_news():
    """Load five stories from each of the three English pages and the Hindi news page."""
    stories = {}
    errors = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {key: pool.submit(fetch_moneycontrol_section, key) for key in MONEYCONTROL_SECTIONS}
        for key, future in futures.items():
            try:
                stories[key] = future.result()
            except Exception as exc:
                errors[key] = str(exc)
    return stories, errors


def build_news_snapshot():
    global LAST_GOOD_NEWS
    fetched, errors = fetch_moneycontrol_news()
    sections = {}
    for key, config in MONEYCONTROL_SECTIONS.items():
        if key in fetched:
            section = {
                "title": config["title"],
                "sourceName": config["sourceName"],
                "sourceUrl": config["url"],
                "stories": fetched[key],
                "sourceState": "live",
            }
            LAST_GOOD_NEWS[key] = copy.deepcopy(section)
            sections[key] = section
        else:
            previous = copy.deepcopy(LAST_GOOD_NEWS.get(key))
            if previous:
                previous["sourceState"] = "stale"
                sections[key] = previous
            else:
                sections[key] = {
                    "title": config["title"],
                    "sourceName": config["sourceName"],
                    "sourceUrl": config["url"],
                    "stories": [],
                    "sourceState": "offline",
                }

    states = [section["sourceState"] for section in sections.values()]
    if all(state == "live" for state in states):
        source_state = "live"
    elif any(state == "live" for state in states):
        source_state = "partial"
    elif any(state == "stale" for state in states):
        source_state = "stale"
    else:
        source_state = "offline"
    ok = any(section["stories"] for section in sections.values())
    return {
        "ok": ok,
        "sections": sections,
        "sourceState": source_state,
        "sourceUrl": "https://www.moneycontrol.com/news/",
        "checkedAt": stamp_ist(),
        "refreshSeconds": NEWS_CACHE_TTL,
        "timezone": "Asia/Kolkata",
        "errors": errors,
    }


def get_news_snapshot(force=False):
    with NEWS_BUILD_LOCK:
        if not force and NEWS_CACHE["value"] is not None and time.time() - NEWS_CACHE["built_at"] < NEWS_CACHE_TTL:
            return copy.deepcopy(NEWS_CACHE["value"])
        snapshot = build_news_snapshot()
        NEWS_CACHE["value"] = snapshot
        NEWS_CACHE["built_at"] = time.time()
        return copy.deepcopy(snapshot)


def parse_bse_graph(raw_text: str):
    """Parse BSE's minute-chart response (JSON string joined with #@#)."""
    wrapped = json.loads(raw_text) if raw_text.lstrip().startswith('"') else raw_text
    if isinstance(wrapped, dict):
        return {}
    parts = str(wrapped).split("#@#")
    if len(parts) < 2:
        return {}
    try:
        headline = json.loads(parts[0])
        points = json.loads(parts[1])
    except Exception:
        return {}
    header = headline[0] if isinstance(headline, list) and headline else {}
    points = points if isinstance(points, list) else []
    # value is the live-session series; value1 is typically the pre-open trace.
    trade_values = [number(point.get("value")) for point in points if isinstance(point, dict)]
    trade_values = [value for value in trade_values if value is not None]
    all_values = trade_values or [
        number(point.get("value1")) for point in points if isinstance(point, dict)
    ]
    all_values = [value for value in all_values if value is not None]
    open_value = trade_values[0] if trade_values else next(
        (number(point.get("value1")) for point in points if isinstance(point, dict) and number(point.get("value1")) is not None),
        None,
    )
    return {
        "last": number(header.get("LatestVal")),
        "previousClose": number(header.get("PreClose")),
        "high": max(all_values) if all_values else None,
        "low": min(all_values) if all_values else None,
        "open": open_value,
        "sourceTime": time_part(header.get("LatestTime") or header.get("currDate")),
    }


def fetch_bse_movers():
    url = f"{BSE_API}/IndexMovers/w?FLAGCODE=&LNFLAG=&PageNo=1&RecordsPerPage=100&IsHomePage=&_={int(time.time() * 1000)}"
    payload = get_bse_json(url)
    items = payload.get("Table", payload.get("table", [])) if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        raise RuntimeError("BSE index list was not in the expected format")
    return {int(number(item.get("code")) or -1): item for item in items if isinstance(item, dict)}


def fetch_bse_graph(code: int):
    url = (
        f"{BSE_API}/SensexGraphData/w?index={code}&flag=0&sector=&seriesid=R"
        f"&frd=null&tod=null&_={int(time.time() * 1000)}"
    )
    response = get_bse_response(url)
    return parse_bse_graph(response.text.strip())


def fetch_bse_sensex_detail():
    url = f"{BSE_REALTIME_API}/GetSensexDatanew/w?_={int(time.time() * 1000)}"
    # Keep this request independent of the minute chart; it includes official
    # open/high/low fields for the SENSEX row.
    payload = get_bse_json(url)
    items = payload if isinstance(payload, list) else payload.get("Table", [])
    item = next((x for x in items if str(x.get("indxcode")) == "16"), None)
    if not item:
        return {}
    return {
        "last": number(item.get("ltp")),
        "high": number(item.get("High")),
        "low": number(item.get("Low")),
        "open": number(item.get("I_open")),
        "change": number(item.get("chg")),
        "changePct": number(item.get("perchg")),
        "time": time_part(item.get("dttm")),
    }


def fetch_bse():
    """Load BSE live index movers plus current-day chart ranges."""
    warm_bse()
    with ThreadPoolExecutor(max_workers=6) as pool:
        movers_future = pool.submit(fetch_bse_movers)
        sensex_future = pool.submit(fetch_bse_sensex_detail)
        graph_futures = {code: pool.submit(fetch_bse_graph, code) for code in (16, 17, 22, 23)}

        try:
            movers = movers_future.result()
        except Exception:
            movers = {}
        try:
            sensex_detail = sensex_future.result()
        except Exception:
            sensex_detail = {}
        graph_details = {}
        for code, future in graph_futures.items():
            try:
                graph_details[code] = future.result()
            except Exception:
                graph_details[code] = {}

    rows = []
    fetched_at = stamp_ist()
    for row_id, name, code in BSE_WATCHLIST:
        item = movers.get(code, {})
        # The realtime Sensex endpoint includes richer fields than the general
        # BSE movers endpoint; use it for the Sensex snapshot when available.
        graph = graph_details.get(code, {})
        detail = sensex_detail if code == 16 else {}
        last = number(item.get("LTP", item.get("ltp")))
        if last is None:
            last = detail.get("last") if detail.get("last") is not None else graph.get("last")
        change = number(item.get("change", item.get("CHANGE", item.get("chg"))))
        if change is None:
            change = detail.get("change")
        if change is None and last is not None and graph.get("previousClose") is not None:
            change = last - graph["previousClose"]
        change_pct = number(item.get("PERCENTCHG", item.get("perchg")))
        if change_pct is None:
            change_pct = detail.get("changePct")
        if change_pct is None and change is not None and graph.get("previousClose"):
            change_pct = change / graph["previousClose"] * 100
        source_time = time_part(item.get("DT_TM") or item.get("dttm") or detail.get("time") or graph.get("sourceTime"), time_part(fetched_at))
        high = detail.get("high") if detail.get("high") is not None else graph.get("high")
        low = detail.get("low") if detail.get("low") is not None else graph.get("low")
        open_value = detail.get("open") if detail.get("open") is not None else graph.get("open")
        # Ensure a valid latest quote is inside the range, even if the chart
        # service's last sample trails the quote by a fraction of a second.
        if last is not None:
            high = max([v for v in (high, last) if v is not None]) if high is not None else last
            low = min([v for v in (low, last) if v is not None]) if low is not None else last
        if last is None and detail.get("last") is None:
            continue
        rows.append({
            "id": row_id,
            "name": name,
            "exchange": "BSE",
            "last": last,
            "high": high,
            "low": low,
            "change": change,
            "changePct": change_pct,
            "open": open_value,
            "time": source_time,
            "order": BSE_ORDER[row_id],
            "precision": 2,
        })
    if not rows:
        raise RuntimeError("BSE response did not contain the requested index rows")
    return {"rows": rows, "fetchedAt": fetched_at}


def build_snapshot():
    errors = {}
    try:
        current_nse = fetch_nse()
        LAST_GOOD["nse"] = current_nse
        nse_state = "live"
    except Exception as exc:
        errors["NSE"] = str(exc)
        current_nse = LAST_GOOD["nse"]
        nse_state = "stale" if current_nse else "offline"

    try:
        current_bse = fetch_bse()
        LAST_GOOD["bse"] = current_bse
        bse_state = "live"
    except Exception as exc:
        errors["BSE"] = str(exc)
        current_bse = LAST_GOOD["bse"]
        bse_state = "stale" if current_bse else "offline"

    all_rows = []
    if current_nse:
        all_rows.extend(copy.deepcopy(current_nse["rows"]))
    if current_bse:
        all_rows.extend(copy.deepcopy(current_bse["rows"]))
    all_rows.sort(key=lambda row: (0 if row["exchange"] == "NSE" else 1, row["order"]))

    if not all_rows:
        return {
            "ok": False,
            "indices": [],
            "checkedAt": stamp_ist(),
            "market": {"status": "Unknown", "message": "Waiting for exchange data"},
            "sources": {
                "NSE": {"state": nse_state, "fetchedAt": None},
                "BSE": {"state": bse_state, "fetchedAt": None},
            },
            "errors": errors,
        }

    market = current_nse.get("market") if current_nse else {"status": "Unknown", "message": ""}
    return {
        "ok": True,
        "indices": all_rows,
        "checkedAt": stamp_ist(),
        "market": market,
        "sources": {
            "NSE": {"state": nse_state, "fetchedAt": current_nse.get("fetchedAt") if current_nse else None},
            "BSE": {"state": bse_state, "fetchedAt": current_bse.get("fetchedAt") if current_bse else None},
        },
        "errors": errors,
        "pollSeconds": 15,
        "timezone": "Asia/Kolkata",
    }


def get_snapshot(force=False):
    with BUILD_LOCK:
        with CACHE_LOCK:
            if not force and CACHE["value"] is not None and time.time() - CACHE["built_at"] < CACHE_TTL:
                return copy.deepcopy(CACHE["value"])
        snapshot = build_snapshot()
        with CACHE_LOCK:
            CACHE["value"] = snapshot
            CACHE["built_at"] = time.time()
        return copy.deepcopy(snapshot)


def cleaned_cell_text(cell, first_line=False):
    if cell is None:
        return ""
    parts = [part.replace("\xa0", " ").strip() for part in cell.stripped_strings]
    parts = [part for part in parts if part]
    if first_line:
        return parts[0] if parts else ""
    return " ".join(parts)


def parse_ipo_number(text):
    match = re.search(r"[-+]?\d[\d,]*(?:\.\d+)?", str(text or ""))
    return number(match.group(0)) if match else None


def decode_cf_protected_text(encoded):
    """Decode Cloudflare's lightweight email obfuscation used for IPO listing markers."""
    try:
        data = bytes.fromhex(str(encoded or ""))
        if len(data) < 2:
            return ""
        key = data[0]
        return bytes(value ^ key for value in data[1:]).decode("utf-8", errors="replace")
    except (ValueError, TypeError):
        return ""


def decoded_cell_text(cell, first_line=False):
    """Return cell text while restoring InvestorGain's protected listing/status marker."""
    if cell is None:
        return ""
    clone = BeautifulSoup(str(cell), "html.parser")
    for protected in clone.select("a.__cf_email__[data-cfemail]"):
        decoded = decode_cf_protected_text(protected.get("data-cfemail"))
        protected.replace_with(decoded or protected.get_text(" ", strip=True))
    return cleaned_cell_text(clone, first_line=first_line)


def ipo_status_details(name_cell):
    status_names = {
        "U": "Upcoming", "UPCOMING": "Upcoming",
        "O": "Open", "OPEN": "Open",
        "CT": "Closing Today", "CLOSING TODAY": "Closing Today",
        "C": "Closed", "CLOSED": "Closed",
        "L": "Listed", "LISTED": "Listed",
    }
    badges = [badge.get_text(" ", strip=True).upper() for badge in name_cell.select("span.badge")]
    code = next((status_names[badge] for badge in badges if badge in status_names), None)
    detail_parts = []
    for span in name_cell.select("span.text-danger, span.text-success, span.text-warning, span.text-info"):
        value = decoded_cell_text(span)
        if value:
            detail_parts.append(value)
    detail = " ".join(detail_parts)
    marker = re.search(
        r"(?<![A-Z0-9])(CT|U|O|C|L)\s*(?:@\s*([-+]?\d[\d,]*(?:\.\d+)?))?\s*(?:\(\s*([-+]?\d+(?:\.\d+)?)\s*%\s*\))?",
        detail,
        re.IGNORECASE,
    )
    marker_code = marker.group(1).upper() if marker else None
    if code is None and marker_code:
        code = status_names.get(marker_code)
    if code is None:
        classes = set(name_cell.get("class", []))
        code = "Upcoming" if "bg-2-lightyellow" in classes else "Open" if "bg-2-green" in classes else "Listed"
    code_by_name = {name: code for code, name in (("U", "Upcoming"), ("O", "Open"), ("CT", "Closing Today"), ("C", "Closed"), ("L", "Listed"))}
    status_code = code_by_name.get(code, "L")
    listing_price = parse_ipo_number(marker.group(2)) if marker and marker.group(2) else None
    listing_return = number(marker.group(3)) if marker and marker.group(3) else None
    return status_code, code, detail, listing_price, listing_return


def fetch_ipo_snapshot():
    """Fetch every row and published field in InvestorGain's live IPO GMP table."""
    response = requests.get(IPO_URL, headers=IPO_HEADERS, timeout=(5, 15))
    if response.status_code != 200:
        raise RuntimeError(f"InvestorGain returned HTTP {response.status_code}")
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", id="reportTable") or soup.select_one("table.report-data-table")
    if table is None:
        raise RuntimeError("InvestorGain IPO table was not found in the page")

    rows = []
    for tr in table.select("tr"):
        name_cell = tr.find("td", attrs={"data-label": "Name"})
        if not name_cell:
            continue
        name_link = name_cell.find("a", href=True)
        cells = tr.find_all("td", recursive=False)
        if not name_link or len(cells) < 13:
            continue

        badges = [badge.get_text(" ", strip=True).upper() for badge in name_cell.select("span.badge")]
        status_badges = {"U", "O", "CT", "C", "L", "UPCOMING", "OPEN", "CLOSING TODAY", "CLOSED", "LISTED"}
        category = next((badge for badge in badges if badge not in status_badges), "IPO")
        status_code, status, listing_detail, listing_price, listing_return = ipo_status_details(name_cell)

        gmp_cell = cells[1]
        gmp_text = cleaned_cell_text(gmp_cell)
        gmp_bold = gmp_cell.find("b")
        gmp_value_text = gmp_bold.get_text(" ", strip=True) if gmp_bold else gmp_text
        gmp = parse_ipo_number(gmp_value_text)
        gmp_pct_match = re.search(r"\(\s*([-+]?\d+(?:\.\d+)?)\s*%\s*\)", gmp_text)
        gmp_pct = number(gmp_pct_match.group(1)) if gmp_pct_match else None
        small = gmp_cell.find("small")
        gmp_range = small.get_text(" ", strip=True) if small else ""
        price_text = cleaned_cell_text(cells[4])
        price = parse_ipo_number(price_text)
        anchor_text = decoded_cell_text(cells[12])

        row = {
            "name": name_link.get_text(" ", strip=True),
            "category": category,
            "status": status,
            "statusCode": status_code,
            "listingDetail": listing_detail,
            "listingPrice": listing_price,
            "listingReturnPct": listing_return,
            "gmp": gmp,
            "gmpPercent": gmp_pct,
            "gmpRange": gmp_range,
            "gmpText": gmp_text,
            "rating": decoded_cell_text(cells[2]),
            "subscription": decoded_cell_text(cells[3]),
            "price": price,
            "priceText": price_text,
            "issueSize": decoded_cell_text(cells[5]),
            "lotSize": decoded_cell_text(cells[6]),
            "openDate": decoded_cell_text(cells[7], first_line=True),
            "openRaw": decoded_cell_text(cells[7]),
            "closeDate": decoded_cell_text(cells[8], first_line=True),
            "closeRaw": decoded_cell_text(cells[8]),
            "allotmentDate": decoded_cell_text(cells[9], first_line=True),
            "allotmentRaw": decoded_cell_text(cells[9]),
            "listingDate": decoded_cell_text(cells[10], first_line=True),
            "listingRaw": decoded_cell_text(cells[10]),
            "updatedOn": decoded_cell_text(cells[11]),
            "anchor": "✅" in anchor_text or "✔" in anchor_text or "✓" in anchor_text,
            "anchorText": anchor_text,
            "url": urljoin(IPO_URL, name_link.get("href", "")),
        }
        rows.append(row)

    if not rows:
        raise RuntimeError("InvestorGain page loaded, but no IPO rows were parsed")
    status_counts = {"U": 0, "O": 0, "CT": 0, "C": 0, "L": 0}
    for row in rows:
        status_counts[row["statusCode"]] = status_counts.get(row["statusCode"], 0) + 1
    return {
        "ok": True,
        "rows": rows,
        "summary": {
            "total": len(rows),
            "upcoming": status_counts["U"],
            "open": status_counts["O"],
            "closingToday": status_counts["CT"],
            "closed": status_counts["C"],
            "listed": status_counts["L"],
        },
        "checkedAt": stamp_ist(),
        "sourceState": "live",
        "sourceUrl": IPO_URL,
        "refreshSeconds": IPO_CACHE_TTL,
    }


def get_ipo_snapshot(force=False):
    with IPO_BUILD_LOCK:
        if not force and IPO_CACHE["value"] is not None and time.time() - IPO_CACHE["built_at"] < IPO_CACHE_TTL:
            return copy.deepcopy(IPO_CACHE["value"])
        try:
            snapshot = fetch_ipo_snapshot()
            IPO_CACHE["value"] = snapshot
            IPO_CACHE["built_at"] = time.time()
            return copy.deepcopy(snapshot)
        except Exception as exc:
            if IPO_CACHE["value"] is not None:
                snapshot = copy.deepcopy(IPO_CACHE["value"])
                snapshot["sourceState"] = "stale"
                snapshot["checkedAt"] = stamp_ist()
                snapshot["error"] = str(exc)
                return snapshot
            return {
                "ok": False,
                "rows": [],
                "summary": {"total": 0, "upcoming": 0, "open": 0, "closingToday": 0, "closed": 0, "listed": 0},
                "checkedAt": stamp_ist(),
                "sourceState": "offline",
                "sourceUrl": IPO_URL,
                "error": str(exc),
            }


def fetch_etf_snapshot():
    """Fetch the complete NSE ETF market-watch table from its page-backed JSON API."""
    payload = get_json(ETF_DATA_URL, ETF_HEADERS, timeout=(5, 18))
    market_data = payload.get("data", {}) if isinstance(payload, dict) else {}
    raw_rows = market_data.get("data", []) if isinstance(market_data, dict) else []
    if not isinstance(raw_rows, list) or not raw_rows:
        raise RuntimeError("NSE ETF response did not contain any ETF rows")

    rows = []
    for item in raw_rows:
        if not isinstance(item, dict) or not item.get("symbol"):
            continue
        value_rupees = number(item.get("totalTradedValue"))
        rows.append({
            "symbol": str(item.get("symbol", "")).strip(),
            "underlying": str(item.get("assets") or "—").strip(),
            "category": str(item.get("mainCatagory") or "Other").strip(),
            "subCategory": str(item.get("subCatagory") or "—").strip(),
            "open": number(item.get("open")),
            "high": number(item.get("dayHigh")),
            "low": number(item.get("dayLow")),
            "previousClose": number(item.get("previousClose")),
            "ltp": number(item.get("lastPrice")),
            "change": number(item.get("change")),
            "changePct": number(item.get("pChange", item.get("PChange"))),
            "volume": number(item.get("totalTradedVolume")),
            "valueCrores": value_rupees / 10000000 if value_rupees is not None else None,
            "iNAV": number(item.get("inav")),
            "nav": number(item.get("nav")),
            "yearHigh": number(item.get("yearHigh")),
            "yearLow": number(item.get("yearLow")),
            "change30d": number(item.get("perChange30d")),
        })

    if not rows:
        raise RuntimeError("NSE ETF data was received, but no ETF symbols were parsed")
    adu = market_data.get("aduCount", {}) if isinstance(market_data, dict) else {}
    totals = market_data.get("totals", {}) if isinstance(market_data, dict) else {}
    total_value = number(totals.get("value")) if isinstance(totals, dict) else None
    market = market_data.get("marketStatus", {}) if isinstance(market_data, dict) else {}
    return {
        "ok": True,
        "rows": rows,
        "summary": {
            "total": len(rows),
            "advances": int(number(adu.get("advances")) or 0),
            "declines": int(number(adu.get("declines")) or 0),
            "unchanged": int(number(adu.get("unchange")) or 0),
            "volume": number(totals.get("volume")) if isinstance(totals, dict) else None,
            "valueCrores": total_value / 10000000 if total_value is not None else None,
        },
        "checkedAt": stamp_ist(),
        "sourceTimestamp": str(market_data.get("timestamp") or ""),
        "navDate": str(market_data.get("navDate") or ""),
        "market": market if isinstance(market, dict) else {},
        "sourceState": "live",
        "sourceUrl": ETF_URL,
        "refreshSeconds": ETF_CACHE_TTL,
    }


def get_etf_snapshot(force=False):
    with ETF_BUILD_LOCK:
        if not force and ETF_CACHE["value"] is not None and time.time() - ETF_CACHE["built_at"] < ETF_CACHE_TTL:
            return copy.deepcopy(ETF_CACHE["value"])
        try:
            snapshot = fetch_etf_snapshot()
            ETF_CACHE["value"] = snapshot
            ETF_CACHE["built_at"] = time.time()
            return copy.deepcopy(snapshot)
        except Exception as exc:
            if ETF_CACHE["value"] is not None:
                snapshot = copy.deepcopy(ETF_CACHE["value"])
                snapshot["sourceState"] = "stale"
                snapshot["checkedAt"] = stamp_ist()
                snapshot["error"] = str(exc)
                return snapshot
            return {
                "ok": False,
                "rows": [],
                "summary": {"total": 0, "advances": 0, "declines": 0, "unchanged": 0},
                "checkedAt": stamp_ist(),
                "sourceState": "offline",
                "sourceUrl": ETF_URL,
                "error": str(exc),
            }


class Handler(BaseHTTPRequestHandler):
    server_version = "GandhiBrosCo/1.0"

    def send_bytes(self, code: int, content_type: str, payload: bytes):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        if path in ("/api/indices", "/api/indices/"):
            try:
                force = parse_qs(parsed.query).get("force", ["0"])[0] == "1"
                payload = get_snapshot(force=force)
                code = 200 if payload.get("ok") else 503
                body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                self.send_bytes(code, "application/json; charset=utf-8", body)
            except Exception as exc:
                body = json.dumps({"ok": False, "error": str(exc), "indices": []}).encode("utf-8")
                self.send_bytes(500, "application/json; charset=utf-8", body)
            return
        if path in ("/api/silver", "/api/silver/"):
            try:
                force = parse_qs(parsed.query).get("force", ["0"])[0] == "1"
                payload = get_silver_snapshot(force=force)
                code = 200 if payload.get("ok") else 503
                body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                self.send_bytes(code, "application/json; charset=utf-8", body)
            except Exception as exc:
                body = json.dumps({"ok": False, "benchmark": None, "error": str(exc)}, ensure_ascii=False).encode("utf-8")
                self.send_bytes(500, "application/json; charset=utf-8", body)
            return
        if path in ("/api/news", "/api/news/"):
            try:
                force = parse_qs(parsed.query).get("force", ["0"])[0] == "1"
                payload = get_news_snapshot(force=force)
                code = 200 if payload.get("ok") else 503
                body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                self.send_bytes(code, "application/json; charset=utf-8", body)
            except Exception as exc:
                body = json.dumps({"ok": False, "stories": [], "error": str(exc)}, ensure_ascii=False).encode("utf-8")
                self.send_bytes(500, "application/json; charset=utf-8", body)
            return
        if path in ("/api/benchmarks", "/api/benchmarks/"):
            try:
                force = parse_qs(parsed.query).get("force", ["0"])[0] == "1"
                payload = get_benchmark_snapshot(force=force)
                code = 200 if payload.get("ok") else 503
                body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                self.send_bytes(code, "application/json; charset=utf-8", body)
            except Exception as exc:
                body = json.dumps({"ok": False, "benchmarks": {}, "error": str(exc)}, ensure_ascii=False).encode("utf-8")
                self.send_bytes(500, "application/json; charset=utf-8", body)
            return
        if path in ("/api/etfs", "/api/etfs/"):
            try:
                force = parse_qs(parsed.query).get("force", ["0"])[0] == "1"
                payload = get_etf_snapshot(force=force)
                code = 200 if payload.get("ok") else 503
                body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                self.send_bytes(code, "application/json; charset=utf-8", body)
            except Exception as exc:
                body = json.dumps({"ok": False, "rows": [], "error": str(exc)}, ensure_ascii=False).encode("utf-8")
                self.send_bytes(500, "application/json; charset=utf-8", body)
            return
        if path in ("/api/ipos", "/api/ipos/"):
            try:
                force = parse_qs(parsed.query).get("force", ["0"])[0] == "1"
                payload = get_ipo_snapshot(force=force)
                code = 200 if payload.get("ok") else 503
                body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                self.send_bytes(code, "application/json; charset=utf-8", body)
            except Exception as exc:
                body = json.dumps({"ok": False, "rows": [], "error": str(exc)}, ensure_ascii=False).encode("utf-8")
                self.send_bytes(500, "application/json; charset=utf-8", body)
            return
        if path.rstrip("/") in ("", "/index.html", "/news", "/index", "/indices", "/etfs", "/ipos"):
            try:
                body = (ROOT / "index.html").read_bytes()
                self.send_bytes(200, "text/html; charset=utf-8", body)
            except OSError:
                self.send_bytes(500, "text/plain; charset=utf-8", b"index.html not found")
            return
        if path == "/favicon.ico":
            self.send_bytes(204, "image/x-icon", b"")
            return
        self.send_bytes(404, "text/plain; charset=utf-8", b"Not found")

    def log_message(self, fmt, *args):
        print(f"[{now_ist().strftime('%H:%M:%S IST')}] {fmt % args}", flush=True)


def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Gandhi & Bros Co. is serving on 0.0.0.0:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
