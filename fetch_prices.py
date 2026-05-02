#!/usr/bin/env python3
"""
AI 股票热力图行情抓取脚本
────────────────────────────────────────────────────────
读取: universe.json
输出: prices.json

设计目标：
1) 股票池只维护 universe.json；同一股票可出现在多个赛道，但行情抓取按 市场:代码 自动去重。
2) 优先使用 Yahoo Finance v7 quote API；失败时回退 chart API，再回退 yfinance。
3) 如果单次抓取失败，保留上一轮 prices.json 的有效报价并标记 stale，避免页面大片变灰。
"""

from __future__ import annotations

import concurrent.futures as cf
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

try:
    import yfinance as yf
except Exception:  # yfinance is an optional fallback
    yf = None


ROOT = Path(__file__).resolve().parent
UNIVERSE_FILE = ROOT / "universe.json"
PRICES_FILE = ROOT / "prices.json"

MARKET_CCY = {"US": "USD", "H": "HKD", "A": "CNY"}
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Accept": "application/json,text/plain,*/*"})


def now_local_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_market(market: str) -> str:
    market = market.upper()
    if market in {"HK", "HKG"}:
        return "H"
    return market


def normalize_ticker(ticker: str, market: str) -> str:
    ticker = str(ticker).strip().upper() if market == "US" else str(ticker).strip()
    if market == "H":

        digits = "".join(ch for ch in ticker if ch.isdigit()) or ticker
        if len(digits) > 4 and digits.startswith("0"):
            digits = digits.lstrip("0") or "0"
        return digits.zfill(4) if digits.isdigit() and len(digits) < 4 else digits
    if market == "A":
        return ticker.zfill(6)
    return ticker


def to_yahoo_symbol(ticker: str, market: str) -> str:
    market = normalize_market(market)
    ticker = normalize_ticker(ticker, market)
    if market == "US":
        return ticker
    if market == "H":
        return f"{ticker}.HK"
    # A 股 Yahoo 后缀：
    # 6/9 开头一般是上交所/科创板 .SS；0/2/3 开头一般是深交所/创业板 .SZ；8/4 开头是北交所 .BJ。
    if ticker.startswith(("6", "9")):
        return f"{ticker}.SS"
    if ticker.startswith(("8", "4")):
        return f"{ticker}.BJ"
    return f"{ticker}.SZ"


def from_yahoo_symbol(symbol: str) -> tuple[str, str]:
    symbol = symbol.upper()
    if symbol.endswith(".HK"):
        return "H", symbol[:-3].zfill(4)
    if symbol.endswith((".SS", ".SZ", ".BJ")):
        return "A", symbol[:-3].zfill(6)
    return "US", symbol


def stock_key(stock: dict[str, Any]) -> str:
    m = normalize_market(stock["m"])
    t = normalize_ticker(stock["t"], m)
    return f"{m}:{t}"


def load_universe() -> list[dict[str, Any]]:
    if not UNIVERSE_FILE.exists():
        raise FileNotFoundError("universe.json not found")
    universe = json.loads(UNIVERSE_FILE.read_text(encoding="utf-8"))
    layers = universe.get("layers", universe)

    uniq: dict[str, dict[str, Any]] = {}
    for layer in layers:
        for cat in layer.get("cat", []):
            for s in cat.get("s", []):
                m = normalize_market(s["m"])
                t = normalize_ticker(s["t"], m)
                k = f"{m}:{t}"
                if k not in uniq:
                    uniq[k] = {"t": t, "n": s.get("n", t), "m": m, "key": k, "yf": to_yahoo_symbol(t, m)}
    return list(uniq.values())


def load_previous_prices() -> dict[str, Any]:
    if not PRICES_FILE.exists():
        return {}
    try:
        return json.loads(PRICES_FILE.read_text(encoding="utf-8")).get("prices", {})
    except Exception:
        return {}


def clean_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def price_payload(
    price: float | None,
    change: float | None,
    currency: str,
    *,
    symbol: str,
    source: str,
    regular_time: int | None = None,
    stale: bool = False,
) -> dict[str, Any] | None:
    p = clean_float(price)
    c = clean_float(change)
    if p is None:
        return None
    date = None
    if regular_time:
        try:
            date = datetime.fromtimestamp(int(regular_time), tz=timezone.utc).date().isoformat()
        except Exception:
            date = None
    return {
        "price": round(p, 2),
        "change": round(c, 2) if c is not None else None,
        "date": date,
        "currency": currency,
        "symbol": symbol,
        "source": source,
        "stale": stale,
        "updated_at": now_utc_iso(),
    }


def chunks(seq: list[Any], size: int) -> list[list[Any]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def fetch_quote_batch(stocks: list[dict[str, Any]], batch_size: int = 70) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for batch in chunks(stocks, batch_size):
        symbols = ",".join(s["yf"] for s in batch)
        url = "https://query1.finance.yahoo.com/v7/finance/quote"
        params = {
            "symbols": symbols,
            "fields": "regularMarketPrice,regularMarketChangePercent,regularMarketChange,regularMarketPreviousClose,currency,regularMarketTime",
            "lang": "en-US",
            "region": "US",
        }
        try:
            r = SESSION.get(url, params=params, timeout=12)
            r.raise_for_status()
            data = r.json().get("quoteResponse", {}).get("result", [])
        except Exception as exc:
            print(f"quote batch failed ({len(batch)}): {exc}", file=sys.stderr)
            data = []

        for q in data:
            symbol = q.get("symbol")
            if not symbol:
                continue
            m, t = from_yahoo_symbol(symbol)
            key = f"{m}:{t}"
            currency = q.get("currency") or MARKET_CCY.get(m, "")
            price = clean_float(q.get("regularMarketPrice"))
            change = clean_float(q.get("regularMarketChangePercent"))
            if change is None:
                prev = clean_float(q.get("regularMarketPreviousClose"))
                if price is not None and prev:
                    change = (price - prev) / prev * 100
            payload = price_payload(
                price,
                change,
                currency,
                symbol=symbol,
                source="yahoo_quote",
                regular_time=q.get("regularMarketTime"),
            )
            if payload:
                out[key] = payload
        time.sleep(0.15)
    return out


def fetch_chart_one(stock: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    symbol = stock["yf"]
    key = stock["key"]
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "5d", "interval": "1d", "includePrePost": "false"}
    try:
        r = SESSION.get(url, params=params, timeout=10)
        r.raise_for_status()
        result = r.json().get("chart", {}).get("result", [])
        if not result:
            return key, None
        node = result[0]
        meta = node.get("meta", {})
        currency = meta.get("currency") or MARKET_CCY.get(stock["m"], "")
        price = clean_float(meta.get("regularMarketPrice"))
        prev = clean_float(meta.get("chartPreviousClose") or meta.get("previousClose"))
        change = (price - prev) / prev * 100 if price is not None and prev else None

        # 如果 meta 没有实时价格，用最近两个收盘价兜底。
        quote = (node.get("indicators", {}).get("quote") or [{}])[0]
        closes = [clean_float(x) for x in quote.get("close", [])]
        closes = [x for x in closes if x is not None]
        timestamps = node.get("timestamp") or []
        if price is None and closes:
            price = closes[-1]
        if change is None and len(closes) >= 2 and closes[-2]:
            change = (closes[-1] - closes[-2]) / closes[-2] * 100
        regular_time = meta.get("regularMarketTime")
        if not regular_time and timestamps:
            regular_time = timestamps[-1]

        return key, price_payload(price, change, currency, symbol=symbol, source="yahoo_chart", regular_time=regular_time)
    except Exception:
        return key, None


def fetch_chart_missing(stocks: list[dict[str, Any]], existing: dict[str, Any]) -> dict[str, dict[str, Any]]:
    missing = [s for s in stocks if s["key"] not in existing]
    out: dict[str, dict[str, Any]] = {}
    if not missing:
        return out
    with cf.ThreadPoolExecutor(max_workers=10) as ex:
        for key, payload in ex.map(fetch_chart_one, missing):
            if payload:
                out[key] = payload
    return out


def fetch_yfinance_missing(stocks: list[dict[str, Any]], existing: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if yf is None:
        return {}
    missing = [s for s in stocks if s["key"] not in existing]
    out: dict[str, dict[str, Any]] = {}
    for stock in missing:
        try:
            hist = yf.Ticker(stock["yf"]).history(period="5d", auto_adjust=True)
            if hist is None or hist.empty or "Close" not in hist:
                continue
            close = [clean_float(x) for x in hist["Close"].dropna().tolist()]
            close = [x for x in close if x is not None]
            if not close:
                continue
            price = close[-1]
            change = (close[-1] - close[-2]) / close[-2] * 100 if len(close) >= 2 and close[-2] else None
            dt = hist.index[-1]
            payload = price_payload(
                price,
                change,
                MARKET_CCY.get(stock["m"], ""),
                symbol=stock["yf"],
                source="yfinance_history",
            )
            if payload:
                try:
                    payload["date"] = dt.date().isoformat()
                except Exception:
                    pass
                out[stock["key"]] = payload
        except Exception:
            pass
        time.sleep(0.05)
    return out


def preserve_previous(
    stocks: list[dict[str, Any]],
    prices: dict[str, dict[str, Any]],
    previous: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    failures: list[str] = []
    for stock in stocks:
        key = stock["key"]
        if key in prices and prices[key].get("price") is not None:
            continue
        old = previous.get(key)
        if old and old.get("price") is not None:
            carry = dict(old)
            carry["stale"] = True
            carry["source"] = f"stale_{carry.get('source', 'previous')}"
            prices[key] = carry
        else:
            prices[key] = {
                "price": None,
                "change": None,
                "date": None,
                "currency": MARKET_CCY.get(stock["m"], ""),
                "symbol": stock["yf"],
                "source": "missing",
                "stale": False,
                "updated_at": now_utc_iso(),
            }
            failures.append(key)
    return prices, failures


def main() -> int:
    t0 = time.time()
    stocks = load_universe()
    previous = load_previous_prices()
    print(f"universe: {len(stocks)} unique symbols")

    prices = fetch_quote_batch(stocks)
    print(f"quote api: {len(prices)} loaded")

    chart_prices = fetch_chart_missing(stocks, prices)
    prices.update(chart_prices)
    print(f"chart fallback: +{len(chart_prices)} loaded")

    yf_prices = fetch_yfinance_missing(stocks, prices)
    prices.update(yf_prices)
    print(f"yfinance fallback: +{len(yf_prices)} loaded")

    prices, failures = preserve_previous(stocks, prices, previous)

    ok = sum(1 for v in prices.values() if v.get("price") is not None and v.get("change") is not None)
    stale = sum(1 for v in prices.values() if v.get("stale"))
    by_market: dict[str, dict[str, int]] = {}
    for stock in stocks:
        m = stock["m"]
        by_market.setdefault(m, {"total": 0, "ok": 0, "stale": 0, "missing": 0})
        by_market[m]["total"] += 1
        v = prices.get(stock["key"], {})
        if v.get("price") is not None and v.get("change") is not None:
            by_market[m]["ok"] += 1
        if v.get("stale"):
            by_market[m]["stale"] += 1
        if v.get("price") is None:
            by_market[m]["missing"] += 1

    output = {
        "generated_at": now_local_str(),
        "generated_at_utc": now_utc_iso(),
        "source": "Yahoo Finance quote/chart API with yfinance fallback",
        "note": "前端每 30 秒轮询本文件；GitHub Actions 定时生成。stale=true 表示使用上一轮有效行情。",
        "total_symbols": len(stocks),
        "loaded_symbols": ok,
        "stale_symbols": stale,
        "missing_symbols": len(failures),
        "by_market": by_market,
        "failures": failures[:80],
        "prices": dict(sorted(prices.items())),
    }

    tmp = PRICES_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(PRICES_FILE)

    elapsed = time.time() - t0
    print(f"done: {ok}/{len(stocks)} loaded, stale={stale}, missing={len(failures)}, elapsed={elapsed:.1f}s")
    return 0 if ok > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
