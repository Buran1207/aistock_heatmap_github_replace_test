#!/usr/bin/env python3
"""
AI 股票热力图行情抓取脚本
读取 universe.json，按 市场:代码 去重，输出 prices.json。

要点：
- H 股在 universe.json / 页面统一五位展示；抓 Yahoo 时自动转成四位 .HK，例如 00100 -> 0100.HK、02513 -> 2513.HK。
- quote API 缺涨跌幅时，用 previousClose 或 chart 最近两个交易日收盘价兜底计算。
- 单次失败时保留上一轮有效报价并标记 stale，避免页面大面积变灰。
"""
from __future__ import annotations

import concurrent.futures as cf
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

try:
    import yfinance as yf
except Exception:
    yf = None

ROOT = Path(__file__).resolve().parent
UNIVERSE_FILE = ROOT / "universe.json"
PRICES_FILE = ROOT / "prices.json"
MARKET_CCY = {"US": "USD", "H": "HKD", "A": "CNY"}
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/125 Safari/537.36"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA, "Accept": "application/json,text/plain,*/*"})


def now_local_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalize_market(market: str) -> str:
    m = str(market).upper().strip()
    return "H" if m in {"HK", "HKG"} else m


def normalize_ticker(ticker: str, market: str) -> str:
    m = normalize_market(market)
    t = str(ticker).strip()
    if m == "US":
        return t.upper()
    if m == "H":
        return t.zfill(5)
    if m == "A":
        return t.zfill(6)
    return t


def hk_to_yahoo(ticker: str) -> str:
    n = int(str(ticker).lstrip("0") or "0")
    return f"{n:04d}.HK"


def to_yahoo_symbol(ticker: str, market: str) -> str:
    m = normalize_market(market)
    t = normalize_ticker(ticker, m)
    if m == "US":
        return t
    if m == "H":
        return hk_to_yahoo(t)
    if t.startswith(("6", "9")):
        return f"{t}.SS"
    if t.startswith(("8", "4")):
        return f"{t}.BJ"
    return f"{t}.SZ"


def from_yahoo_symbol(symbol: str) -> tuple[str, str]:
    s = str(symbol).upper()
    if s.endswith(".HK"):
        return "H", s[:-3].zfill(5)
    if s.endswith((".SS", ".SZ", ".BJ")):
        return "A", s[:-3].zfill(6)
    return "US", s


def stock_key(stock: dict[str, Any]) -> str:
    m = normalize_market(stock["m"])
    t = normalize_ticker(stock["t"], m)
    return f"{m}:{t}"


def load_universe() -> list[dict[str, Any]]:
    u = json.loads(UNIVERSE_FILE.read_text(encoding="utf-8"))
    layers = u.get("layers", u)
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
    return None if math.isnan(f) or math.isinf(f) else f


def price_payload(price: Any, change: Any, currency: str, *, symbol: str, source: str, regular_time: Any = None, stale: bool = False) -> dict[str, Any] | None:
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
    return [seq[i:i+size] for i in range(0, len(seq), size)]


def fetch_quote_batch(stocks: list[dict[str, Any]], batch_size: int = 65) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for batch in chunks(stocks, batch_size):
        symbols = ",".join(s["yf"] for s in batch)
        params = {
            "symbols": symbols,
            "fields": "regularMarketPrice,regularMarketChangePercent,regularMarketPreviousClose,currency,regularMarketTime",
            "lang": "en-US",
            "region": "US",
        }
        try:
            r = SESSION.get("https://query1.finance.yahoo.com/v7/finance/quote", params=params, timeout=12)
            r.raise_for_status()
            rows = r.json().get("quoteResponse", {}).get("result", [])
        except Exception as exc:
            print(f"quote batch failed ({len(batch)}): {exc}", file=sys.stderr)
            rows = []
        for q in rows:
            symbol = q.get("symbol")
            if not symbol:
                continue
            m, t = from_yahoo_symbol(symbol)
            key = f"{m}:{t}"
            price = clean_float(q.get("regularMarketPrice"))
            change = clean_float(q.get("regularMarketChangePercent"))
            prev = clean_float(q.get("regularMarketPreviousClose"))
            if change is None and price is not None and prev:
                change = (price - prev) / prev * 100
            payload = price_payload(price, change, q.get("currency") or MARKET_CCY.get(m, ""), symbol=symbol, source="yahoo_quote", regular_time=q.get("regularMarketTime"))
            if payload:
                out[key] = payload
        time.sleep(0.15)
    return out


def chart_prev_change(symbol: str, price: float | None = None) -> dict[str, Any] | None:
    try:
        r = SESSION.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}", params={"range": "1mo", "interval": "1d"}, timeout=12)
        r.raise_for_status()
        res = r.json().get("chart", {}).get("result", [None])[0]
        if not res:
            return None
        meta = res.get("meta", {})
        q = res.get("indicators", {}).get("quote", [{}])[0]
        closes = [clean_float(x) for x in q.get("close", [])]
        closes = [x for x in closes if x is not None]
        p = clean_float(meta.get("regularMarketPrice")) or price or (closes[-1] if closes else None)
        prev = clean_float(meta.get("chartPreviousClose"))
        if (prev is None or prev == 0) and len(closes) >= 2:
            prev = closes[-2] if abs((p or 0) - closes[-1]) < 1e-8 else closes[-1]
        change = (p - prev) / prev * 100 if p is not None and prev else None
        return price_payload(p, change, meta.get("currency") or "", symbol=symbol, source="yahoo_chart", regular_time=meta.get("regularMarketTime"))
    except Exception:
        return None


def fetch_one_fallback(stock: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    key = stock["key"]
    symbol = stock["yf"]
    payload = chart_prev_change(symbol)
    if payload and payload.get("price") is not None and payload.get("change") is not None:
        return key, payload
    if yf is not None:
        try:
            tk = yf.Ticker(symbol)
            hist = tk.history(period="1mo", interval="1d", auto_adjust=False)
            if hist is not None and len(hist) > 0:
                closes = [clean_float(x) for x in hist["Close"].tolist()]
                closes = [x for x in closes if x is not None]
                if closes:
                    p = closes[-1]
                    prev = closes[-2] if len(closes) >= 2 else None
                    change = (p - prev) / prev * 100 if prev else None
                    return key, price_payload(p, change, MARKET_CCY.get(stock["m"], ""), symbol=symbol, source="yfinance_history")
        except Exception:
            pass
    return key, payload


def main() -> int:
    stocks = load_universe()
    previous = load_previous_prices()
    prices = fetch_quote_batch(stocks)

    by_key = {s["key"]: s for s in stocks}
    need = [s for s in stocks if s["key"] not in prices or prices[s["key"]].get("price") is None or prices[s["key"]].get("change") is None]
    if need:
        with cf.ThreadPoolExecutor(max_workers=12) as ex:
            for key, payload in ex.map(fetch_one_fallback, need):
                if payload:
                    old = prices.get(key, {})
                    # merge fallback fields, especially missing change
                    merged = {**old, **{k: v for k, v in payload.items() if v is not None}}
                    prices[key] = merged

    failures: list[str] = []
    partial: list[str] = []
    final: dict[str, Any] = {}
    for s in stocks:
        key = s["key"]
        p = prices.get(key)
        if not p or p.get("price") is None:
            old = previous.get(key)
            if old and old.get("price") is not None:
                final[key] = {**old, "stale": True, "source": str(old.get("source", "previous")) + "+stale", "updated_at": now_utc_iso()}
            else:
                failures.append(key)
                final[key] = {"price": None, "change": None, "date": None, "currency": MARKET_CCY.get(s["m"], ""), "symbol": s["yf"], "source": None, "stale": False}
        else:
            if p.get("change") is None:
                partial.append(key)
            final[key] = {**p, "currency": p.get("currency") or MARKET_CCY.get(s["m"], ""), "symbol": p.get("symbol") or s["yf"]}

    payload = {
        "generated_at": now_local_str(),
        "generated_at_utc": now_utc_iso(),
        "source": "Yahoo Finance quote/chart + yfinance fallback",
        "note": "盘中或最近交易日行情；H股页面五位代码，Yahoo抓取自动转四位.HK。",
        "total_symbols": len(stocks),
        "loaded_symbols": sum(1 for v in final.values() if v.get("price") is not None and v.get("change") is not None),
        "missing_symbols": len(failures),
        "price_only_symbols": len(partial),
        "failures": failures,
        "partial_symbols": partial,
        "prices": dict(sorted(final.items())),
    }
    PRICES_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {PRICES_FILE}: total={len(stocks)} loaded={payload['loaded_symbols']} missing={len(failures)} partial={len(partial)}")
    if failures:
        print("failures:", ", ".join(failures[:30]), file=sys.stderr)
    if partial:
        print("partial:", ", ".join(partial[:30]), file=sys.stderr)
    return 0 if not failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
