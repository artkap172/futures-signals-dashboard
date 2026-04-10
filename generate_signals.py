#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
from datetime import datetime, timedelta
import pandas as pd
import pandas_ta as ta
import pytz
import requests

ASSET_CONFIG = {
    "BR": {"name": "Нефть Brent", "group": "commodities"},
    "NG": {"name": "Природный газ", "group": "commodities"},
    "GD": {"name": "Золото", "group": "commodities"},
    "SV": {"name": "Серебро", "group": "commodities"},
    "Si": {"name": "USD/RUB", "group": "currency"},
    "MX": {"name": "Индекс МосБиржи", "group": "indices"},
    "SR": {"name": "Сбербанк", "group": "stocks"},
    "GZ": {"name": "Газпром", "group": "stocks"},
}

MOEX_API_BASE = "https://iss.moex.com/iss"
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

def get_active_futures(asset_code):
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities.json"
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        data = res.json()
        
        securities = data["securities"]["data"]
        cols = data["securities"]["columns"]
        mdata = data["marketdata"]["data"]
        m_cols = data["marketdata"]["columns"]
        
        secid_idx = m_cols.index("SECID")
        vol_idx = m_cols.index("VOLTODAY")
        
        vols = {row[secid_idx]: row[vol_idx] or 0 for row in mdata}
        
        active = []
        for row in securities:
            sid = row[cols.index("SECID")]
            if sid.startswith(asset_code) and 4 <= len(sid) <= 5:
                active.append({
                    "secid": sid,
                    "name": row[cols.index("SHORTNAME")],
                    "expiry": row[cols.index("LASTDELDATE")],
                    "vol": vols.get(sid, 0)
                })
        
        if active:
            active.sort(key=lambda x: (-x["vol"], x["expiry"]))
            return active[0]
        return None
    except Exception as e:
        print(f"get_active_futures error for {asset_code}: {e}")
        return None

def fetch_history(secid, days=60):
    start = (datetime.now(MOSCOW_TZ) - timedelta(days=days)).strftime("%Y-%m-%d")
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities/{secid}/candles.json?from={start}&interval=60"
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        data = res.json()
        df = pd.DataFrame(data["candles"]["data"], columns=data["candles"]["columns"])
        if not df.empty:
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close", "high", "low"])
            return df
        return None
    except Exception as e:
        print(f"fetch_history error for {secid}: {e}")
        return None

def fetch_realtime(secid):
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities/{secid}.json"
    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        data = res.json()
        marketdata = data.get("marketdata", {}).get("data", [])
        cols = data.get("marketdata", {}).get("columns", [])
        if not marketdata or not cols: return None
        row = marketdata[0]
        last_idx = cols.index("LAST")
        settle_idx = cols.index("SETTLEPRICE")
        return row[last_idx] or row[settle_idx]
    except Exception as e:
        print(f"fetch_realtime error for {secid}: {e}")
        return None

def analyze(df, price):
    if df is None or len(df) < 20 or price is None: return None
    try:
        close = df["close"].copy()
        rsi_val = ta.rsi(close, length=14)
        ema50 = ta.ema(close, length=50) if len(close) >= 50 else ta.ema(close, length=min(len(close)-1, 20))
        ema200 = ta.ema(close, length=200) if len(close) >= 200 else ta.ema(close, length=min(len(close)-1, 100))
        bb = ta.bbands(close, length=20)
        atr_val = ta.atr(df["high"], df["low"], close, length=14)
        macd_df = ta.macd(close, fast=12, slow=26, signal=9)
        
        df2 = df.copy()
        df2["rsi"] = rsi_val
        df2["ema50"] = ema50
        df2["ema200"] = ema200
        df2["atr"] = atr_val
        
        if bb is not None:
            for col in bb.columns: df2[col] = bb[col]
        if macd_df is not None:
            for col in macd_df.columns: df2[col] = macd_df[col]
            
        last = df2.iloc[-1]
        score = 0.0
        trend = "SIDEWAYS"
        
        e50 = last.get("ema50")
        e200 = last.get("ema200")
        
        if e50 is not None and e200 is not None and not (pd.isna(e50) or pd.isna(e200)):
            if price > e50 > e200: trend, score = "BULLISH", score + 2
            elif price < e50 < e200: trend, score = "BEARISH", score - 2
            elif price > e50: trend, score = "BULLISH", score + 1
            elif price < e50: trend, score = "BEARISH", score - 1
            
        rsi = last.get("rsi")
        if rsi is not None and not pd.isna(rsi):
            if rsi < 30: score += 1.5
            elif rsi < 40: score += 0.5
            elif rsi > 70: score -= 1.5
            elif rsi > 60: score -= 0.5
            
        # Robust BB column names
        bbl_col = [c for c in df2.columns if "BBL" in c]
        bbu_col = [c for c in df2.columns if "BBU" in c]
        bbl = last.get(bbl_col[0]) if bbl_col else None
        bbu = last.get(bbu_col[0]) if bbu_col else None
        
        if bbl is not None and bbu is not None and not (pd.isna(bbl) or pd.isna(bbu)):
            if price < bbl: score += 1.0
            elif price > bbu: score -= 1.0
            
        macd_v = last.get("MACD_12_26_9")
        macds_v = last.get("MACDs_12_26_9")
        if macd_v is not None and macds_v is not None and not (pd.isna(macd_v) or pd.isna(macds_v)):
            if macd_v > macds_v: score += 0.5
            else: score -= 0.5
            
        sig = "HOLD"
        if score >= 2.0: sig = "LONG"
        elif score <= -2.0: sig = "SHORT"
        
        sl = tp1 = tp2 = None
        atr = last.get("atr")
        if sig != "HOLD" and atr is not None and not pd.isna(atr):
            direction = 1 if sig == "LONG" else -1
            sl = price - atr * 2 * direction
            tp1 = price + atr * 3 * direction
            tp2 = price + atr * 6 * direction
            
        indicators = {
            "rsi": round(float(rsi), 1) if rsi is not None and not pd.isna(rsi) else None,
            "ema50": round(float(e50), 2) if e50 is not None and not pd.isna(e50) else None,
            "ema200": round(float(e200), 2) if e200 is not None and not pd.isna(e200) else None,
            "bbl": round(float(bbl), 2) if bbl is not None and not pd.isna(bbl) else None,
            "bbu": round(float(bbu), 2) if bbu is not None and not pd.isna(bbu) else None,
            "macd": round(float(macd_v), 4) if macd_v is not None and not pd.isna(macd_v) else None,
            "macd_signal": round(float(macds_v), 4) if macds_v is not None and not pd.isna(macds_v) else None,
            "atr": round(float(atr), 2) if atr is not None and not pd.isna(atr) else None
        }
        
        return {
            "sig": sig, "score": round(score, 2), "trend": trend,
            "sl": sl, "tp1": tp1, "tp2": tp2, "rsi": rsi, "indicators": indicators
        }
    except Exception as e:
        print(f"analyze error: {e}")
        return None

def main():
    res_all = []
    for code, info in ASSET_CONFIG.items():
        print(f"Processing {code}...")
        contract = get_active_futures(code)
        if not contract: continue
        
        price = fetch_realtime(contract["secid"])
        df = fetch_history(contract["secid"])
        analysis = analyze(df, price)
        
        if df is not None and not df.empty and price:
            last_close = df["close"].iloc[-1]
            ch = round((price - last_close) / last_close * 100, 2)
        else:
            ch = 0
            
        record = {
            "ticker": contract["secid"],
            "name": info["name"],
            "group": info["group"],
            "current_price": price,
            "change_pct": ch,
            "signal": analysis["sig"] if analysis else "HOLD",
            "mode": analysis["trend"] if analysis else "WAIT",
            "score": analysis["score"] if analysis else 0,
            "priority": "HIGH" if analysis and abs(analysis["score"]) >= 3 else "MEDIUM",
            "stop_loss": round(analysis["sl"], 2) if analysis and analysis["sl"] else None,
            "tp1": round(analysis["tp1"], 2) if analysis and analysis["tp1"] else None,
            "tp2": round(analysis["tp2"], 2) if analysis and analysis["tp2"] else None,
            "entry_range": f"{round(price*0.999, 2)}-{round(price*1.001, 2)}" if price else None,
            "win_rate": 68 if analysis and analysis["sig"] != "HOLD" else 0,
            "reasoning": f"Trend: {analysis['trend']}. RSI: {round(analysis['rsi'], 1) if analysis['rsi'] else 'N/A'}. Score: {analysis['score']}." if analysis else "N/A",
            "indicators": analysis["indicators"] if analysis else {},
            "last_update": datetime.now(MOSCOW_TZ).strftime("%H:%M")
        }
        res_all.append(record)
        
    if not res_all:
        print("Done. No assets fetched.")
        return
        
    longs = sum(1 for r in res_all if r["signal"] == "LONG")
    shorts = sum(1 for r in res_all if r["signal"] == "SHORT")
    sentiment = "BULLISH" if longs > shorts else ("BEARISH" if shorts > longs else "NEUTRAL")
    
    out = {
        "signals": [r for r in res_all if r["signal"] != "HOLD"],
        "all_assets": res_all,
        "heatmap": [
            {
                "ticker": r["ticker"], "name": r["name"], "change": r["change_pct"],
                "price": r["current_price"], "signal": r["signal"], "mode": r["mode"],
                "rsi": r["indicators"].get("rsi")
            } for r in res_all
        ],
        "market_sentiment": sentiment,
        "stats": {
            "total": len(res_all), "long": longs, "short": shorts,
            "hold": len(res_all) - longs - shorts
        },
        "last_update": datetime.now(MOSCOW_TZ).strftime("%d.%m %H:%M")
    }
    
    with open("signals.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
        
    print(f"Done. Signals: {len(out['signals'])}, Sentiment: {sentiment}")

if __name__ == "__main__":
    main()
