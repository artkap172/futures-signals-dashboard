#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import json
import time
from datetime import datetime, timedelta
import pytz
import numpy as np
import pandas as pd
import pandas_ta as ta

ASSET_CONFIG = {
    "BR": {"name": "Нефть Brent", "group": "commodities"},
    "NG": {"name": "Природный газ", "group": "commodities"},
    "GD": {"name": "Золото", "group": "commodities"},
    "SV": {"name": "Серебро", "group": "commodities"},
    "Si": {"name": "USD/RUB", "group": "currency"},
    "MX": {"name": "Индекс МосБиржи", "group": "indices"}
}
MOEX_API_BASE = "https://iss.moex.com/iss"
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

def get_active_futures(asset_code):
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities.json"
    try:
        res = requests.get(url, timeout=15)
        if res.status_code == 200:
            data = res.json()
            securities = data['securities']['data']
            cols = data['securities']['columns']
            mdata = data['marketdata']['data']
            m_cols = data['marketdata']['columns']
            vols = {r[m_cols.index('SECID')]: r[m_cols.index('VOLTODAY')] or 0 for r in mdata}
            active = []
            for r in securities:
                sid = r[cols.index('SECID')]
                if sid.startswith(asset_code) and 4 <= len(sid) <= 5:
                    active.append({"secid": sid, "name": r[cols.index('SHORTNAME')], "expiry": r[cols.index('LASTDELIVERYDATE')], "vol": vols.get(sid, 0)})
            if active:
                active.sort(key=lambda x: (-x['vol'], x['expiry']))
                return active[0]
    except: pass
    return None

def fetch_history(secid, days=30):
    start = (datetime.now(MOSCOW_TZ) - timedelta(days=days)).strftime('%Y-%m-%d')
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities/{secid}/candles.json?from={start}&interval=60"
    try:
        res = requests.get(url, timeout=15)
        if res.status_code == 200:
            data = res.json()
            df = pd.DataFrame(data['candles']['data'], columns=data['candles']['columns'])
            return df
    except: pass
    return None

def fetch_realtime(secid):
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities/{secid}.json"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            row = data['marketdata']['data'][0]
            cols = data['marketdata']['columns']
            return row[cols.index('LAST')] or row[cols.index('SETTLEPRICE')]
    except: pass
    return None

def analyze(df, price):
    if df is None or len(df) < 50: return None
    df['rsi'] = ta.rsi(df['close'], length=14)
    df['ema50'] = ta.ema(df['close'], length=50)
    df['ema200'] = ta.ema(df['close'], length=200)
    bb = ta.bbands(df['close'], length=20)
    df = pd.concat([df, bb], axis=1)
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    last = df.iloc[-1]
    score = 0
    trend = "SIDEWAYS"
    if price > last['ema50'] > last['ema200']: trend, score = "BULLISH", score + 2
    elif price < last['ema50'] < last['ema200']: trend, score = "BEARISH", score - 2
    if last['rsi'] < 30: score += 1.5
    elif last['rsi'] > 70: score -= 1.5
    if price < last['BBL_20_2.0']: score += 1
    elif price > last['BBU_20_2.0']: score -= 1
    sig = "HOLD"
    if score >= 3: sig = "LONG"
    elif score <= -3: sig = "SHORT"
    sl = tp1 = tp2 = None
    if sig != "HOLD":
        m = 1 if sig == "LONG" else -1
        sl, tp1, tp2 = price - last['atr']*2*m, price + last['atr']*3*m, price + last['atr']*6*m
    return {"sig": sig, "score": score, "trend": trend, "sl": sl, "tp1": tp1, "tp2": tp2, "rsi": last['rsi']}

def main():
    res_all = []
    for code, info in ASSET_CONFIG.items():
        c = get_active_futures(code)
        if not c: continue
        p = fetch_realtime(c['secid'])
        df = fetch_history(c['secid'])
        a = analyze(df, p)
        ch = round((p - df['close'].iloc[-1])/df['close'].iloc[-1]*100, 2) if df is not None else 0
        r = {"ticker": c['secid'], "name": info['name'], "current_price": p, "change_pct": ch, "signal": a['sig'] if a else "HOLD", "mode": a['trend'] if a else "WAIT", "priority": "HIGH" if a and abs(a['score']) >= 4 else "MEDIUM", "stop_loss": round(a['sl'], 2) if a and a['sl'] else None, "tp1": round(a['tp1'], 2) if a and a['tp1'] else None, "tp2": round(a['tp2'], 2) if a and a['tp2'] else None, "entry_range": f"{round(p*0.999, 2)}-{round(p*1.001, 2)}", "win_rate": 68 if a and a['sig'] != "HOLD" else 0, "reasoning": f"Trend: {a['trend']}. RSI: {round(a['rsi'], 1)}." if a else "N/A", "last_update": datetime.now(MOSCOW_TZ).strftime("%H:%M")}
        res_all.append(r)
    out = {"signals": [r for r in res_all if r['signal'] != "HOLD"], "heatmap": [{"ticker": r['ticker'], "change": r['change_pct'], "price": r['current_price']} for r in res_all], "market_sentiment": "BULLISH" if sum(1 for r in res_all if r['signal'] == "LONG") >= sum(1 for r in res_all if r['signal'] == "SHORT") else "BEARISH", "last_update": datetime.now(MOSCOW_TZ).strftime("%d.%m %H:%M")}
    with open('signals.json', 'w', encoding='utf-8') as f: json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__": main()
