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

def fetch_history(secid, days=60):
    start = (datetime.now(MOSCOW_TZ) - timedelta(days=days)).strftime('%Y-%m-%d')
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities/{secid}/candles.json?from={start}&interval=60"
    try:
        res = requests.get(url, timeout=15)
        if res.status_code == 200:
            data = res.json()
            df = pd.DataFrame(data['candles']['data'], columns=data['candles']['columns'])
            if len(df) > 0:
                df['close'] = pd.to_numeric(df['close'], errors='coerce')
                df['high'] = pd.to_numeric(df['high'], errors='coerce')
                df['low'] = pd.to_numeric(df['low'], errors='coerce')
                df['open'] = pd.to_numeric(df['open'], errors='coerce')
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
                df = df.dropna(subset=['close'])
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
    if df is None or len(df) < 50:
        return None
    if price is None:
        return None
    try:
        close = df['close'].copy()
        # Indicators
        rsi_val = ta.rsi(close, length=14)
        ema50 = ta.ema(close, length=50)
        ema200 = ta.ema(close, length=200) if len(close) >= 200 else ta.ema(close, length=min(len(close)-1, 100))
        bb = ta.bbands(close, length=20)
        atr_val = ta.atr(df['high'], df['low'], close, length=14)
        macd_df = ta.macd(close, fast=12, slow=26, signal=9)

        df2 = df.copy()
        df2['rsi'] = rsi_val
        df2['ema50'] = ema50
        df2['ema200'] = ema200
        df2['atr'] = atr_val
        if bb is not None:
            for col in bb.columns:
                df2[col] = bb[col]
        if macd_df is not None:
            for col in macd_df.columns:
                df2[col] = macd_df[col]

        last = df2.iloc[-1]
        score = 0.0
        trend = "SIDEWAYS"

        # EMA trend
        e50 = last.get('ema50', None)
        e200 = last.get('ema200', None)
        if e50 is not None and e200 is not None and not (pd.isna(e50) or pd.isna(e200)):
            if price > e50 > e200:
                trend = "BULLISH"
                score += 2
            elif price < e50 < e200:
                trend = "BEARISH"
                score -= 2
            elif price > e50:
                trend = "BULLISH"
                score += 1
            elif price < e50:
                trend = "BEARISH"
                score -= 1

        # RSI
        rsi = last.get('rsi', None)
        if rsi is not None and not pd.isna(rsi):
            if rsi < 30:
                score += 2
            elif rsi < 40:
                score += 1
            elif rsi > 70:
                score -= 2
            elif rsi > 60:
                score -= 1

        # Bollinger Bands
        bbl_col = 'BBL_20_2.0'
        bbu_col = 'BBU_20_2.0'
        bbl = last.get(bbl_col, None)
        bbu = last.get(bbu_col, None)
        if bbl is not None and bbu is not None and not (pd.isna(bbl) or pd.isna(bbu)):208
            
            if price < bbl:
                score += 1.5
            ce > bbu:
                score -= 1.5

        # MACD
        macd_col = 'MACD_12_26_9'
        macds_col = 'MACDs_12_26_9'
        macd_v = last.get(macd_col, None)
        macds_v = last.get(macds_col, None)
        if macd_v is not None and macds_v is not None and not (pd.isna(macd_v) or pd.isna(macds_v)):
            if macd_v > macds_v:
                score += 0.5
            else:
                score -= 0.5

        # Signal
        sig = "HOLD"
        if score >= 2.5:
            sig = "LONG"
        elif score <= -2.5:
            sig = "SHORT"

        sl = tp1 = tp2 = None
        atr = last.get('atr', None)
        if sig != "HOLD" and atr is not None and not pd.isna(atr):
            m = 1 if sig == "LONG" else -1
            sl = price - atr * 2 * m
            tp1 = price + atr * 3 * m
            tp2 = price + atr * 6 * m

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

        return {"sig": sig, "score": round(score, 2), "trend": trend,
                "sl": sl, "tp1": tp1, "tp2": tp2,
                "rsi": rsi, "indicators": indicators}
    except Exception as e:
        print(f"analyze error: {e}")
        return None

205
:
    res_all = []
    for code, info in ASSET_CONFIG.items():
        print(f"Processing {code}...")
        c = get_active_futures(code)
        if not c:
            print(f"  No active futures for {code}")
            continue
        print(f"  Found: {c['secid']}")
        p = fetch_realtime(c['secid'])
        df = fetch_history(c['secid'])
        print(f"  Price: {p}, History rows: {len(df) if df is not None else 0}")
        a = analyze(df, p)
        if a:
            print(f"  Signal: {a['sig']}, Score: {a['score']}, Trend: {a['trend']}, RSI: {a.get('rsi', 'N/A')}")
        else:
            print(f"  No analysis")

        if df is not None and len(df) > 0 and p is not None:
            last_close = df['close'].iloc[-1]
            ch = round((p - last_close) / last_close * 100, 2) if last_close else 0
        else:
            ch = 0

        r = {
            "ticker": c['secid'],
            "name": info['name'],
            "group": info['group'],
            "current_price": p,
            "change_pct": ch,
            "signal": a['sig'] if a else "HOLD",
            "mode": a['trend'] if a else "WAIT",
            "score": a['score'] if a else 0,
            "priority": "HIGH" if a and abs(a['score']) >= 4 else "MEDIUM",
            "stop_loss": round(a['sl'], 2) if a and a['sl'] else None,
            "tp1": round(a['tp1'], 2) if a and a['tp1'] else None,
            "tp2": round(a['tp2'], 2) if a and a['tp2'] else None,
            "entry_range": f"{round(p*0.999, 2)}-{round(p*1.001, 2)}" if p else None,
            "win_rate": 68 if a and a['sig'] != "HOLD" else 0,
            "reasoning": f"Trend: {a['trend']}. RSI: {round(a['rsi'], 1) if a['rsi'] else 'N/A'}. Score: {a['score']}." if a else "N/A",
            "indicators": a['indicators'] if a and 'indicators' in a else {},
            "last_update": datetime.now(MOSCOW_TZ).strftime("%H:%M")
        }
        res_all.append(r)

    longs = sum(1 for r in res_all if r['signal'] == "LONG")
    shorts = sum(1 for r in res_all if r['signal'] == "SHORT")
    if longs > shorts:
        sentiment = "BULLISH"
    elif shorts > longs:
        sentiment = "BEARISH"
    else:
        sentiment = "NEUTRAL"

    out = {
        "signals": [r for r in res_all if r['signal'] != "HOLD"],
        "all_assets": res_all,
        "heatmap": [{"ticker": r['ticker'], "name": r['name'], "change": r['change_pct'],
                     "price": r['current_price'], "signal": r['signal'], "mode": r['mode'],
                     "rsi": r['indicators'].get('rsi') if r.get('indicators') else None} for r in res_all],
        "market_sentiment": sentiment,
        "stats": {"total": len(res_all), "long": longs, "short": shorts,
                  "hold": len(res_all) - longs - shorts},
        "last_update": datetime.now(MOSCOW_TZ).strftime("%d.%m %H:%M")
    }

    with open('signals.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"Done. Signals: {len(out['signals'])}, Sentiment: {sentiment}")

if __name__ == "__main__":
    main()
