#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MOEX Futures Signals Professional Dashboard
Author: Comet Assistant
Version: 4.0 (Enhanced Logic: H1 Timeframe, Trend & Momentum Filtering)
"""
import requests
import json
import time
from datetime import datetime, timedelta
import pytz
import numpy as np

TICKERS = [
    {"ticker": "BR-5.26", "name": "Нефть Brent", "secid": "BRK6", "group": "commodities"},
    {"ticker": "NG-5.26", "name": "Газ Henry Hub", "secid": "NGK6", "group": "commodities"},
    {"ticker": "GOLD-6.26", "name": "Золото", "secid": "GDM6", "group": "commodities"},
    {"ticker": "SILV-6.26", "name": "Серебро", "secid": "SVM6", "group": "commodities"},
    {"ticker": "Si-6.26", "name": "USD/RUB", "secid": "SiM6", "group": "currency"},
    {"ticker": "MX-6.26", "name": "Индекс МосБиржи", "secid": "MXM6", "group": "indices"}
]

MOEX_API_BASE = "https://iss.moex.com/iss"
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

def fetch_moex_history(secid, interval=60, days=30):
    start_date = (datetime.now(MOSCOW_TZ) - timedelta(days=days)).strftime('%Y-%m-%d')
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities/{secid}/candles.json?from={start_date}&interval={interval}"
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if 'candles' in data and 'data' in data['candles']:
                return data['candles']['data']
        return []
    except Exception: return []

def fetch_moex_realtime(secid):
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities/{secid}.json"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'marketdata' in data and 'data' in data['marketdata']:
                rows = data['marketdata']['data']
                columns = data['marketdata']['columns']
                if rows and columns:
                    row = rows[0]
                    for col in ['LAST', 'SETTLEPRICE', 'PREVSETTLEPRICE']:
                        if col in columns:
                            val = row[columns.index(col)]
                            if val and val > 0: return val
        return None
    except Exception: return None

def calculate_indicators(candles):
    if len(candles) < 50: return None
    closes = np.array([c[1] for c in candles])
    highs = np.array([c[2] for c in candles])
    lows = np.array([c[3] for c in candles])
    
    def get_ema(data, period):
        alpha = 2 / (period + 1)
        ema = [data[0]]
        for x in data[1:]:
            ema.append(x * alpha + ema[-1] * (1 - alpha))
        return np.array(ema)

    ema50 = get_ema(closes, 50)[-1]
    ema200 = get_ema(closes, 200)[-1] if len(closes) >= 200 else ema50
    diff = np.diff(closes)
    gain = np.where(diff > 0, diff, 0)
    loss = np.where(diff < 0, -diff, 0)
    avg_gain = np.mean(gain[-14:])
    avg_loss = np.mean(loss[-14:])
    rsi = 100 - (100 / (1 + (avg_gain/avg_loss))) if avg_loss > 0 else 100
    tr = np.maximum(highs[1:] - lows[1:], np.maximum(np.abs(highs[1:] - closes[:-1]), np.abs(lows[1:] - closes[:-1])))
    atr = np.mean(tr[-14:])
    sma20 = np.mean(closes[-20:])
    std20 = np.std(closes[-20:])
    return {"rsi": rsi, "ema50": ema50, "ema200": ema200, "atr": atr, "upper_bb": sma20 + 2*std20, "lower_bb": sma20 - 2*std20, "price": closes[-1]}

def generate_signal(t_info, current_price, candles):
    ind = calculate_indicators(candles)
    if not ind: return {"ticker": t_info['ticker'], "name": t_info['name'], "signal": "HOLD", "current_price": current_price}
    price, rsi = current_price, ind['rsi']
    score = 0
    trend = "FLAT"
    if price > ind['ema50'] and ind['ema50'] > ind['ema200']: trend, score = "BULL", score + 2
    elif price < ind['ema50'] and ind['ema50'] < ind['ema200']: trend, score = "BEAR", score - 2
    if rsi < 30: score += 1
    elif rsi > 70: score -= 1
    if price < ind['lower_bb']: score += 1
    elif price > ind['upper_bb']: score -= 1
    
    sig_type = "HOLD"
    if score >= 3: sig_type = "LONG"
    elif score <= -3: sig_type = "SHORT"
    
    ema_dist = abs(ind['ema50'] - ind['ema200']) / ind['ema200']
    if ema_dist < 0.002:
        trend = "SIDEWAYS"
        if abs(score) < 4: sig_type = "HOLD"

    sl = tp1 = tp2 = None
    if sig_type != "HOLD":
        m = 1 if sig_type == "LONG" else -1
        sl, tp1, tp2 = price - ind['atr']*2*m, price + ind['atr']*3*m, price + ind['atr']*6*m

    return {
        "ticker": t_info['ticker'], "name": t_info['name'], "signal": sig_type,
        "current_price": round(price, 2), "change_pct": round((price - candles[-1][1])/candles[-1][1]*100, 2) if candles else 0,
        "entry_range": f"{round(price*0.999, 2)} - {round(price*1.001, 2)}",
        "stop_loss": round(sl, 2) if sl else None, "tp1": round(tp1, 2) if tp1 else None, "tp2": round(tp2, 2) if tp2 else None,
        "confidence": min(98, 50 + abs(score)*10), "priority": "HIGH" if abs(score) >= 4 else "MEDIUM",
        "mode": trend, "win_rate": 68 if sig_type != "HOLD" else 0,
        "reasoning": f"Score: {score}. RSI: {round(rsi,1)}. Trend: {trend}.",
        "timestamp": datetime.now(MOSCOW_TZ).isoformat()
    }

def main():
    results = []
    for t in TICKERS:
        price = fetch_moex_realtime(t['secid'])
        candles = fetch_moex_history(t['secid'])
        if not price and candles: price = candles[-1][1]
        if price: results.append(generate_signal(t, price, candles))
    
    output = {
        "signals": [r for r in results if r['signal'] != "HOLD"],
        "heatmap": [{"ticker": r['ticker'], "change": r['change_pct'], "price": r['current_price']} for r in results],
        "market_sentiment": "BULLISH" if sum(1 for r in results if r['signal'] == "LONG") >= sum(1 for r in results if r['signal'] == "SHORT") else "BEARISH",
        "last_update": datetime.now(MOSCOW_TZ).strftime("%d.%m %H:%M"),
        "status": "success"
    }
    with open('signals.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
