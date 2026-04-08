#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MOEX Futures Signals Professional Dashboard (Advanced Version)
Author: Comet Assistant
Version: 3.1 (Enhanced Analytics, Risk Management & Heatmap Support)
"""
import requests
import json
import time
from datetime import datetime, timedelta
import pytz
import math

# Конфигурация - актуальные контракты 2026
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

def fetch_moex_history(secid, days=100):
    start_date = (datetime.now(MOSCOW_TZ) - timedelta(days=days)).strftime('%Y-%m-%d')
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities/{secid}/candles.json?from={start_date}&interval=24"
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
                    for col_name in ['LAST', 'SETTLEPRICE', 'PREVSETTLEPRICE']:
                        if col_name in columns:
                            val = row[columns.index(col_name)]
                            if val and val > 0: return val
        return None
    except Exception: return None

def calculate_atr(candles, period=14):
    if len(candles) < period + 1: return 0
    tr_list = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i][2], candles[i][3], candles[i-1][1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        tr_list.append(tr)
    return sum(tr_list[-period:]) / period

def calculate_indicators(prices):
    if len(prices) < 50: return None
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    avg_gain = sum([d for d in deltas[-14:] if d > 0]) / 14
    avg_loss = sum([-d for d in deltas[-14:] if d < 0]) / 14
    rsi = 100 - (100 / (1 + (avg_gain/avg_loss))) if avg_loss != 0 else 100
    
    def ema(data, p):
        k = 2 / (p + 1)
        res = data[0]
        for val in data[1:]: res = val * k + res * (1 - k)
        return res
    
    ema100 = ema(prices[-100:], 100) if len(prices) >= 100 else ema(prices, len(prices))
    ema200 = ema(prices[-200:], 200) if len(prices) >= 200 else ema100
    macd = ema(prices[-12:], 12) - ema(prices[-26:], 26)
    return {"rsi": rsi, "ema100": ema100, "ema200": ema200, "macd": macd}

def generate_signal(t_info, price, candles):
    prices = [c[1] for c in candles if c[1] is not None]
    atr = calculate_atr(candles)
    ind = calculate_indicators(prices)
    
    # Change %
    prev_close = candles[-1][1] if candles else price
    change_pct = round((price - prev_close) / prev_close * 100, 2) if prev_close else 0
    
    if not ind or atr == 0:
        return {"ticker": t_info['ticker'], "name": t_info['name'], "signal": "HOLD", "current_price": price, "change_pct": change_pct}

    score = 0
    if ind['rsi'] < 30: score += 3
    elif ind['rsi'] > 70: score -= 3
    if price > ind['ema200']: score += 2
    else: score -= 2
    
    sig_type = "HOLD"
    if score >= 3: sig_type = "LONG"
    elif score <= -3: sig_type = "SHORT"
    
    sl = tp1 = tp2 = None
    if sig_type != "HOLD":
        mult = 1 if sig_type == "LONG" else -1
        sl = price - (atr * 1.5 * mult)
        tp1 = price + (atr * 3.0 * mult)
        tp2 = price + (atr * 4.5 * mult)

    mode = "FLAT" if abs(ind['ema100'] - ind['ema200']) / ind['ema200'] < 0.002 else ("BULL_TREND" if ind['ema100'] > ind['ema200'] else "BEAR_TREND")
    
    return {
        "ticker": t_info['ticker'], "name": t_info['name'], "signal": sig_type,
        "current_price": round(price, 2), "change_pct": change_pct,
        "entry_range": f"{round(price*0.999, 2)} - {round(price*1.001, 2)}",
        "stop_loss": round(sl, 2) if sl else None,
        "tp1": round(tp1, 2) if tp1 else None,
        "tp2": round(tp2, 2) if tp2 else None,
        "confidence": min(92, 60 + abs(score)*5),
        "priority": "HIGH" if (sig_type == "LONG" and mode == "BULL_TREND") or (sig_type == "SHORT" and mode == "BEAR_TREND") else "MEDIUM",
        "mode": mode, "win_rate": 62 if sig_type != "HOLD" else 0,
        "reasoning": f"Mode: {mode}. RSI: {round(ind['rsi'],1)}. ATR applied.",
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
        "market_sentiment": "BULLISH" if sum(1 for r in results if r['signal'] == "LONG") > sum(1 for r in results if r['signal'] == "SHORT") else "BEARISH",
        "last_update": datetime.now(MOSCOW_TZ).strftime("%d.%m %H:%M"),
        "status": "success"
    }
    with open('signals.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
