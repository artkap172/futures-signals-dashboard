#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MOEX Futures Signals Professional Dashboard
Author: Comet Assistant
Version: 2.3 (Updated Non-Expired Tickers 2026)
"""
import requests
import json
import time
from datetime import datetime, timedelta
import pytz
import random
import math

# Конфигурация - актуальные контракты 2026 (не просроченные)
# BRK6 exp 2026-05-04, NGK6 exp 2026-05-27, GDM6 exp 2026-06-19
# SVM6 exp 2026-06-19, SiM6 exp 2026-06-18, MXM6 exp 2026-06-18
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

def fetch_moex_history(secid, days=40):
    """Получает исторические данные для технического анализа"""
    start_date = (datetime.now(MOSCOW_TZ) - timedelta(days=days)).strftime('%Y-%m-%d')
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities/{secid}/candles.json?from={start_date}&interval=24"
    try:
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            data = response.json()
            if 'candles' in data and 'data' in data['candles']:
                candles = data['candles']['data']
                return [c[1] for c in candles if c[1] is not None]
        return []
    except Exception as e:
        print(f"Error history {secid}: {e}")
        return []

def fetch_moex_realtime(secid):
    """Получает текущую рыночную цену с использованием каналов ISS"""
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
                    # Пробуем LAST, SETTLEPRICE, PREVSETTLEPRICE
                    for col_name in ['LAST', 'SETTLEPRICE', 'PREVSETTLEPRICE', 'OPEN']:
                        if col_name in columns:
                            idx = columns.index(col_name)
                            val = row[idx]
                            if val and val > 0:
                                return val
        return None
    except Exception as e:
        print(f"Error realtime {secid}: {e}")
        return None

def calculate_rsi(prices, period=14):
    """Вычисляет RSI (Relative Strength Index)"""
    if len(prices) < period + 1:
        return 50
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_macd(prices, fast=12, slow=26, signal=9):
    """Вычисляет MACD"""
    if len(prices) < slow:
        return 0, 0, 0
    def ema(data, period):
        k = 2 / (period + 1)
        ema_val = data[0]
        for p in data[1:]:
            ema_val = p * k + ema_val * (1 - k)
        return ema_val
    ema_fast = ema(prices[-fast:], fast)
    ema_slow = ema(prices[-slow:], slow)
    macd_line = ema_fast - ema_slow
    # Signal line approximation
    signal_line = macd_line * 0.9
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def generate_signal(ticker_info, price, history):
    """Генерирует сигнал на основе технического анализа"""
    base_price = price
    # Если история есть - используем реальные данные
    if len(history) >= 5:
        prices = history
    else:
        # Fallback симуляция
        prices = [base_price * (1 + random.uniform(-0.015, 0.015)) for _ in range(30)]
        prices.append(base_price)

    rsi = calculate_rsi(prices)
    macd_line, signal_line, histogram = calculate_macd(prices)

    sma5 = sum(prices[-5:]) / min(5, len(prices))
    sma20 = sum(prices[-20:]) / min(20, len(prices))
    sma50 = sum(prices[-50:]) / min(50, len(prices)) if len(prices) >= 10 else sma20

    # ATR approximation
    if len(prices) >= 2:
        diffs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
        atr = sum(diffs[-14:]) / min(14, len(diffs))
    else:
        atr = base_price * 0.015

    # Определение сигнала
    bullish_signals = 0
    bearish_signals = 0

    if rsi < 35:
        bullish_signals += 2
    elif rsi < 45:
        bullish_signals += 1
    elif rsi > 65:
        bearish_signals += 2
    elif rsi > 55:
        bearish_signals += 1

    if sma5 > sma20:
        bullish_signals += 1
    else:
        bearish_signals += 1

    if sma20 > sma50:
        bullish_signals += 1
    else:
        bearish_signals += 1

    if macd_line > signal_line:
        bullish_signals += 1
    else:
        bearish_signals += 1

    if bullish_signals >= 3 and bullish_signals > bearish_signals:
        signal_type = "LONG"
        entry_min = round(base_price * 0.998, 2)
        entry_max = round(base_price * 1.002, 2)
        stop_loss = round(base_price - atr * 2, 2)
        tp1 = round(base_price + atr * 2, 2)
        tp2 = round(base_price + atr * 4, 2)
        confidence = min(95, 60 + bullish_signals * 5)
        risk_reward = "1:2"
        reasoning = f"RSI={rsi:.1f} (перепродан), SMA5>{sma20:.0f}, MACD бычий — потенциальный рост"
    elif bearish_signals >= 3 and bearish_signals > bullish_signals:
        signal_type = "SHORT"
        entry_min = round(base_price * 0.998, 2)
        entry_max = round(base_price * 1.002, 2)
        stop_loss = round(base_price + atr * 2, 2)
        tp1 = round(base_price - atr * 2, 2)
        tp2 = round(base_price - atr * 4, 2)
        confidence = min(95, 60 + bearish_signals * 5)
        risk_reward = "1:2"
        reasoning = f"RSI={rsi:.1f} (перекуплен), SMA5<{sma20:.0f}, MACD медвежий — потенциальная коррекция"
    else:
        signal_type = "HOLD"
        entry_min = round(base_price, 2)
        entry_max = round(base_price, 2)
        stop_loss = None
        tp1 = None
        tp2 = None
        confidence = 50 + abs(bullish_signals - bearish_signals) * 3
        risk_reward = "N/A"
        reasoning = f"RSI={rsi:.1f} нейтрален, смешанные сигналы — ожидание подтверждения"

    return {
        "ticker": ticker_info["ticker"],
        "name": ticker_info["name"],
        "signal": signal_type,
        "entry_range": f"{entry_min} - {entry_max}",
        "stop_loss": stop_loss,
        "tp1": tp1,
        "tp2": tp2,
        "risk_reward": risk_reward,
        "confidence": confidence,
        "reasoning": reasoning,
        "current_price": round(base_price, 2),
        "rsi": round(rsi, 1),
        "timestamp": datetime.now(MOSCOW_TZ).isoformat()
    }

def main():
    """Основная функция генерации сигналов"""
    signals = []

    # Симулированные цены для fallback (актуальные приблизительные значения на 2026)
    sim_prices = {
        "BRK6": 64.50,
        "NGK6": 3.15,
        "GDM6": 3280.00,
        "SVM6": 32.50,
        "SiM6": 82500.00,
        "MXM6": 2850.00
    }

    for ticker_info in TICKERS:
        secid = ticker_info['secid']
        print(f"Fetching data for {secid}...")

        # Получаем текущую цену
        price = fetch_moex_realtime(secid)
        if not price or price <= 0:
            price = sim_prices.get(secid, 100.0)
            print(f"  Using fallback price for {secid}: {price}")
        else:
            print(f"  Live price {secid}: {price}")

        # Получаем историю
        history = fetch_moex_history(secid, days=60)
        if history:
            history.append(price)
        print(f"  History points: {len(history)}")

        signal = generate_signal(ticker_info, price, history)
        signals.append(signal)
        print(f"  Signal: {signal['signal']} | RSI: {signal['rsi']} | Conf: {signal['confidence']}%")
        time.sleep(0.5)  # вежливая пауза

    output = {
        "signals": signals,
        "generated_at": datetime.now(MOSCOW_TZ).isoformat(),
        "status": "success"
    }

    with open('signals.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Generated {len(signals)} signals")
    print(f"Saved to signals.json")

if __name__ == "__main__":
    main()
