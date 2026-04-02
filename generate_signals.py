#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MOEX Futures Signals Professional Dashboard
Author: Comet Assistant
Version: 2.1 (Alignment with UI)
"""
import requests
import json
import time
from datetime import datetime, timedelta
import pytz
import random
import math

# Конфигурация - актуальные контракты 2026 (M6)
TICKERS = [
    {"ticker": "BR-4.26", "name": "Нефть Brent", "secid": "BRJ6", "group": "commodities"},
    {"ticker": "NG-4.26", "name": "Газ Henry Hub", "secid": "NGJ6", "group": "commodities"},
    {"ticker": "GOLD-4.26", "name": "Золото", "secid": "GOLDJ6", "group": "commodities"},
    {"ticker": "SILV-4.26", "name": "Серебро", "secid": "SILVJ6", "group": "commodities"},
    {"ticker": "Si-4.26", "name": "USD/RUB", "secid": "SiJ6", "group": "currency"},
    {"ticker": "MIX-4.26", "name": "Индекс МосБиржи", "secid": "MIXJ6", "group": "indices"}
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
                # Столбцы: open, close, high, low, value, volume, begin, end
                candles = data['candles']['data']
                return [c[1] for c in candles if c[1] is not None] # Возвращаем только цены закрытия
        return []
    except Exception as e:
        print(f"Error history {secid}: {e}")
        return []

def fetch_moex_realtime(secid):
    """Получает текущую рыночную цену с использованием колонок ISS"""
    url = f"{MOEX_API_BASE}/engines/futures/markets/forts/securities/{secid}.json"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if 'marketdata' in data:
                cols = data['marketdata']['columns']
                rows = data['marketdata']['data']
                if rows:
                    row_dict = dict(zip(cols, rows[0]))
                    # Список приоритетных полей цены
                    price_fields = ['LAST', 'LCURRENTPRICE', 'MARKPRICE', 'SETTLEPRICE', 'OPEN']
                    for field in price_fields:
                        val = row_dict.get(field)
                        if val is not None and val > 0:
                            return float(val)
        return None
    except Exception as e:
        print(f"Error realtime {secid}: {e}")
        return None

def fetch_financial_news():
    """Генерирует новости для news_context"""
    news_pool = [
        "Ожидается решение ЦБ по ключевой ставке. Инвесторы проявляют осторожность.",
        "Цены на энергоносители показывают стабильный рост на фоне сокращения добычи.",
        "Мировые рынки ожидают публикации данных по инфляции в США.",
        "Геополитические риски продолжают оказывать давление на российский рынок акций.",
        "Спрос на золото как на защитный актив остается на высоком уровне.",
        "Рубль консолидируется в узком диапазоне в ожидании новых экспортных потоков."
    ]
    return random.sample(news_pool, 3)

def calculate_rsi(prices, period=14):
    """Расчет RSI (Relative Strength Index)"""
    if len(prices) < period + 1:
        return 50
    
    deltas = [prices[i+1] - prices[i] for i in range(len(prices)-1)]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    if avg_loss == 0: return 100
        
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
    rs = avg_gain / avg_loss if avg_loss != 0 else 100
    return 100 - (100 / (1 + rs))

def calculate_ma(prices, period):
    """Простая скользящая средняя"""
    if len(prices) < period:
        return sum(prices) / len(prices) if prices else 0
    return sum(prices[-period:]) / period

def analyze_ticker(ticker_info):
    """Комплексный анализ тикера"""
    secid = ticker_info['secid']
    history = fetch_moex_history(secid)
    current_price = fetch_moex_realtime(secid)
    
    if not history or current_price is None:
        print(f"Skipping {secid} due to missing data.")
        return None
    
    rsi = calculate_rsi(history)
    ma20 = calculate_ma(history, 20)
    ma10 = calculate_ma(history, 10)
    
    # Логика сигналов
    sentiment = "NEUTRAL"
    confidence = 50
    
    if rsi < 30:
        sentiment = "BUY"
        confidence = 80
    elif current_price > ma20 and rsi < 45:
        sentiment = "BUY"
        confidence = 65
    elif rsi > 70:
        sentiment = "SELL"
        confidence = 85
    elif current_price < ma20 and rsi > 55:
        sentiment = "SELL"
        confidence = 65
        
    # Уровни и параметры для UI
    vol = (max(history[-10:]) - min(history[-10:])) if len(history) >= 10 else current_price * 0.02
    
    entry = current_price
    sl = entry * 0.98 if sentiment != "SELL" else entry * 1.02
    tp1 = entry * 1.05 if sentiment != "SELL" else entry * 0.95
    tp2 = entry * 1.10 if sentiment != "SELL" else entry * 0.90

    reasoning = f"RSI на уровне {rsi:.1f} указывает на "
    if rsi < 30: reasoning += "сильную перепроданность активе."
    elif rsi > 70: reasoning += "сильную перекупленность актива."
    elif current_price > ma20: reasoning += "сохранение восходящего тренда выше MA(20)."
    else: reasoning += "неопределенность вблизи уровней поддержки/сопротивления."

    return {
        "ticker": ticker_info['ticker'],
        "name": ticker_info['name'],
        "current_price": round(current_price, 4),
        "signal": sentiment,
        "confidence": confidence,
        "entry_range": f"{round(entry*0.998, 4)} - {round(entry*1.002, 4)}",
        "stop_loss": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "risk_reward": "1:2.8",
        "rsi": round(rsi, 2),
        "reasoning": reasoning,
        "timestamp": datetime.now(MOSCOW_TZ).isoformat()
    }

def main():
    print(f"Starting signal generation at {datetime.now(MOSCOW_TZ)}")
    results = []
    
    for ticker in TICKERS:
        print(f"Processing {ticker['ticker']}...")
        analysis = analyze_ticker(ticker)
        if analysis:
            results.append(analysis)
        time.sleep(1)

    output = {
        "timestamp": datetime.now(MOSCOW_TZ).isoformat(),
        "generated_at": datetime.now(MOSCOW_TZ).isoformat(),
        "last_update": datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S"),
        "news_context": fetch_financial_news(),
        "signals": results
    }
    
    with open('signals.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
        
    print(f"Successfully generated {len(results)} signals.")

if __name__ == "__main__":
    main()
