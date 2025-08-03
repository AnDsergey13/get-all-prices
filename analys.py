import json
import collections
from datetime import datetime
import math
import argparse
import os

def calculate_price_changes(input_file, output_file):
    # Загрузка исторических данных
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    print(f"Загружено {len(data)} записей")
    
    # Сортировка по времени на всякий случай
    data.sort(key=lambda x: x[0])
    
    # Вычисление процентных изменений
    results = []
    total = len(data) - 1
    
    for i in range(1, len(data)):
        # Периодический вывод прогресса
        if i % 100000 == 0:
            dt = datetime.fromtimestamp(data[i][0]/1000)
            print(f"Обработано {i}/{total} записей ({i/total:.1%}) | Текущая дата: {dt.strftime('%Y-%m-%d %H:%M')}")
        
        timestamp = data[i][0]
        price1 = float(data[i-1][1])
        price2 = float(data[i][1])
        
        # Расчет процентного изменения
        change = (price2 - price1) / price1 * 100
        rounded_change = round(change, 10)  # Округление до 10 знаков
        
        results.append([timestamp, rounded_change])
    
    # Сохранение результатов
    with open(output_file, 'w') as f:
        json.dump(results, f)
    
    print(f"\nПроцентные изменения сохранены в {output_file}")
    return results

def frequency_analysis(changes_data, symbol, interval):
    symbol_lower = symbol.lower()
    
    # Извлекаем только значения изменений
    changes = [item[1] for item in changes_data]
    
    # Считаем частоту
    counter = collections.Counter(changes)
    
    # Сортируем по частоте
    sorted_freq = counter.most_common()
    
    # Сохраняем полный анализ в файл
    with open(f'frequency_analysis_full_{symbol_lower}_{interval}.json', 'w') as f:
        json.dump(sorted_freq, f)
    
    # Находим экстремальные значения
    sorted_by_value = sorted(changes_data, key=lambda x: x[1])
    top_negative = sorted_by_value[:10]  # Самые большие отрицательные изменения
    top_positive = sorted_by_value[-10:][::-1]  # Самые большие положительные изменения
    
    # Подготовка данных для вывода
    total = len(changes)
    report = []
    
    # Формируем заголовок
    report.append(f"Анализ для {symbol} (интервал: {interval})")
    report.append(f"{'Процентное изменение':^25} | {'Количество':^12}")
    report.append("-" * 40)
    
    # Топ-50 самых частых
    report.append("\nСамые частые изменения (топ-50):")
    for value, count in sorted_freq[:50]:
        report.append(f"{value:>24.10f}% | {count:>11,}")
    
    # Топ-50 самых редких
    report.append("\nСамые редкие изменения (топ-50):")
    for value, count in sorted_freq[-50:]:
        report.append(f"{value:>24.10f}% | {count:>11,}")
    
    # Самые большие изменения
    report.append("\nСамые большие положительные изменения:")
    for item in top_positive:
        dt = datetime.fromtimestamp(item[0]/1000).strftime('%Y-%m-%d %H:%M')
        report.append(f"{dt} | {item[1]:>24.10f}%")
    
    report.append("\nСамые большие отрицательные изменения:")
    for item in top_negative:
        dt = datetime.fromtimestamp(item[0]/1000).strftime('%Y-%m-%d %H:%M')
        report.append(f"{dt} | {item[1]:>24.10f}%")
    
    # Статистика
    report.append("\nОбщая статистика:")
    report.append(f"Всего изменений: {total:,}")
    report.append(f"Уникальных значений: {len(sorted_freq):,}")
    
    # Сохранение отчета
    report_filename = f'frequency_analysis_report_{symbol_lower}_{interval}.txt'
    with open(report_filename, 'w') as f:
        f.write("\n".join(report))
    
    # Вывод сокращенной версии в консоль
    print("\n".join(report[:55] + ["...", f"Полный отчет сохранен в {report_filename}"]))
    return sorted_freq

def main():
    parser = argparse.ArgumentParser(description='Analyze cryptocurrency price changes')
    parser.add_argument('--symbol', type=str, default='BNBUSDT',
                        help='Trading symbol (e.g., BNBUSDT, BTCUSDT, ETHUSDT)')
    parser.add_argument('--interval', type=str, default='1m', 
                        choices=['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'],
                        help='Time interval for candles (default: 1m)')
    args = parser.parse_args()
    
    symbol = args.symbol
    symbol_lower = symbol.lower()
    interval = args.interval
    
    input_file = f"{symbol_lower}_{interval}_prices.json"
    
    # Проверяем существование файла
    if not os.path.exists(input_file):
        print(f"Ошибка: файл {input_file} не найден.")
        print(f"Сначала скачайте данные для пары {symbol} с интервалом {interval} с помощью main.py")
        return
    
    changes_file = f"price_changes_{symbol_lower}_{interval}.json"
    
    # Шаг 1: Расчет процентных изменений
    changes_data = calculate_price_changes(input_file, changes_file)
    
    # Шаг 2: Частотный анализ
    frequency_analysis(changes_data, symbol, interval)

if __name__ == "__main__":
    main()