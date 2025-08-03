import requests
import time
import json
from datetime import datetime, timedelta
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

def date_to_milliseconds(date_str):
    """Convert UTC date string to milliseconds since epoch."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp() * 1000)
    except Exception as e:
        logger.error(f"Date conversion error: {str(e)}")
        raise

def fetch_klines(start_time, end_time, max_retries=5, delay=0.2):
    """Fetch minute klines from Binance API with retries."""
    url = "https://api.binance.com/api/v3/klines"
    params = {
        'symbol': 'BNBUSDT',
        'interval': '1m',
        'startTime': start_time,
        'endTime': end_time,
        'limit': 1000
    }
    
    for attempt in range(max_retries):
        try:
            logger.debug(f"Request attempt {attempt+1}: {params}")
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"Successful request: {start_time} to {end_time}")
                logger.debug(f"Received {len(data)} records")
                return data
            elif response.status_code == 429:
                logger.warning(f"Rate limit exceeded! Retrying in {delay*(2**attempt)}s")
                time.sleep(delay * (2 ** attempt))
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
                logger.error(f"Request params: {params}")
                if response.status_code >= 500:
                    logger.info("Server error, retrying...")
                else:
                    return None
                    
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
        
        if attempt < max_retries - 1:
            sleep_time = delay * (2 ** attempt)
            logger.info(f"Retrying in {sleep_time:.1f}s...")
            time.sleep(sleep_time)
    
    logger.error(f"Max retries exceeded for request: {params}")
    return None

def save_progress(data, filename):
    """Save download progress to file"""
    try:
        with open(filename, 'w') as f:
            json.dump(data, f)
        logger.info(f"Progress saved: {len(data)} records")
    except Exception as e:
        logger.error(f"Failed to save progress: {str(e)}")

def main():
    # Параметры сбора данных
    start_date = "2017-11-06 00:00:00"  # Начало торгов BNB/USDT на Binance
    output_file = "bnb_usdt_minute_prices.json"
    
    logger.info("Starting BNB/USDT minute data download")
    
    # Рассчет временных меток
    current_start = date_to_milliseconds(start_date)
    end_time = int(time.time() * 1000)  # Текущее время в мс
    
    all_data = []
    request_count = 0
    last_successful_timestamp = current_start
    
    try:
        while current_start < end_time:
            # Вычисляем конечное время для запроса
            current_end = current_start + 1000 * 60 * 1000 - 1
            
            # Получаем данные
            logger.debug(f"Fetching {datetime.fromtimestamp(current_start/1000)} to {datetime.fromtimestamp(min(current_end, end_time)/1000)}")
            klines = fetch_klines(current_start, min(current_end, end_time))
            
            if klines is None:
                logger.error(f"Failed to get data for range: {current_start}-{current_end}")
                logger.info("Waiting 60 seconds before continuing...")
                time.sleep(60)
                continue
            
            if not klines:
                logger.info("No data returned, moving to next time range")
                current_start = current_end + 1
                continue
            
            # Обрабатываем полученные данные
            batch = []
            for k in klines:
                timestamp = k[0]
                close_price = k[4]  # Цена закрытия свечи
                batch.append([timestamp, close_price])
            
            all_data.extend(batch)
            request_count += 1
            last_successful_timestamp = current_start
            
            # Обновляем стартовое время для следующего запроса
            last_timestamp = klines[-1][0]
            current_start = last_timestamp + 60000  # Следующая минута
            
            # Периодическое сохранение прогресса
            if request_count % 50 == 0:
                save_progress(all_data, output_file)
                
                # Промежуточная статистика
                first_ts = all_data[0][0]
                last_ts = all_data[-1][0]
                logger.info(f"Progress: {len(all_data)} records | "
                            f"From {datetime.fromtimestamp(first_ts/1000)} to "
                            f"{datetime.fromtimestamp(last_ts/1000)}")
            
            # Задержка для соблюдения лимитов API
            time.sleep(0.2)
    
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
    except Exception as e:
        logger.exception("Fatal error during download:")
    finally:
        # Финализируем сохранение данных
        save_progress(all_data, output_file)
        logger.info(f"Final save: {len(all_data)} records")
        
        # Отчет о завершении
        if current_start >= end_time:
            logger.info("Download completed successfully")
        else:
            logger.warning(f"Download incomplete. Last successful timestamp: {last_successful_timestamp}")
        
        logger.info(f"Total requests made: {request_count}")
        logger.info(f"Data saved to {output_file}")

if __name__ == "__main__":
    main()