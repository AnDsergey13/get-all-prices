import requests
import time
import json
from datetime import datetime, timedelta
import logging
import argparse
import os

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

def get_first_trading_date(symbol, max_retries=3):
    """
    Получает реальную дату начала торгов для символа через API Binance.
    Запрашивает данные с очень ранней даты и использует первую полученную свечу.
    """
    logger.info(f"Getting first trading date for {symbol}")
    
    # Используем очень раннюю дату (до создания Binance)
    early_start = date_to_milliseconds("2010-01-01 00:00:00")
    end_time = int(time.time() * 1000)
    
    url = "https://api.binance.com/api/v3/klines"
    params = {
        'symbol': symbol,
        'interval': '1m',
        'startTime': early_start,
        'endTime': end_time,
        'limit': 1  # Запрашиваем только первую свечу
    }
    
    for attempt in range(max_retries):
        try:
            logger.debug(f"Attempt {attempt+1}: Requesting first candle for {symbol}")
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                if data:
                    first_timestamp = data[0][0]  # Время открытия первой свечи
                    first_date = datetime.fromtimestamp(first_timestamp/1000)
                    logger.info(f"First trading date for {symbol}: {first_date}")
                    return first_timestamp
                else:
                    logger.warning(f"No data returned for {symbol}")
                    return None
            elif response.status_code == 429:
                logger.warning(f"Rate limit exceeded! Retrying in {2**attempt}s")
                time.sleep(2 ** attempt)
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
        
        if attempt < max_retries - 1:
            time.sleep(1)
    
    logger.error(f"Failed to get first trading date for {symbol}")
    return None

def fetch_klines(start_time, end_time, symbol, max_retries=5, delay=0.2):
    """Fetch minute klines from Binance API with retries."""
    url = "https://api.binance.com/api/v3/klines"
    params = {
        'symbol': symbol,
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
                if data:
                    logger.info(f"Successful request: {start_time} to {end_time}")
                    logger.debug(f"Received {len(data)} records")
                    return data
                else:
                    logger.debug("No data in response")
                    return []
            elif response.status_code == 429:
                logger.warning(f"Rate limit exceeded! Retrying in {delay*(2**attempt)}s")
                time.sleep(delay * (2 ** attempt))
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
                logger.error(f"Request params: {params}")
                if response.status_code >= 500:
                    logger.info("Server error, retrying...")
                    time.sleep(delay * (2 ** attempt))
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
    parser = argparse.ArgumentParser(description='Download cryptocurrency data from Binance')
    parser.add_argument('--symbol', type=str, default='BNBUSDT',
                        help='Trading symbol (e.g., BNBUSDT, BTCUSDT, ETHUSDT)')
    parser.add_argument('--start_date', type=str, default='2017-11-06 00:00:00',
                        help='Start date in format YYYY-MM-DD HH:MM:SS')
    args = parser.parse_args()
    
    symbol = args.symbol
    symbol_lower = symbol.lower()
    start_date = args.start_date
    output_file = f"{symbol_lower}_minute_prices.json"
    
    logger.info(f"Starting {symbol} minute data download")
    
    # Получаем реальную дату начала торгов
    first_trading_timestamp = get_first_trading_date(symbol)
    if first_trading_timestamp:
        # Используем максимальное значение между указанной датой и реальной датой начала торгов
        current_start = max(date_to_milliseconds(start_date), first_trading_timestamp)
        logger.info(f"Using start date: {datetime.fromtimestamp(current_start/1000)}")
    else:
        # Если не удалось определить дату начала, используем указанную
        current_start = date_to_milliseconds(start_date)
        logger.warning(f"Could not determine first trading date. Using specified date: {datetime.fromtimestamp(current_start/1000)}")
    
    end_time = int(time.time() * 1000)
    
    all_data = []
    request_count = 0
    last_successful_timestamp = current_start
    empty_intervals = 0
    max_empty_intervals = 100  # Уменьшил, так как теперь мы знаем реальную дату начала
    
    try:
        while current_start < end_time:
            current_end = current_start + 1000 * 60 * 1000 - 1
            
            logger.debug(f"Fetching {datetime.fromtimestamp(current_start/1000)} to {datetime.fromtimestamp(min(current_end, end_time)/1000)}")
            klines = fetch_klines(current_start, min(current_end, end_time), symbol)
            
            if klines is None:
                logger.error(f"Failed to get data for range: {current_start}-{current_end}")
                logger.info("Waiting 60 seconds before continuing...")
                time.sleep(60)
                continue
            
            if not klines:
                logger.info("No data returned, moving to next time range")
                empty_intervals += 1
                
                if empty_intervals > max_empty_intervals:
                    logger.error(f"No data for {max_empty_intervals} consecutive intervals.")
                    break
                
                current_start = current_end + 1
                continue
            
            empty_intervals = 0
            
            batch = []
            for k in klines:
                timestamp = k[0]
                close_price = k[4]
                batch.append([timestamp, close_price])
            
            all_data.extend(batch)
            request_count += 1
            last_successful_timestamp = current_start
            
            last_timestamp = klines[-1][0]
            current_start = last_timestamp + 60000
            
            if request_count % 50 == 0:
                save_progress(all_data, output_file)
                
                first_ts = all_data[0][0]
                last_ts = all_data[-1][0]
                logger.info(f"Progress: {len(all_data)} records | "
                            f"From {datetime.fromtimestamp(first_ts/1000)} to "
                            f"{datetime.fromtimestamp(last_ts/1000)}")
            
            time.sleep(0.2)
    
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
    except Exception as e:
        logger.exception("Fatal error during download:")
    finally:
        save_progress(all_data, output_file)
        logger.info(f"Final save: {len(all_data)} records")
        
        if current_start >= end_time:
            logger.info("Download completed successfully")
        else:
            logger.warning(f"Download incomplete. Last successful timestamp: {last_successful_timestamp}")
        
        logger.info(f"Total requests made: {request_count}")
        logger.info(f"Data saved to {output_file}")

if __name__ == "__main__":
    main()