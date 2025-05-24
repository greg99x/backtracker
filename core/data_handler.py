
import pandas as pd
import yfinance as yf
import os
from datetime import datetime, timedelta
import logging

class DataStore:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.data = {}  # Contains current full data. key: symbol
        self.yfinance_objects = {}  # Contains instances of yf.Ticker

    def read_csv(self, symbol, filename) -> bool:
        '''
        Read data from CSV file, for given ticker
        '''
        try:
            if not os.path.exists(filename):
                self.logger.info(f"File '{filename}' does not exist.")
                return False
            
            df = pd.read_csv(filename, index_col=0, parse_dates=True)
            self.data[symbol] = df
            self.logger.info(f'Read data with shape: {df.shape}')
            self.logger.info(f'Reader: Last date in date: {df.index.max()}')
            return True
        
        except Exception as e:
            print(f"Error reading CSV for {symbol}: {e}")
            return False

    def write_csv(self, symbol, filename) -> bool:
        '''
        Write CSV file, from data dict for given symbol to file 'filename'
        '''
        try:
            self.data[symbol].to_csv(filename)
            self.logger.info(f'Wrote data with shape: {self.data[symbol].shape}')
            self.logger.info(f'Writer: Last date in data: {self.data[symbol].index.max()}')
            return True
        except Exception as e:
            self.logger.debug(f"Error writing CSV for {symbol}: {e}")
            return False

    def _get_data_from_yf(self, symbol, start_date=None, end_date=None, interval='1d') -> pd.DataFrame:
        '''
        Wrapper function for yf.Ticker.history calls
        Return pd.DataFrame with downloaded history
        For now, only tested to work on time interval '1d'
        '''
        if symbol not in self.yfinance_objects:
            self.yfinance_objects[symbol] = yf.Ticker(symbol)

        try:
            if not start_date and not end_date:
                df = self.yfinance_objects[symbol].history(
                    period='max',
                    interval=interval
                )
            else:
                df = self.yfinance_objects[symbol].history(
                    start=start_date,
                    end=end_date,
                    interval=interval
                )
            return df
        except Exception as e:
            self.logger.debug(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()


    def update_data(self, symbol, end_date=None, redownload_timedelta=-1) -> bool:
            '''
            Checks last available price in data, and download missing date until the date 'end_date'
            Return True if success
            Return False if error
            '''
            if symbol not in self.data:
                self.data[symbol] = pd.DataFrame()
                self.logger.info(f'Symbol {symbol} not in data yet.')

            if redownload_timedelta > 0:
                self.logger.debug('Timedelta for downloading data must be negative')
                return False

            df_existing = self.data[symbol]
            last_date = df_existing.index.max() if not df_existing.empty else None

            # Define the start of the new range
            if last_date is None:
                start_date = None  # get full history
                self.logger.info('Start date for update is none')
            else:
                start_date = (last_date + timedelta(days=redownload_timedelta)).strftime('%Y-%m-%d')
                self.logger.info(f'Start date for update is {start_date}')

            if end_date is None:
                end_date = None
                self.logger.info('End date is none')
            else:
                end_date = end_date.strftime('%Y-%m-%d')
                self.logger.info(f'End date is {end_date}')
            
            try:
                new_data = self._get_data_from_yf(symbol, start_date=start_date, end_date=end_date)


                if new_data.empty:
                    self.logger.info(f"No new data downloaded for {symbol}.")
                    return False

                self.logger.info(f'Downloaded data with shape: {new_data.shape}')
                self.data[symbol] = pd.concat([df_existing, new_data]).sort_index().drop_duplicates()
                return True
            
            except Exception as e:
                self.logger.debug(f"Failed to update data for {symbol}: {e}")
                return False

'''
if __name__ == '__main__':
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)  # Set minimum level to DEBUG
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)  # Also handle DEBUG level logs
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    data_store = DataStore(logger=logger)
    read_status = data_store.read_csv('BTC-USD','btcusd.csv')
    status = data_store.update_data('BTC-USD',redownload_timedelta=-2)
    data_store.write_csv('BTC-USD','btcusd.csv')
'''
