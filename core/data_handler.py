
import pandas as pd
import yfinance as yf
import os
from datetime import datetime, timedelta
import logging
from core.event import MarketEvent
from time import time

class DataStore:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.data = {}  # Contains current full data. key: symbol
        self.yfinance_objects = {}  # Contains instances of yf.Ticker
        self.data_for_market_event = pd.DataFrame(columns=[
            'Symbol',
            'Date',
            'Open',
            'High',
            'Low',
            'Close',
            'Volume',
            'MarketEvent' #Flag for whether marketevent has been already called
        ])
        self.data_for_market_event = self.data_for_market_event.set_index('Date')


    def _clear_data(self):
        self.data={}

    def _clear_data_for_market_event(self):
        self.data_for_market_event = pd.DataFrame(columns=[
            'Symbol',
            'Date',
            'Open',
            'High',
            'Low',
            'Close',
            'Volume',
            'MarketEvent' #Flag for whether marketevent has been already called
        ])
        self.data_for_market_event = self.data_for_market_event.set_index('Date')

    def read_csv(self, symbol, filename) -> bool:
        '''
        Read data from CSV file, for given ticker
        '''
        try:
            if not os.path.exists(filename):
                self.logger.info(f"File '{filename}' does not exist.")
                return False
            
            df = pd.read_csv(filename, parse_dates=['Date'])
            df = df.set_index('Date')
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
            try:
                self.yfinance_objects[symbol] = yf.Ticker(symbol)
                self.logger.info(f'Ticker created: {symbol}')
            except Exception as e:
                self.logger.error(f"Creating yfinance.Ticker failed: {e}")
                raise

        self.logger.info(start_date)
        self.logger.info(type(start_date))

        if isinstance(start_date,datetime):
            try:
                start_date = start_date.strftime("%Y-%m-%d")
            except Exception as e:
                self.logger.debug(f'Could not translate {start_date} to string')
        self.logger.info(start_date)
        self.logger.info(type(start_date))
        if isinstance(end_date,datetime):
            try:
                end_date = end_date.strftime("%Y-%m-%d")
            except Exception as e:
                self.logger.debug(f'Could not translate {end_date} to string')

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

    def _check_OHLCV_format(self,symbol) -> bool:
        if symbol not in self.data:
            self.logger.info(f'_check_OHLCV_format: Symbol {symbol} not in DataStore data.') 
            return False
        
        required_columns = {'Open', 'High', 'Low', 'Close', 'Volume'}
        data = self.data[symbol]

        if data.index.name != 'Date':
            self.logger.warning(f'Index for {symbol} is not Date')
            return False
        
        # Ensure index is datetime
        if not pd.api.types.is_datetime64_any_dtype(data.index):
            self.logger.warning(f"Index for {symbol} data is not datetime.")
            return False

        if data.empty:
            self.logger.info(f'_check_OHLCV_format: Data for {symbol} is empty.')
            return False
        
        return required_columns.issubset(data.columns)
    
    def get_price(self, symbol, current_time):
        """
        Returns the latest available price data (row) before or equal to current_time
        from self.data[symbol], or None if no valid data.
        Currently returns the close price when called. Might need enhancement
        """
        if symbol not in self.data:
            self.logger.warning(f"No data available for symbol: {symbol}")
            return None

        if self._check_OHLCV_format(symbol):
            return None

        df = self.data[symbol]

        # Filter all timestamps <= current_time
        valid_times = df.index[df.index <= pd.to_datetime(current_time)]
        if valid_times.empty:
            self.logger.info(f"No data before {current_time} for {symbol}.")
            return None

        # Get latest row before or at current_time
        closest_time = valid_times.max()

        #Check if line is OHLCV or just a single line
        line = df.loc[closest_time]
        return df.loc[closest_time]['Close']

    def create_data_for_eventqueue(self):
        self.logger.debug(f"Symbols in data: {list(self.data.keys())}")
        for symbol, data in self.data.items():
            # Only proceed if data format is correct (invert your logic)
            if not self._check_OHLCV_format(symbol):
                self.logger.warning(f"Data format check failed for symbol {symbol}")
                continue  # skip this symbol
            else:
                self.logger.info('Data format checking passed')
            
            #Important limitation!!!! Later need to be revised if more info is needed
            columns_to_copy = ['Open','High','Low','Close','Volume']
            copy_data = data.copy(columns_to_copy)
            
            # Add required columns for data_for_market_event
            copy_data['Symbol'] = symbol
            copy_data['MarketEvent'] = 0.0
            
            # Make sure index name is 'Date' for consistency
            if copy_data.index.name != 'Date':
                copy_data = copy_data.set_index('Date')
            
            # Append to existing DataFrame
            if self.data_for_market_event.empty and not copy_data.empty :
                self.data_for_market_event = copy_data
            else:
                self.data_for_market_event = pd.concat([self.data_for_market_event, copy_data])
        
        # Sort by index (Date) ascending
        self.data_for_market_event = self.data_for_market_event.sort_index()

    def has_next(self) -> bool:
        # Method for core engine to see if there is still unprocessed data that should go to market events.
        # Return false if data was not loaded.
        if self.data_for_market_event.empty:
            self.logger.debug('has_next: data_for_market_event is empty.')
            return False
        
        return self.data_for_market_event.iloc[-1]['MarketEvent'] == 0
    
    def get_next_event(self):
        time1 = time()
        next_item = self.data_for_market_event[self.data_for_market_event['MarketEvent'] == 0].iloc[0]
        # Create market event
        time2 = time()
        event = MarketEvent(
        timestamp = next_item.name,
        symbol = next_item['Symbol'],
        open = next_item['Open'],
        high = next_item['High'],
        low = next_item['Low'],
        close = next_item ['Close'],
        volume = next_item['Volume'])
        time3 = time()
        # Set flag in data_for_market_event that event was already created.
        index = self.data_for_market_event[self.data_for_market_event['MarketEvent']==0].index[0]
        self.data_for_market_event.loc[index,'MarketEvent'] = 1
        time4 = time()
        return [event, time2-time1, time3-time2, time4-time3]









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
