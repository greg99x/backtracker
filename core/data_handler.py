import pandas as pd
import yfinance as yf
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime, timedelta
import logging
from core.event import MarketEvent
from core.core import EventQueue
from time import time
import numpy as np
import pandera.pandas as pa
from pandera.pandas import Column
from functools import wraps

class DataValidators:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        # Define a fixed OHLCV schema
        self.ohlcv_schema = pa.DataFrameSchema(
            {
                'Symbol': Column(pa.String),
                'Open': Column(pa.Float),
                'High': Column(pa.Float),
                'Low': Column(pa.Float),
                'Close': Column(pa.Float),
                'Volume': Column(pa.Float,nullable=True,coerce=True),
                'Dividend': Column(pa.Float, nullable=True, coerce=True),
                'StockSplit': Column(pa.Float, nullable=True,coerce=True),
            },
            index=pa.Index(pa.DateTime, name="Date",coerce=True)
        )

    def ohlcv_validate(self, df: pd.DataFrame | None, lazy=True) -> bool:
        """
        Method to validate a DataFrame using the fixed OHLCV schema.

        Args:
            coerce (bool): Coerce dtypes
            lazy (bool): Report all schema errors
        """
        if df is None:
            return False
        
        if not isinstance(df, pd.DataFrame):
            self.logger.error(f"Validation error: Input is not a pandas DataFrame but {type(df)}")
            return False
        try:
            self.ohlcv_schema.validate(df, lazy=lazy)
        except pa.errors.SchemaErrors as err:
            self.logger.error(f'Error in ohlcv validation: {err}')
            return False
        return True

class Csvio:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def read_csv(self, filename: str) -> pd.DataFrame | None:
        '''read file to CSV with optional logging'''
        try:
            if not os.path.exists(filename):
                self.logger.error(f"File '{filename}' does not exist.")
                return None
            
            df = pd.read_csv(filename, parse_dates=['Date'])
            df = df.set_index('Date')
            return df
        except Exception as e:
            self.logger.error(f"Error reading CSV: {e}")
            return None

    def write_csv(self, df: pd.DataFrame, filename: str, log=True) -> bool:
        '''Write CSV file, from data dict for given symbol to file 'filename'''
        try:
            df.to_csv(filename)
            return True
        except Exception as e:
            if log:
                self.logger.error(f"Error writing CSV for {filename}: {e}")
            return False

class DataStore:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.validator = DataValidators(logger=self.logger)
        '''
        Data format required:

        | Column      | Type             | Description               |
        |-------------|------------------|---------------------------|
        | Date (index)| datetime64[ns]   | Index, must be sorted     |
        | Symbol      | string           | Symbol ticker of asset    |
        | Open        | float64          | Opening price             |
        | High        | float64          | Daily high                |
        | Low         | float64          | Daily low                 |
        | Close       | float64          | Closing price             |
        | Volume      | float64          | Trading volume            |
        | Dividens    | float64          | Paid dividens             |
        | Stocksplit  | float64          | Stocksplit                |

        self.data:
        Dict that stores pandas frames with market data
        key: symbol: str
        value: data: PandasFrame indexed by date
        This is used to interface with I/O modules, yfinance API etc.
        '''
        self.data = {}

        '''
        Used for reducing boilerplate later, to easily create DataFrames
        set_index(Date) must be used when using this template for empty frames 
        '''
        self.ohlcv_column_dtypes = {
        'Symbol': 'string',
        'Date': 'datetime64[ns]',
        'Open': 'float64',
        'High': 'float64',
        'Low': 'float64',
        'Close': 'float64',
        'Volume': 'float64',
        'Dividend': 'float64',
        'StockSplit': 'float64'}
        
    def _create_empty_OHLCV_frame(self) -> pd.DataFrame | None:
        df = pd.DataFrame({col: pd.Series(dtype=dt) for col, dt in self.ohlcv_column_dtypes.items()})
        df = df.set_index('Date')
        typecheck = self.validator.ohlcv_validate(df)
        if typecheck:
            return df
        else:
            return None

    def write_data(self,symbol: str, data: pd.DataFrame) -> None:
        '''Interface method to update data in DataStore by outside modules'''
        typecheck = self.validator.ohlcv_validate(data)
        if typecheck:
            self.data[symbol] = data
        else:
            return None
        
    def append_data(self,symbol: str, data: pd.DataFrame) -> None:
        '''
        Interface method to append new data in DataStore by outside module
        Duplicates are dropped, and new data overwrites old data
        '''
        data_symbol = data['Symbol'].iloc[0]
        if data_symbol != symbol:
            self.logger.warning(f'New data symbol dont match arg symbol: {symbol},{data_symbol}')
            return None
        typecheck = self.validator.ohlcv_validate(data)
        if typecheck:
            df_combined = pd.concat([self.data[symbol],data])
            df_duplicates_removed = df_combined[~df_combined.index.duplicated(keep='last')]
            self.data[symbol] = df_duplicates_removed
        else:
            return None
        
    def clear_symbol_data(self,symbol: str) -> None:
        if symbol in self.data:
            self.logger.info(f'Data cleared for {symbol}')
            del self.data[symbol]

    def get_closest_price_dummy(self,symbol: str, current_time: datetime) -> float | None:
        """
        Returns the latest available price data (row) before or equal to current_time
        from self.data[symbol], or None if no valid data.
        Currently returns the close price when called. Needs much more complex behaviour!
        """
        if symbol not in self.data:
            self.logger.warning(f"No data available for symbol: {symbol}")
            return None

        df = self.data[symbol]

        # Filter all timestamps <= current_time
        valid_times = df.index[df.index <= pd.to_datetime(current_time)]
        if valid_times.empty:
            self.logger.warning(f"No data before {current_time} for {symbol}.")
            return None

        # Get latest row before or at current_time
        closest_time = valid_times.max()

        return df.loc[closest_time]['Close']
    
    def get_all_symbol_data(self,symbol: str) -> pd.DataFrame | None:
        '''Interface method to get all available data for given symbol'''
        if symbol not in self.data.keys():
            return None
        return self.data[symbol]

    def get_last_time(self,symbol: str) -> datetime | None:
        '''Interface method to get the final time available for a given symbol'''
        if symbol in self.data:
            return self.data[symbol].index.max()
        else:
            return None

    def get_symbol_list(self) -> list:
        return list(self.data.keys())

class YfInterface:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        """
        Storage for instances of yfinance.Ticker objects, so that they don't need instanciation every time.
        """
        self.yfinance_objects = {}

    def fetch_data(self, symbol: str, start_date: datetime, end_date: datetime, interval='1d') -> pd.DataFrame | None:
        '''
        Wrapper function for yf.Ticker.history calls
        Return pd.DataFrame with downloaded history
        For now, only tested to work on time interval '1d'
        '''
        if symbol not in self.yfinance_objects:
            try:
                self.yfinance_objects[symbol] = yf.Ticker(symbol)
            except Exception as e:
                self.logger.warning(f"Creating yfinance.Ticker failed: {e}")
                return None

        if isinstance(start_date,datetime):
            try:
                start_date = start_date.strftime("%Y-%m-%d")
            except Exception as e:
                self.logger.warning(f'YFinance fetch: Could not translate {start_date} to string')
                return None

        if isinstance(end_date,datetime):
            try:
                end_date = end_date.strftime("%Y-%m-%d")
            except Exception as e:
                self.logger.warning(f'YFinance fetch: Could not translate {end_date} to string')
                return None

        try:
            df = self.yfinance_objects[symbol].history(
                start=start_date,
                end=end_date,
                interval=interval
            )
            self.logger.info(f'YfInterface downloaded data for: {symbol}')
            self.logger.info(f'Yfinterface downloaded data with shape: {df.shape}')
        except Exception as e:
            self.logger.warning(f"Error fetching data for {symbol}: {e}")
            return None
        df = self.comply_column_names(df)
        return df

    def fetch_max_data(self,symbol: str, interval='1d') -> pd.DataFrame | None:
        '''
        Wrapper function for yf.Ticker.history calls with max as period
        Return pd.DataFrame with downloaded history
        For now, only tested to work on time interval '1d'
        '''
        if symbol not in self.yfinance_objects:
            try:
                self.yfinance_objects[symbol] = yf.Ticker(symbol)
            except Exception as e:
                self.logger.warning(f"Creating yfinance.Ticker failed: {e}")
                return None
        try:
            df = self.yfinance_objects[symbol].history(
                period='max',
                interval=interval
            )
        except Exception as e:
            self.logger.warning(f"Error fetching data for {symbol}: {e}")
            return None
        df = self.comply_column_names(df)
        return df

    def comply_column_names(self,df: pd.DataFrame) -> None:
        '''YFinance column names sometimes don't match expected column name. this helps comply'''
        if 'Stock Splits' in df.columns:
            df.rename(columns={'Stock Splits': 'StockSplit'}, inplace=True)
        if 'Dividends' in df.columns:
            df.rename(columns={'Dividends': 'Dividend'}, inplace=True)
        return df


class DataHandler:
    def __init__(self, eventqueue, logger=None):
        self.eventqueue = eventqueue
        self.logger = logger or logging.getLogger(__name__)
        self.validator = DataValidators(logger=self.logger)
        self.datastore = DataStore(logger=self.logger)
        self.csvio = Csvio(logger=self.logger)
        self.yfinterface = YfInterface(logger=self.logger)

    def read_csv(self, symbol: str, filename: str, log=True) -> None:
        df = self.csvio.read_csv(filename)
        
        if df is None:
            self.logger.info(f'No data to write to CSV: {symbol}')
            return None
        
        typecheck = self.validator.ohlcv_validate(df)
        if not typecheck:
            return None
        
        self.datastore.write_data(symbol,df)
        if log:
            self.logger.info(f'Read data with shape: {df.shape}')
            self.logger.info(f'Reader: Last date in date: {df.index.max()}')
    
    def write_csv(self,symbol: str, filename: str, log=True) -> None:

        df = self.datastore.get_all_symbol_data(symbol)

        if df is None:
            return None
        
        typecheck = self.validator.ohlcv_validate(df)
        if not typecheck:
            return None
        
        self.csvio.write_csv(df,filename)
        if log:
            self.logger.info(f'Wrote data with shape: {df.shape}')
            self.logger.info(f'Writer: Last date in data: {df.index.max()}')

    def write_symbol_data(self,symbol: str, data: pd.DataFrame) -> None:
        self.datastore.write_data(symbol,data)
        
    def fetch_yf_data(self,symbol: str, start_date: datetime, end_date: datetime, interval='1d',redownload_timedelta=-1) -> None:
        if end_date > datetime.now():
            # end_date can't be in the future
            return None
        
        last_time = self.datastore.get_last_time(symbol)

        if last_time is not None:
            start_date = last_time + timedelta(days=redownload_timedelta)

        df = self.yfinterface.fetch_data(symbol,start_date,end_date)
        if 'Symbol' not in df.columns:
            df['Symbol'] = symbol

        typecheck = self.validator.ohlcv_validate(df)
        if not typecheck:
            self.logger.warning('DataHandler.fetch_yf_data Typecheck failed')
            return None
        
        if symbol not in self.datastore.get_symbol_list():
            self.datastore.write_data(symbol,df)
        else:
            self.datastore.append_data(symbol,df)
    
    def create_market_event(self,index: datetime, next_item: pd.Series) -> MarketEvent:
        event = MarketEvent(
        timestamp = index,
        symbol = next_item['Symbol'],
        open = next_item['Open'],
        high = next_item['High'],
        low = next_item['Low'],
        close = next_item ['Close'],
        volume = next_item['Volume'])
        return event

    def create_event_queue_lazy(self) -> None:
        symbols = self.datastore.get_symbol_list()
        eventqueue_dataframe = self.datastore._create_empty_OHLCV_frame()
        for symbol in symbols:
            df = self.datastore.get_all_symbol_data(symbol)
            typecheck = self.validator.ohlcv_validate(df)
            if not typecheck:
                return None
            if eventqueue_dataframe.empty:
                eventqueue_dataframe = df
            else:
                eventqueue_dataframe = pd.concat([eventqueue_dataframe,df])
        
        eventqueue_dataframe = eventqueue_dataframe.sort_index(ascending=False)
        assert eventqueue_dataframe.index.is_monotonic_decreasing
        for index, row in eventqueue_dataframe.iterrows():
            event = self.create_market_event(index,row)
            self.eventqueue.put(event)

    def clear_symbol_data(self,symbol: str) -> None:
        self.datastore.clear_symbol_data(symbol)

    def get_price(self,symbol: str, time: datetime) -> float:
        return self.datastore.get_closest_price_dummy(symbol,time)

if __name__ == '__main__':
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)  # Set minimum level to DEBUG
    eventqueue = EventQueue()
    core = DataHandler(eventqueue,logger=logger)
    core.read_csv('BTC-USD',r'C:\backtester\dev\test.csv',log=True)
    df = core.datastore.get_all_symbol_data('BTC-USD')
    start = time()
    core.create_event_queue_lazy()
    print(time()-start)
    event = eventqueue.get()
    print(event.type,event.timestamp,event.symbol,event.price,event.open,event.high,event.low,event.close, event.volume)
    print(eventqueue.size())
    print(core.datastore.get_last_time('BTC-USD'))