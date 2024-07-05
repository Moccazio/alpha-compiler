"""
Custom Bundle for Loading the SEP daily stock dataset from Sharadar, from a dump.

Created by Peter Harrington (pbharrin) on 3/8/18.
Updated by Maximilan Wimmer (moccazio) on 5/7/24 
"""

import os
from os import environ as env

import pandas as pd
import numpy as np
from exchange_calendars import get_calendar
from zipline.utils.cli import maybe_show_progress
from contextlib import closing
import traceback

import pandas as pd
# from zipline.utils.calendars import get_calendar # zipline Quantopian 
from zipline.utils.calendar_utils import get_calendar # zipline-reloaded
import sys

#-------------------------------------------------------------------------
# Exchange Metadata (for country code mapping)
EXCHANGE_NAME = 'NYSE'
exchange_d = {'exchange': [EXCHANGE_NAME], 'canonical_name': [EXCHANGE_NAME], 'country_code': ['US']}

METADATA_HEADERS = ['start_date', 'end_date', 'auto_close_date',
                    'symbol', 'exchange', 'asset_name']

#-------------------------------------------------------------------------
# add functions 

def check_for_abnormal_returns(df, thresh=3.0):
    """Checks to see if any days have abnormal returns"""
    returns = df['close'].pct_change()
    abnormal_rets = returns[returns > thresh]
    if abnormal_rets.shape[0] > 0:
        sys.stderr.write('Abnormal returns for: {}\n'.format(df.iloc[0]['ticker']))
        sys.stderr.write('{}\n'.format(str(abnormal_rets)))
      

def from_sep_dump(file_name, start=None, end=None):
    """
    ticker,date,open,high,low,close,volume,closeadj,closeunadj,lastupdated

    To use this make your ~/.zipline/extension.py look similar this:

    from zipline.data.bundles import register
    from sep_sharadar import from_sep_dump

    register("sep",
         from_sep_dump("/path/to/your/SEP/dump/SHARADAR_SEP.csv"),)

    """
    # us_calendar = get_calendar("NYSE").all_sessions # zipline Quantopian 
    us_calendar = get_calendar("XNYS") # zipline-reloaded
    ticker2sid_map = {}

    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,  # unused
               daily_bar_writer,
               adjustment_writer,
               calendar,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=start,
               end=end):

        print("starting ingesting data from: {}".format(file_name))

        # read in the whole dump (will require ~7GB of RAM)
        df = pd.read_csv(file_name, index_col='date',
                         parse_dates=['date'], na_values=['NA'])

        # drop unused columns, dividends will be used later
        #df = df.drop(['lastupdated', 'dividends', 'closeunadj'], axis=1) # dividends moved to SHARADAR/ACTIONS 
        df = df.drop(['lastupdated', 'closeunadj', 'closeadj'], axis=1)
        # counter of valid securites, this will be our primary key
        sec_counter = 0
        data_list = []  # list to send to daily_bar_writer
        metadata_list = []  # list to send to asset_db_writer (metadata)

        # iterate over all the unique securities and pack data, and metadata
        # for writing
        for tkr, df_tkr in df.groupby('ticker'):
            df_tkr = df_tkr.sort_index()

            row0 = df_tkr.iloc[0]  # get metadata from row

            print(" preparing {}".format(row0["ticker"]))
            check_for_abnormal_returns(df_tkr)

            # check to see if there are missing dates in the middle
            # this_cal = us_calendar[(us_calendar >= df_tkr.index[0]) & (us_calendar <= df_tkr.index[-1])]  # zipline Quantopian 
            this_cal = us_calendar.sessions_in_range(df_tkr.index[0], df_tkr.index[-1]) # zipline-reloaded
          
            if len(this_cal) != df_tkr.shape[0]:
                print('MISSING interstitial dates for: %s using forward fill' % row0["ticker"])
                print('number of dates missing: {}'.format(len(this_cal) - df_tkr.shape[0]))
                df_desired = pd.DataFrame(index=this_cal.tz_localize(None))
                df_desired = df_desired.join(df_tkr)
                df_tkr = df_desired.fillna(method='ffill')

            # update metadata; 'start_date', 'end_date', 'auto_close_date',
            # 'symbol', 'exchange', 'asset_name'
            metadata_list.append((df_tkr.index[0],
                                  df_tkr.index[-1],
                                  df_tkr.index[-1] + pd.Timedelta(days=1),
                                  row0["ticker"],
                                  EXCHANGE_NAME,  # all have exchange = NYSE, even though this is not true
                                  row0["ticker"]
                                  )
                                 )

            # drop metadata columns
            df_tkr = df_tkr.drop(['ticker'], axis=1)

            # pack data to be written by daily_bar_writer
            data_list.append((sec_counter, df_tkr))
            ticker2sid_map[tkr] = sec_counter  # record the sid for use later
            sec_counter += 1

        print("writing data for {} securities".format(len(metadata_list)))
        daily_bar_writer.write(data_list, show_progress=False)

        # write metadata
        asset_db_writer.write(equities=pd.DataFrame(metadata_list,
                                                    columns=METADATA_HEADERS),
                              exchanges=pd.DataFrame(data=exchange_d))
        print("a total of {} securities were loaded into this bundle".format(
            sec_counter))

        # write adjustments
        # empty dataframe for splits
                 
        dfs = pd.DataFrame(columns=['sid', 'effective_date', 'ratio'], data=[[1, pd.to_datetime('2000-01-01'), 1.0]])
        adjustment_writer.write(splits=dfs)
        
    return ingest
