#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from concurrent.futures import ThreadPoolExecutor
import requests
import asyncio

from datetime import datetime, timedelta
import time
import pytz

import pandas as pd
import logging
import shutil
import json
import io
import os

# --------------------------------------------------------------------------------------------------
# Function Definitions
# --------------------------------------------------------------------------------------------------

# Function to initialize the logging file
def init_logging():
    
    global tz
    tz = pytz.timezone('Europe/Berlin') 
    if not os.path.exists('log'):
        os.makedirs('log')

    date = datetime.now(tz).date().strftime('%Y%m%d')
    logging.basicConfig(filename='log/{}.log'.format(date), level=logging.INFO)

    return(None)


# Function to get a list of all dates that are to be downloaded
def get_dates():
    
    # Get earliest date available on IEX side
    date_today = datetime.today().date()
    IEX_date = date_today - timedelta(days = 30) # 30 trailing calendar days acc. to IEX docu

    # Get last date for which download was done to get start date
    try:
        folder_date = []
        year = max([name for name in os.listdir('output/') if not name.startswith('.')])
        foldernames = os.listdir('output/{}'.format(year))
        folder_CW = [name for name in foldernames if not name.startswith('.')]

        for i in range(len(folder_CW)):
            foldernames = os.listdir('output/{}/{}'.format(year, folder_CW[i]))
            folder_date += [name for name in foldernames if not name.startswith('.')]
        last_date = datetime.strptime(max(folder_date)[0:8], '%Y%m%d').date()

        start_date = max(IEX_date, last_date + timedelta(days=1))
    except:
        start_date = IEX_date

    # Get todo dates with weekends filtered out
    dates = [start_date + timedelta(days=i) for i in range((date_today - start_date).days)]
    todo_dates = [date.strftime('%Y%m%d') for date in dates if date.isoweekday() <= 5]

    return(todo_dates)


# Function to get and create a path for the current date
def get_path(date_str):
    
    # Extract year and CW from the date
    date = datetime.strptime(date_str, '%Y%m%d')
    year = str(date.year)
    CW = str(date.isocalendar()[1]).rjust(2, '0')
    
    # Check if there exists a folder for this date 
    path = 'output/{}/{}-CW{}/{}'.format(year, year, CW, date_str)
    if not os.path.exists(path):
        os.makedirs(path)
        os.mkdir('{}/NONE/'.format(path))
        os.mkdir('{}/ERROR/'.format(path))
        os.mkdir('{}/DONE/'.format(path))
    
    return(path)


# Function for downloading a list of all enabled IEX tickers 
def get_tickers(path):
    
    base_url = 'https://cloud.iexapis.com/stable/ref-data/iex/symbols?format=csv&token={}'
    df = pd.read_csv(base_url.format(os.environ['TOKEN']))
    df.to_csv('{}/tickers_all.csv'.format(path))
    tickers = df[~df['isEnabled'] == False].loc[:, 'symbol'].tolist()

    return(tickers)


# Function for downloading the data for 1 IEX ticker
def get_csv(session, idx, date_str, path, ticker, api_url):
    
    with requests.Session() as session:
        response = session.get(api_url)

        if response.status_code == 200:
            raw_data = response.content.decode('utf8')

            # Case when empty file is returned with success 200 response
            if raw_data == '':
                data = None
                write_csv(data, idx, ticker, path, date_str, api_url, status = 'NONE')
            # Case when requested file is returned with success 200 response
            else: 
                data = pd.read_csv(io.StringIO(raw_data), sep = ',')
                write_csv(data, idx, ticker, path, date_str, api_url, status = 'DONE')

        # Case when error status code is returned
        else:
            data = response.status_code
            write_csv(data, idx, date_str, path, ticker, api_url, status = 'ERROR')

    return(None)


# Function to write the downloaded data for 1 IEX ticker to file
def write_csv(data, idx, date_str, path, ticker, api_url, status):
        
    # Case when empty file is returned with success 200 response
    if status == 'NONE':
        with open('{}/NONE/NONE_{}_{}.txt'.format(path, ticker, date_str),'w+') as textfile:
            textfile.write(api_url)
        status = '{} | {} | {} | {} | NONE'.format(datetime.now(tz), date_str, idx, ticker)
        print(status + '\n' + api_url + '\n')
        logging.info(status + '\n' + api_url + '\n')

    # Case when requested file is returned with success 200 response
    elif status == 'DONE':
        data.to_csv('{}/DONE/{}_{}.csv'.format(path, ticker, date_str))
        status = '{} | {} | {} | {} \n'.format(datetime.now(tz), date_str, idx, ticker)
        print(status)
        logging.info(status)

    # Case when error status code is returned
    elif status == 'ERROR':
        with open('{}/ERROR/ERROR_{}_{}.txt'.format(path, ticker, date_str),'w+') as textfile:
            textfile.write("status_code: {}\n".format(data))
            textfile.write(api_url)
        status = '{} | {} | {} | {} | ERROR {}'.format(datetime.now(tz), date_str, idx, ticker, data)
        print(status + '\n' + api_url + '\n')
        logging.info(status + '\n' + api_url + '\n')
        

# Function to prepare all parameters for the download session for 1 date
def asyncio_prep(date_str):
    
    base_url = 'https://cloud.iexapis.com/stable/stock/{}/chart/date/{}?format=csv&token={}&chartIEXOnly=true'

    path = get_path(date_str)
    tickers = get_tickers(path)
    api_urls = [base_url.format(ticker, date_str, os.environ['TOKEN']) for ticker in tickers]
    params = {'date_str': date_str, 'path': path, 'tickers': tickers, 'api_urls': api_urls}

    status = '\n{}\n# Download for: {}\n{}\n'.format(''.join(['#']*70), date_str, ''.join(['#']*70))
    print(status)
    logging.info(status)

    return(params)
    

# Asynchronous function to conduct the download session for 1 date
async def download_tickers_asynchronous(params):
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        with requests.Session() as session:
            
            loop = asyncio.get_event_loop()
            tasks = []
            for idx in range(len(params['tickers'])):
                tasks.append(loop.run_in_executor(executor, get_csv, 
                                                  *(session, idx, params['date_str'], params['path'], 
                                                  params['tickers'][idx],  params['api_urls'][idx])))
                tasks.append(await asyncio.sleep(0.01)) 

    return(None)
                  
# --------------------------------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------------------------------

# Conduct an asynchronous download session for each date
if __name__ == '__main__':

    with open('config.json') as config_file:
        data = json.load(config_file)
        os.environ['TOKEN'] = data['TOKEN']

    init_logging()
    todo_dates = get_dates()

    for date in todo_dates:

        # Conduct download
        params = asyncio_prep(date)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(download_tickers_asynchronous(params))

        # Add text file to indicate termination
        with open('{}/terminated_{}.txt'.format(params['path'], date),'w+') as textfile:
            textfile.write(date)

        # Zip the folder for the date
        shutil.make_archive(params['path'], 'zip', params['path'])
        shutil.rmtree(params['path'])