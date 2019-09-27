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
    
    # Get a list of all dates for which downloads were done
    year = datetime.now(tz).date().year 
    folder_date = []
    
    if os.path.exists('output/{}'.format(year)):
        foldernames = os.listdir('output/{}'.format(year))
        folder_CW = [foldername for foldername in foldernames if not foldername.startswith('.')]
        
        for i in range(0, len(folder_CW)):
            for foldername in os.listdir('output/{}/{}'.format(year, folder_CW[i])):
                if not foldername.startswith('.'):
                    folder_date += [foldername]
                
    # Get a list of all available dates on IEX side
    date_today = datetime.today().date()
    date = date_today - timedelta(weeks = 4) # 30 calendar days given
    date = date - timedelta(days = date.isoweekday()) # move to monday

    IEX_date = []
    while (date < date_today):
        if date.isoweekday() <= 5:
            IEX_date += [date.strftime('%Y%m%d')]
        date += timedelta(days = 1)
        
    # Get a list of all dates for which downloads are todo
    todo_dates = [date for date in IEX_date if date not in folder_date]

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
def get_csv(session, api_url, idx, ticker, path, date_str):
    
    with requests.Session() as session:
        response = session.get(api_url)

        if response.status_code == 200:
            data = response.content.decode('utf8')

            # Case when empty file is returned with success 200 response
            if data == '':
                csv = None
                write_csv(csv, idx, ticker, path, date_str, api_url, status = 'NONE')
            # Case when requested file is returned with success 200 response
            else: 
                csv = pd.read_csv(io.StringIO(data), sep = ',')
                write_csv(csv, idx, ticker, path, date_str, api_url)

        # Case when error status code is returned
        else:
            csv = None
            write_csv(csv, idx, ticker, path, date_str, api_url, status = 'ERROR')

    return(None)


# Function to write the downloaded data for 1 IEX ticker to file
def write_csv(csv, idx, ticker, path, date_str, api_url, status = None):
        
    # Check if file is empty
    if status == 'NONE':

        # Output file and print status (to keep track of missing tickers)
        with open('{}/NONE/NONE_{}_{}.txt'.format(path, ticker, date_str),'w+') as textfile:
            textfile.write(api_url)
        
        status = '{} | {} | {} | {} | NONE'.format(datetime.now(tz), date_str, idx, ticker)
        print(status + '\n' + api_url + '\n')
        logging.info(status + '\n' + api_url + '\n')

    # Check if status code was not 200
    elif status == 'ERROR':

        # Output file and print status (to keep track of missing tickers)
        with open('{}/ERROR/ERROR_{}_{}.txt'.format(path, ticker, date_str),'w+') as textfile:
            textfile.write(api_url)
        
        status = '{} | {} | {} | {} | ERROR'.format(datetime.now(tz), date_str, idx, ticker)
        print(status + '\n' + api_url + '\n')
        logging.info(status + '\n' + api_url + '\n')
        
    else:
        
        csv.to_csv('{}/DONE/{}_{}.csv'.format(path, ticker, date_str))
        status = '{} | {} | {} | {} \n'.format(datetime.now(tz), date_str, idx, ticker)
        print(status)
        logging.info(status)


# Function to prepare all parameters for the download session for 1 date
def asyncio_prep(date_str):
    
    base_url = 'https://cloud.iexapis.com/stable/stock/{}/chart/date/{}?format=csv&token={}&chartIEXOnly=true'

    path = get_path(date_str)
    tickers = get_tickers(path)
    api_urls = [base_url.format(ticker, date_str, os.environ['TOKEN']) for ticker in tickers]
    params = [path, tickers, api_urls, date_str]

    status = '\n{}\n# Download for: {}\n{}\n'.format(''.join(['#']*70), date_str, ''.join(['#']*70))
    print(status)
    logging.info(status)

    return(params)
    

# Asynchronous function to conduct the download session for 1 date
async def download_tickers_asynchronous(params):
    
    path = params[0]
    tickers = params[1]
    api_urls = params[2]
    date_str = params[3]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        with requests.Session() as session:
            
            loop = asyncio.get_event_loop()
            tasks = []
            for idx in range(0, len(api_urls)):
                tasks.append(loop.run_in_executor(executor, get_csv, *(session, api_urls[idx], idx, 
                                                                     tickers[idx], path, date_str)))
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
        path = params[0]
        with open('{}/terminated_{}.txt'.format(path, date),'w+') as textfile:
            textfile.write(date)

        # Zip the folder for the date
        shutil.make_archive(path, 'zip', path)
        shutil.rmtree(path)