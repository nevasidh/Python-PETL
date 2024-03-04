import os
import sys
import petl
import pymssql
import petl as etl
import configparser
import requests
import datetime
import json
import decimal

# get data from Configuration file
config = configparser.ConfigParser()
try:
    config.read('Config/ETLDemo.ini')
except Exception as exp:
    print('Could not read configuration file : ' + str(exp))
    sys.exit()

# read settings from configuration ini file
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['URL']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']

# request data from URL
try:
    BOCResponse = requests.get(url + startDate + '')
except Exception as exp:
    print('Could not make request' + str(exp))
    sys.exit()

# initialize list of list for data storage for Dates and Rates
BOCDates = []
BOCRates = []

# check response and procesd Bank of Canada JSON object
if BOCResponse.status_code == 200:
    BOCRaw = json.loads(BOCResponse.text)

    # extract observations data into column arrays
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))

    # create petl table from two columns arrays and rename the columns
    exchangeRates = petl.fromcolumns([BOCDates, BOCRates], header=['date', 'rate'])

    # Load Expenses.xlsx document
    try:
        expenses = petl.io.xlsx.fromxlsx('Expenses.xlsx', sheet='Sheet1')
    except Exception as exp:
        print('Sorry! Could not load expenses file - ' + str(exp))
        sys.exit()

    # join the table
    expenses = petl.outerjoin(exchangeRates, expenses, key='date')

    # fill missing values
    expenses = petl.filldown(expenses, 'rate')

    # remove dates with no expenses
    expenses = petl.select(expenses, lambda rec: rec.USD != None)

    # add CDN Column
    expenses = petl.addfield(expenses, 'CAD', lambda rec: decimal.Decimal(rec.USD) * rec.rate)

    # initialize Database Connection
    try:
        dbConnection = pymssql.connect(server='localhost', user='sa', password='', database=destDatabase)
        print('Connected')
        print(expenses)
    except Exception as exp:
        print('Error connecting to SQL Server -' + str(exp))
        sys.exit()

    # populate Expenses database table
    try:
        petl.io.todb(expenses, dbConnection, 'Expenses')
    except Exception as exp:
        print('Error populating database table -' + str(exp))
        sys.exit()
