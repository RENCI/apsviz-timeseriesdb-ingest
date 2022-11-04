#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import os
import sys
import glob
import re
import psycopg2
import pandas as pd
import numpy as np
from psycopg2.extensions import AsIs
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# This function takes a dataset name as input, and uses it to query the drf_harvest_data_file_met table, creating a list
# of filenames. The list is converted to a DataFrame and returned.
def getInputFiles(inputDataSource, inputSourceName, inputSourceArchive):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT dir_path, file_name 
                       FROM drf_harvest_data_file_meta 
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                       source_archive = %(sourcearchive)s AND ingested = False
                       ORDER BY data_date_time""",
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive})

        # convert query output to Pandas DataFrame
        df = pd.DataFrame(cur.fetchall(), columns=['dir_path','file_name'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return Pandas dataframe
        #if inputSourceName == 'adcirc':
        #    return(df.head(100))
        #else:  
        #    return(df.head(50))
        return(df)

    # If exception log error
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)

# This function takes as input the data_source (hsofs...), and a list of station_id(s), and returns source_id(s) for    
# model data from the drf_gauge_source table in the apsviz_gauges database. This funciton specifically gets source_id(s) for
# model data, such as from ADCIRC. The data_source, such is hsofs, is the grid that is used in the ADCIRC run.
def getSourceID(inputDataSource, inputSourceName, inputSourceArchive, station_tuples):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
        cur = conn.cursor()

        # Set enviromnent 
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT s.source_id AS source_id, g.station_id AS station_id, g.station_name AS station_name,
                       s.data_source AS data_source, s.source_name AS source_name, s.source_archive AS source_archive
                       FROM drf_gauge_station g INNER JOIN drf_gauge_source s ON s.station_id=g.station_id
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                       source_archive = %(sourcearchive)s AND station_name IN %(stationtuples)s
                       ORDER BY station_name""",
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 'stationtuples': AsIs(station_tuples)})

        # convert query output to Pandas dataframe
        dfstations = pd.DataFrame(cur.fetchall(), columns=['source_id','station_id','station_name','data_source','source_name','source_archive'])
   
        # Close cursor and database connection 
        cur.close()
        conn.close()

        # Return Pandas dataframe 
        return(dfstations)

    # If exception log error
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)

# This function takes as input a directory input path, directory output path and a filename, and returns a csv 
# file that containes gauge data. the function uses the getObsSourceID and getModelSourceID functions above to get
# a list of existing source ids that it includes in the gauge data to enable joining the gauge data table with 
# gauge source table. The function adds a timemark, that it gets from the input file name. The timemark values can
# be used to uniquely query an ADCIRC  model run.
def addMeta(harvestDir, ingestDir, inputFile, inputDataSource, inputSourceName, inputSourceArchive):
    # Read input file, convert column name to lower case, rename station column to station_name, convert its data 
    # type to string, and add timemark and source_id columns
    df = pd.read_csv(harvestDir+inputFile)
    df.columns= df.columns.str.lower()
    df = df.rename(columns={'station': 'station_name'})
    df = df.astype({"station_name": str})
    df.insert(0,'timemark', '')
    df.insert(0,'source_id', '')
   
    # Extract list of stations from dataframe for querying the database, and get source_archive name from filename.
    station_tuples = tuple(sorted([str(x) for x in df['station_name'].unique().tolist()]))

    # Run getSourceID function to get the source_id(s)
    dfstations = getSourceID(inputDataSource, inputSourceName, inputSourceArchive, station_tuples)
 
    # Get the timemark frome the the data filename 
    datetimes = re.findall(r'(\d+-\d+-\d+T\d+:\d+:\d+)',inputFile)
    if re.search('forecast', inputDataSource):
        # If the inputDataSource has forecast in its name get the second datetime in the filename
        df['timemark'] = datetimes[1]
    elif re.search('nowcast', inputDataSource):
        # If the inputDataSource has nowcast in its name get the third datetime in the filename
        df['timemark'] = datetimes[2]
    else:
        # If the inputDataSource does not have forecast or  nowcast in its name get the first datetime in the filename
        df['timemark'] = datetimes[0] 

    # Add source id(s) to dataframe 
    for index, row in dfstations.iterrows():
        df.loc[df['station_name'] == row['station_name'], 'source_id'] = row['source_id']

    # Drom station_name column from dataframe
    df.drop(columns=['station_name'], inplace=True)

    # Write dataframe to csv file
    df.to_csv(ingestDir+'data_copy_'+inputFile, index=False)

# This function takes as input a directory input path, a directory output path and a dataset variable. It 
# generates and list of input filenames, and uses them to run the addMeta function above.
def processData(ingestDir, inputDataSource, inputSourceName, inputSourceArchive):
    dfDirFiles = getInputFiles(inputDataSource, inputSourceName, inputSourceArchive) 
 
    for index, row in dfDirFiles.iterrows():
        harvestDir = row[0]
        inputFile = row[1] 

        addMeta(harvestDir, ingestDir, inputFile, inputDataSource, inputSourceName, inputSourceArchive)

# Main program function takes args as input, which contains the  ingestDir, inputDataSource, inputSourceName, and inputSourceArchive values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createIngestData.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    ingestDir = os.path.join(args.ingestDir, '')
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive

    logger.info('Start processing data from data source '+inputDataSource+', with source name '+inputSourceName+', from source archive '+inputSourceArchive+'.')
    processData(ingestDir, inputDataSource, inputSourceName, inputSourceArchive) 
    logger.info('Finished processing data from data source '+inputDataSource+', with source name '+inputSourceName+', from source archive '+inputSourceArchive+'.')

# Run main function takes ingestDir, inputDataSource, inputSourceName, inputSourceArchiv as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--inputDataSource", help="Input data source name", action="store", dest="inputDataSource", choices=['namforecast_hsofs','namforecast_ec95d','nowcast_hsofs','nowcast_ec95d','tidal_gauge','tidal_predictions', 'air_barometer','ocean_buoy','wind_anemometer','coastal_gauge','river_gauge'], required=True)
    parser.add_argument("--inputSourceName", help="Input source name", action="store", dest="inputSourceName", choices=['adcirc','noaa','ndbc','ncem'], required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", choices=['noaa','ndbc','contrails','renci'], required=True)

    args = parser.parse_args()
    main(args)


