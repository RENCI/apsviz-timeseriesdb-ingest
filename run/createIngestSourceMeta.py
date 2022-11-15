#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import psycopg2
import sys
import os
import pandas as pd
from psycopg2.extensions import AsIs
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# This function takes a gauge location type (COASTAL, TIDAL or RIVERS), and uses it to query the drf_gauge_station table, 
# and return a list of station id(s), and station names.
def getStationID(locationType):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
        cur = conn.cursor()

        # Set enviromnent 
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query 
        cur.execute("""SELECT station_id, station_name FROM drf_gauge_station
                       WHERE location_type = %(location_type)s
                       ORDER BY station_name""", 
                    {'location_type': locationType})
       
        # convert query output to Pandas dataframe 
        df = pd.DataFrame(cur.fetchall(), columns=['station_id', 'station_name'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return Pandas dataframe
        return(df)

    # If exception log error
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)

# This function takes a input a directory path and outputFile, and used them to read the input file
# and add station_id(s) that are extracted from the drf_gauge_station table in theapsviz_gauges database.
def addMeta(ingestDir, outputFile):
    # Extract list of stations from dataframe for query database using the getStationID function
    locationType = outputFile.split('_')[3]

    df = getStationID(locationType)

    # Get source name from outputFilee
    source = outputFile.split('_')[0]

    # Check if source is ADCIRC, contrails or noaa, and make appropriate additions to DataFrame 
    if source == 'adcirc':
        # Get source_name and data_source from outputFile, and add them to the dataframe along
        # with the source_archive value
        df['data_source'] = outputFile.split('_')[4].lower()+'_'+outputFile.split('_')[5].lower()
        df['source_name'] = source
        df['source_archive'] = outputFile.split('_')[2].lower() 
        df['units'] = 'm'
    elif source == 'ncem':
        # Add data_source, source_name, and source_archive to dataframe
        location_type = outputFile.split('_')[3].lower()
        df['data_source'] = location_type+'_gauge'
        df['source_name'] = source
        df['source_archive'] = 'contrails'
        df['units'] = 'm'
    elif source == 'noaa':
        # Add data_source, source_name, and source_archive to dataframe
        gtype = outputFile.split('_')[5].lower()
        if gtype == 'gauge':
            df['data_source'] = 'tidal_gauge'
            df['units'] = 'm'
        elif gtype == 'predictions':
            df['data_source'] = 'tidal_predictions'
            df['units'] = 'm'
        elif gtype == 'barometer':
            df['data_source'] = 'air_barometer'
            df['units'] = 'mb'
        else:
            sys.exit(1)

        df['source_name'] = source
        df['source_archive'] = source

    elif source == 'ndbc':
        gtype = outputFile.split('_')[5].lower()
        if gtype == 'buoy':
            # Add data_source, source_name, and source_archive to dataframe
            df['data_source'] = 'ocean_buoy'
            df['units'] = 'm'
        elif gtype == 'anemometer':
            df['data_source'] = 'wind_anemometer'
            df['units'] = 'mps'
        else:
            sys.exit(1)

        df['source_name'] = source
        df['source_archive'] = source
    else:
        # If source in incorrect log message and exit
        sys.exit('Incorrect source')
 
    # Drop station_name from DataFrame 
    df.drop(columns=['station_name'], inplace=True)

    # Reorder column name and update indeces 
    newColsOrder = ['station_id','data_source','source_name','source_archive','units']
    df=df.reindex(columns=newColsOrder)

    # Write dataframe to csv file 
    df.to_csv(ingestDir+'source_'+outputFile, index=False, header=False)

# Main program function takes args as input, which contains the ingestDir, and outputFile values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createIngestSourceMeta.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    ingestDir = os.path.join(args.ingestDir, '')
    outputFile = args.outputFile

    logger.info('Start processing source data for file '+outputFile+'.')

    # Run addMeta function
    addMeta(ingestDir, outputFile)
    logger.info('Finished processing source data for file '+outputFile+'.')

# Run main function takes ingestDir, and outputFile as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--outputFile", action="store", help="Output file name", dest="outputFile", required=True)

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

