#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import psycopg
import sys
import os
import pandas as pd
from loguru import logger

# This function takes a gauge location type (COASTAL, TIDAL or RIVERS), and uses it to query the drf_gauge_station table, 
# and return a list of station id(s), and station names.
def getStationID(locationType):
    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
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
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes a input a directory path and outputFile, and used them to read the input file
# and add station_id(s) that are extracted from the drf_gauge_station table in theapsviz_gauges database.
def addMeta(ingestDir, inputDataSource, inputSourceName, inputSourceArchive, inputUnits, inputLocationType):
    df = getStationID(inputLocationType)

    df['data_source'] = inputDataSource
    df['source_name'] = inputSourceName
    df['source_archive'] = inputSourceArchive
    df['units'] = inputUnits

    # Drop station_name from DataFrame 
    df.drop(columns=['station_name'], inplace=True)

    # Reorder column name and update indeces 
    newColsOrder = ['station_id','data_source','source_name','source_archive','units']
    df=df.reindex(columns=newColsOrder)

    # Write dataframe to csv file 
    outputFile = 'source_'+inputSourceName+'_stationdata_'+inputSourceArchive+'_'+inputLocationType+'_'+inputDataSource+'_meta.csv'
    df.to_csv(ingestDir+outputFile, index=False, header=False)

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
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputUnits = args.inputUnits
    inputLocationType = args.inputLocationType

    logger.info('Start processing source data for data source '+inputDataSource+', with source name '+inputSourceName+', source archive '+inputSourceArchive+', and location type '+inputLocationType+'.')

    # Run addMeta function
    addMeta(ingestDir, inputDataSource, inputSourceName, inputSourceArchive, inputUnits, inputLocationType)
    logger.info('Finished processing source data for file data source '+inputDataSource+', with source name '+inputSourceName+', source archive '+inputSourceArchive+', and location type '+inputLocationType+'.')

# Run main function takes ingestDir, and outputFile as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--inputDataSource", help="Input data source name", action="store", dest="inputDataSource", choices=['namforecast_hsofs','nowcast_hsofs','namforecast_ec95d','nowcast_ec95d','tidal_gauge','tidal_predictions','ocean_buoy','coastal_gauge','river_gauge','wind_anemometer','air_barometer'], required=True)
    parser.add_argument("--inputSourceName", help="Input source name", action="store", dest="inputSourceName", choices=['adcirc','noaa','ndbc','ncem'], required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", choices=['noaa','ndbc','contrails','renci'], required=True) 
    parser.add_argument("--inputUnits", help="Input units", action="store", dest="inputUnits", required=True)
    parser.add_argument("--inputLocationType", help="Input location type", action="store", dest="inputLocationType", required=True)

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

