#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import argparse
import glob
import os
import sys
import psycopg
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# This function is used to delete duplicate records in the observation data. The observation data has duplicate records with the 
# same timestamp, but different timemarks because they are from different harvest data files.
def deleteDuplicateTimes(inputDataSource, inputSourceName, inputSourceArchive, minTime, maxTime):
    try:
        with psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'],
                             host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'],
                             password=os.environ['SQL_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()
    
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
   
            cur.execute("""DELETE FROM
                               drf_gauge_data a
                                   USING drf_gauge_data b,
                                         drf_gauge_source s
                           WHERE
                               s.data_source = %(datasource)s AND s.source_name = %(sourcename)s AND s.source_archive = %(sourcearchive)s AND
                               a.time >= %(mintime)s AND a.time <= %(maxtime)s AND
                               s.source_id=b.source_id AND
                               a.obs_id < b.obs_id AND
                               a.time = b.time AND
                               s.source_id=a.source_id""",
                        {'datasource': inputDataSource,'sourcename': inputSourceName,'sourcearchive': inputSourceArchive, 'mintime': minTime, 'maxtime': maxTime})

            cur.close()
            conn.close()

    except (Exception, psycopg.DatabaseError) as error:
        print(error)

# This function takes data source, source name, and source archive as input. It ingest these variables into the source meta table (drf_source_meta).
# The variables in this table are then used as inputs in runIngest.py 
def ingestSourceMeta(inputDataSource, inputSourceName, inputSourceArchive, inputLocationType):
    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            # Run query
            cur.execute("""INSERT INTO drf_source_meta(data_source, source_name, source_archive, location_type)
                           VALUES (%(datasource)s, %(sourcename)s, %(sourcearchive)s, %(locationtype)s)""",
                        {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 'locationtype': inputLocationType})

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes an input directory and an ingest directory as input. The input directory is used to search for geom 
# station files that are to be ingested. The ingest directory is used to define the path of the file to be ingested. The 
# ingest directory is the directory path in the apsviz-timeseriesdb database container.
def ingestStations(ingestDir):
    # Create list of geom files, to be ingested by searching the input directory for geom files.
    inputFiles = glob.glob(ingestDir+"stations/geom_*.csv")

    # Define the ingest path and file using the ingest directory and the geom file name

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            # Loop thru geom file list, ingesting each one
            for geomFile in inputFiles:
                # Run query
                with open(geomFile, "r") as f:
                    with cur.copy("COPY drf_gauge_station (station_name,lat,lon,tz,gauge_owner,location_name,location_type,country,state,county,geom) FROM STDIN WITH (FORMAT CSV)") as copy:
                        while data := f.read(100):
                            copy.write(data)

                # Remove station data file after ingesting it.
                logger.info('Remove station data file: '+geomFile+' after ingesting it')
                os.remove(geomFile)

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes an input directory and ingest directory as input. It uses the input directory to search for source  
# csv files, that were created by the createIngestSourceMeta.py program. It uses the ingest directory to define the path
# of the file that is to be ingested. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
def ingestSourceData(ingestDir):
    # Create list of source files, to be ingested by searching the input directory for source files.
    inputFiles = glob.glob(ingestDir+"source_*.csv")

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            # Loop thru source file list, ingesting each one
            for sourceFile in inputFiles:
                # Run ingest query
                with open(sourceFile, "r") as f:
                    with cur.copy("COPY drf_gauge_source (station_id,data_source,source_name,source_archive,units) FROM STDIN WITH (FORMAT CSV)") as copy:
                        while data := f.read(100):
                            copy.write(data)

                # Remove source data file after ingesting it.
                logger.info('Remove source data file: '+sourceFile+' after ingesting it')
                os.remove(sourceFile)

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes an dataset name as input and uses it to query the drf_harvest_data_file_meta table,
# creating a DataFrame that contains a list of data files to ingest. The ingest directory is the directory
# path in the apsviz-timeseriesdb database container.
def getHarvestDataFileMeta(inputDataSource, inputSourceName, inputSourceArchive):
    try:
        # Create connection to database, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD']) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            # Run query
            cur.execute("""SELECT dir_path, file_name
                           FROM drf_harvest_data_file_meta
                           WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                           source_archive = %(sourcearchive)s AND ingested = False
                           ORDER BY data_date_time""",
                        {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive})

            # convert query output to Pandas dataframe
            df = pd.DataFrame(cur.fetchall(), columns=['dir_path','file_name'])
 
            # Close cursor and database connection
            cur.close()
            conn.close()

            # Return Pandas dataframe
            #if inputSourceName == 'adcirc':
            #    # Limit to 100 files at a time
            #    return(df.head(100))
            #else:
            #    # Limit to 50 files at a time
            #    return(df.head(50))
            return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes an input directory and ingest directory as input. It uses the input directory to seach for
# harvest_files that need to be ingested. It uses the ingest directory to define the path of the harvest_file
# to ingesting. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
def ingestHarvestDataFileMeta(ingestDir):
    inputFiles = glob.glob(ingestDir+"harvest_files_*.csv")

    try:
        # Create connection to databaseset, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            for infoFile in inputFiles:
                # Run ingest query
                with open(infoFile, "r") as f:
                    with cur.copy("COPY drf_harvest_data_file_meta (dir_path,file_name,data_date_time,data_begin_time,data_end_time,data_source,source_name,source_archive,ingested,overlap_past_file_date_time) FROM STDIN WITH (FORMAT CSV)") as copy:
                        while data := f.read(100):
                            copy.write(data)

                # Remove harvest meta file after ingesting it.
                logger.info('Remove harvest meta file: '+infoFile+' after ingesting it')
                os.remove(infoFile)

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes an ingest directory and input dataset as input, and uses them to run the getHarvestDataFileMeta
# function. The getHarvestDataFileMeta function produces a DataFrame (dfDirFiles) 
# that contains a list of data files, that are queried from the drf_harvest_data_file_meta table. These files are then 
# ingested into the drf_gauge_data table. After the data has been ingested, from a file, the column "ingested", in the 
# drf_harvest_data_file_meta table, is updated from False to True. The ingest directory is the directory path in the 
# apsviz-timeseriesdb database container.
def ingestData(ingestDir, databaseDir, inputDataSource, inputSourceName, inputSourceArchive):
    # Get DataFrame the contains list of data files that need to be ingested
    dfDirFiles = getHarvestDataFileMeta(inputDataSource, inputSourceName, inputSourceArchive)

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            # Loop thru DataFrame ingesting each data file
            for index, row in dfDirFiles.iterrows():
                ingestFile = row[1]
                ingestPathFile = ingestDir+'data_copy_'+ingestFile


                if inputDataSource == 'air_barometer':
                    # Run ingest query
                    with open(ingestPathFile, "r") as f:
                        with cur.copy("COPY drf_gauge_data (source_id,timemark,time,air_pressure) FROM STDIN WITH (FORMAT CSV)") as copy:
                            while data := f.read(100):
                                copy.write(data)

                elif inputDataSource == 'wind_anemometer':
                    # Run ingest query
                    with open(ingestPathFile, "r") as f:
                        with cur.copy("COPY drf_gauge_data (source_id,timemark,time,wind_speed) FROM STDIN WITH (FORMAT CSV)") as copy:
                            while data := f.read(100):
                                copy.write(data)

                else:
                    # Run ingest query
                    with open(ingestPathFile, "r") as f:
                        with cur.copy("COPY drf_gauge_data (source_id,timemark,time,water_level) FROM STDIN WITH (FORMAT CSV)") as copy:
                            while data := f.read(100):
                                copy.write(data)

                # Remove duplicate times, in the observation data, from previous timemark files
                if inputSourceName != 'adcirc':
                    # Get min and max times from observation data files
                    minTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','water_level'])['time'].min()
                    maxTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','water_level'])['time'].max()

                    logger.info('Remove duplicate times for data source '+inputDataSource+', with source name '+inputSourceName
                                +', and input source archive: '+inputSourceArchive+' with start time of '+str(minTime)+' and end time of '+str(maxTime)+'.')

                    # Delete duplicate times
                    deleteDuplicateTimes(inputDataSource, inputSourceName, inputSourceArchive, minTime, maxTime)

                    logger.info('Removed duplicate times for data source '+inputDataSource+', with source name '+inputSourceName
                                +', and input source archive: '+inputSourceArchive+' with start time of '+str(minTime)+' and end time of '+str(maxTime)+'.')

                else:
                    # Get min and max times from adcirc data files
                    minTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','water_level'])['time'].min()
                    maxTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','water_level'])['time'].max()

                    logger.info('Data source '+inputDataSource+', with source name '+inputSourceName+', and source archive '+inputSourceArchive
                                +', with min time: '+str(minTime)+' and max time '+str(maxTime)+' does not need duplicate times removed.')

                # Run update 
                cur.execute("""UPDATE drf_harvest_data_file_meta
                               SET ingested = True
                               WHERE file_name = %(update_file)s
                               """,
                            {'update_file': ingestFile})


                # Remove ingest data file after ingesting it.
                logger.info('Remove ingest data file: '+ingestPathFile+' after ingesting it')
                os.remove(ingestPathFile)

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes not input, and creates the drf_gauge_station_source_data view.
def createView():
    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            # Run query
            cur.execute("""CREATE VIEW drf_gauge_station_source_data AS
                           SELECT d.obs_id AS obs_id,
                                  s.source_id AS source_id,
                                  g.station_id AS station_id,
                                  g.station_name AS station_name,
                                  d.timemark AS timemark,
                                  d.time AS time,
                                  d.water_level AS water_level,
                                  d.wind_speed AS wind_speed,
                                  d.air_pressure AS air_pressure, 
                                  g.tz AS tz,
                                  g.gauge_owner AS gauge_owner,
                                  s.data_source AS data_source,
                                  s.source_name AS source_name,
                                  s.source_archive AS source_archive,
                                  s.units AS units,
                                  g.location_name AS location_name,
                                  g.location_type AS location_type,
                                  g.country AS country,
                                  g.state AS state,
                                  g.county AS county,
                                  g.geom AS geom
                           FROM drf_gauge_data d
                           INNER JOIN drf_gauge_source s ON s.source_id=d.source_id
                           INNER JOIN drf_gauge_station g ON s.station_id=g.station_id""")

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# Main program function takes args as input, which contains the inputDir, databaseDir, inputTask, inputDataSource, inputSourceName, and inputSourceArchive values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'ingestTasks.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    if args.ingestDir is None:
        ingestDir = ''
    elif args.ingestDir is not None:
        ingestDir = os.path.join(args.ingestDir, '')
    else:
        sys.exit('Incorrect ingestDir')

    if args.databaseDir is None:
        databaseDir = ''
    elif args.databaseDir is not None:
        databaseDir = os.path.join(args.databaseDir, '')
    else:
        sys.exit('Incorrect databaseDir')

    inputTask = args.inputTask

    if args.inputDataSource is None:
        inputDataSource = ''
    elif args.inputDataSource is not None:
        inputDataSource = args.inputDataSource
    else:
        sys.exit('Incorrect inputDataSource')

    if args.inputSourceName is None:
        inputSourceName = ''
    elif args.inputSourceName is not None:
        inputSourceName = args.inputSourceName
    else:
        sys.exit('Incorrect inputSourceName')

    if args.inputSourceArchive is None:
        inputSourceArchive = ''
    elif args.inputSourceArchive is not None:
        inputSourceArchive = args.inputSourceArchive
    else:
        sys.exit('Incorrect inputSourceArchive')

    if args.inputLocationType is None:
        inputLocationType = ''
    elif args.inputLocationType is not None:
        inputLocationType = args.inputLocationType
    else:
        sys.exit('Incorrect inputLocationType')

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'source_meta':
        logger.info('Ingesting source meta: '+inputDataSource+', '+inputSourceName+', '+inputSourceArchive+', '+inputLocationType+'.')
        ingestSourceMeta(inputDataSource, inputSourceName, inputSourceArchive, inputLocationType)
        logger.info('ingested source meta: '+inputDataSource+', '+inputSourceName+', '+inputSourceArchive+', '+inputLocationType+'.')
    elif inputTask.lower() == 'ingeststations':
        logger.info('Ingesting station data.')
        ingestStations(ingestDir)
        logger.info('Ingested station data.')
    elif inputTask.lower() == 'ingestsource':
        logger.info('Ingesting source data.')
        ingestSourceData(ingestDir)
        logger.info('ingested source data.')
    elif inputTask.lower() == 'file':
        logger.info('Ingesting input file information.')
        ingestHarvestDataFileMeta(ingestDir)
        logger.info('Ingested input file information.')
    elif inputTask.lower() == 'data':
        logger.info('Ingesting data from data source '+inputDataSource+', with source name '+inputSourceName+', from source archive '+inputSourceArchive+'.')
        ingestData(ingestDir, databaseDir, inputDataSource, inputSourceName, inputSourceArchive)
        logger.info('Ingested data from data source '+inputDataSource+', with source name '+inputSourceName+', from source archive '+inputSourceArchive+'.')
    elif inputTask.lower() == 'view':
        logger.info('Creating view.')
        createView()
        logger.info('Created view.')

# Run main function takes inputDir, databaseDir, inputTask, inputDataSource, inputSourceName, and inputSourceArchive as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=False)
    parser.add_argument("--databaseDIR", "--databaseDir", help="Database directory path", action="store", dest="databaseDir", required=False)
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['Source_meta','IngestStations','ingestSource', 'File','Data','View'], required=True)
    parser.add_argument("--inputDataSource", help="Input data source to be processed", action="store", dest="inputDataSource", choices=['namforecast_hsofs','nowcast_hsofs','namforecast_ec95d','nowcast_ec95d','coastal_gauge','river_gauge','tidal_gauge','tidal_predictions','ocean_buoy','air_barometer','wind_anemometer'], required=False)
    parser.add_argument("--inputSourceName", help="Input source name to be processed", action="store", dest="inputSourceName", choices=['adcirc','ncem','noaa','ndbc'], required=False)
    parser.add_argument("--inputSourceArchive", help="Input source archive to be processed", action="store", dest="inputSourceArchive", choices=['contrails','renci','noaa','ndbc'], required=False)
    parser.add_argument("--inputLocationType", help="Input location type to be processed", action="store", dest="inputLocationType", choices=['tidal','river','coastal','ocean'], required=False)

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

