#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import argparse
import glob
import os
import sys
import psycopg2
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# This function takes data source, source name, and source archive as input. It ingest these variables into the source meta table (drf_source_meta).
# The variables in this table are then used as inputs in runIngest.py 
def ingestSourceMeta(inputDataSource, inputSourceName, inputSourceArchive, inputLocationType):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""INSERT INTO drf_source_meta(data_source, source_name, source_archive, location_type)
                       VALUES (%(datasource)s, %(sourcename)s, %(sourcearchive)s, %(locationtype)s)""",
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 'locationtype': inputLocationType})

        # Commit ingest
        conn.commit()

        # Close cursor and database connection
        cur.close()
        conn.close()

    # If exception log error
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)

# This function takes an input directory and an ingest directory as input. The input directory is used to search for geom 
# station files that are to be ingested. The ingest directory is used to define the path of the file to be ingested. The 
# ingest directory is the directory path in the apsviz-timeseriesdb database container.
def ingestStations(ingestDir, databaseDir):
    # Create list of geom files, to be ingested by searching the input directory for geom files.
    inputFiles = glob.glob(ingestDir+"stations/geom_*.csv")

    # Loop thru geom file list, ingesting each one 
    for geomFile in inputFiles:
        # Define the ingest path and file using the ingest directory and the geom file name
        databasePathFile = databaseDir+'stations/'+Path(geomFile).parts[-1]
 
        try:
            # Create connection to database and get cursor
            conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
            cur.execute("""BEGIN""")

            # Run query
            cur.execute("""COPY drf_gauge_station(station_name,lat,lon,tz,gauge_owner,location_name,location_type,country,state,county,geom)
                           FROM %(ingest_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'ingest_path_file': databasePathFile})

            # Commit ingest
            conn.commit()
 
            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception log error
        except (Exception, psycopg2.DatabaseError) as error:
            logger.info(error)

# This function takes an input directory and ingest directory as input. It uses the input directory to search for source  
# csv files, that were created by the createIngestSourceMeta.py program. It uses the ingest directory to define the path
# of the file that is to be ingested. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
def ingestSourceData(ingestDir, databaseDir):
    # Create list of source files, to be ingested by searching the input directory for source files.
    inputFiles = glob.glob(ingestDir+"source_*.csv")

    # Loop thru source file list, ingesting each one
    for sourceFile in inputFiles:
        # Define the ingest path and file using the ingest directory and the source file name
        databasePathFile = databaseDir+Path(sourceFile).parts[-1]

        try:
            # Create connection to database and get cursor
            conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
            cur.execute("""BEGIN""")

            # Run query
            cur.execute("""COPY drf_gauge_source(station_id,data_source,source_name,source_archive,units)
                           FROM %(ingest_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'ingest_path_file': databasePathFile})

            # Commit ingest
            conn.commit()

            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception log error
        except (Exception, psycopg2.DatabaseError) as error:
            logger.info(error)

# This function takes an dataset name as input and uses it to query the drf_harvest_data_file_meta table,
# creating a DataFrame that contains a list of data files to ingest. The ingest directory is the directory
# path in the apsviz-timeseriesdb database container.
def getHarvestDataFileMeta(inputDataSource, inputSourceName, inputSourceArchive):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT file_name
                       FROM drf_harvest_data_file_meta
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                       source_archive = %(sourcearchive)s AND ingested = False
                       ORDER BY data_date_time""",
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['file_name'])
 
        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return Pandas dataframe
        if inputSourceName == 'adcirc':
            # Limit to 100 files at a time
            return(df.head(100))
        else:
            # Limit to 50 files at a time
            return(df.head(50))

    # If exception log error
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)

# This function takes an input directory and ingest directory as input. It uses the input directory to seach for
# harvest_files that need to be ingested. It uses the ingest directory to define the path of the harvest_file
# to ingesting. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
def ingestHarvestDataFileMeta(ingestDir, databaseDir):
    inputFiles = glob.glob(ingestDir+"harvest_files_*.csv")

    for infoFile in inputFiles:
        # Create list of data info files, to be ingested by searching the input directory for data info files.
        # databaseDir is the path in apsviz-timeseriesdb container, which starts the /home
        databasePathFile = databaseDir+Path(infoFile).parts[-1]

        try:
            # Create connection to database and get cursor
            conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
            cur.execute("""BEGIN""")

            # Run query
            cur.execute("""COPY drf_harvest_data_file_meta(dir_path,file_name,data_date_time,data_begin_time,data_end_time,data_source,source_name,source_archive,ingested,overlap_past_file_date_time)
                           FROM %(ingest_path_file)s
                           DELIMITER ','
                           CSV HEADER""",
                        {'ingest_path_file': databasePathFile})

            # Commit ingest
            conn.commit()

            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception log error
        except (Exception, psycopg2.DatabaseError) as error:
            logger.info(error)

        # Remove harvest meta file after ingesting it.
        logger.info('Remove harvest meta file: '+infoFile+' after ingesting it')
        os.remove(infoFile)

# This function takes an ingest directory and input dataset as input, and uses them to run the getHarvestDataFileMeta
# function and define the databasePathFile variable. The getHarvestDataFileMeta function produces a DataFrame (dfDirFiles) 
# that contains a list of data files, that are queried from the drf_harvest_data_file_meta table. These files are then 
# ingested into the drf_gauge_data table. After the data has been ingested, from a file, the column "ingested", in the 
# drf_harvest_data_file_meta table, is updated from False to True. The ingest directory is the directory path in the 
# apsviz-timeseriesdb database container.
def ingestData(databaseDir, inputDataSource, inputSourceName, inputSourceArchive):
    # Get DataFrame the contains list of data files that need to be ingested
    dfDirFiles = getHarvestDataFileMeta(inputDataSource, inputSourceName, inputSourceArchive)

    # Loop thru DataFrame ingesting each data file 
    for index, row in dfDirFiles.iterrows():
        # Get name of file, that needs to be ingested, from DataFrame, and create data_copy file name and output path
        # (databasePathFile) outsaved to the DataIngesting directory area where is the be ingested using the copy command.
        ingestFile = row[0]
        databasePathFile = databaseDir+'data_copy_'+ingestFile

        try:
            # Create connection to database and get cursor
            conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
            cur.execute("""BEGIN""")

            if inputDataSource == 'air_barometer':
                # Run query
                cur.execute("""COPY drf_gauge_data(source_id,timemark,time,air_pressure)
                               FROM %(ingest_path_file)s
                               DELIMITER ','
                               CSV HEADER""",
                            {'ingest_path_file': databasePathFile})

                # Commit ingest
                conn.commit()

            elif inputDataSource == 'wind_anemometer':
                # Run query
                cur.execute("""COPY drf_gauge_data(source_id,timemark,time,wind_speed)
                               FROM %(ingest_path_file)s
                               DELIMITER ','
                               CSV HEADER""",
                            {'ingest_path_file': databasePathFile})

                # Commit ingest
                conn.commit()

            else:
                # Run query
                cur.execute("""COPY drf_gauge_data(source_id,timemark,time,water_level)
                               FROM %(ingest_path_file)s
                               DELIMITER ','
                               CSV HEADER""",
                            {'ingest_path_file': databasePathFile})

                # Commit ingest
                conn.commit()

            # Run update 
            cur.execute("""UPDATE drf_harvest_data_file_meta
                           SET ingested = True
                           WHERE file_name = %(update_file)s
                           """,
                        {'update_file': ingestFile})

            # Commit update 
            conn.commit()

            # Close cursor and database connection
            cur.close()
            conn.close()

        # If exception log error
        except (Exception, psycopg2.DatabaseError) as error:
            logger.info(error)

# This function takes not input, and creates the drf_gauge_station_source_data view.
def createView():
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

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

        # Commit ingest
        conn.commit()

        # Close cursor and database connection
        cur.close()
        conn.close()

    # If exception log error
    except (Exception, psycopg2.DatabaseError) as error:
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
        ingestStations(ingestDir, databaseDir)
        logger.info('Ingested station data.')
    elif inputTask.lower() == 'ingestsource':
        logger.info('Ingesting source data.')
        ingestSourceData(ingestDir, databaseDir)
        logger.info('ingested source data.')
    elif inputTask.lower() == 'file':
        logger.info('Ingesting input file information.')
        ingestHarvestDataFileMeta(ingestDir, databaseDir)
        logger.info('Ingested input file information.')
    elif inputTask.lower() == 'data':
        logger.info('Ingesting data from data source '+inputDataSource+', with source name '+inputSourceName+', from source archive '+inputSourceArchive+'.')
        ingestData(databaseDir, inputDataSource, inputSourceName, inputSourceArchive)
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

