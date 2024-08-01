#!/usr/bin/env python
# coding: utf-8

# Import Python modules
import argparse
import glob
import os
import sys
import psycopg
from psycopg import sql
import pandas as pd
from pathlib import Path
from loguru import logger

def deleteDuplicateTimes(inputDataSource, inputSourceName, inputSourceArchive, minTime, maxTime):
    ''' This function is used to delete duplicate records in the observation data. The observation data has duplicate 
        records with the same timestamp, but different timemarks because they are from different harvest data files.
        Parameters
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa...). Used by ingestSourceMeta,
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa...). Used by
                ingestSourceMeta, and ingestData.
            minTime: string
                The minimum time in the data file.
            maxTime: string
                The maximum time in the data file. 
        Returns 
            None
    '''         

    try:
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'],
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'],
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'], 
                             autocommit=True) as conn:
            cur = conn.cursor()
    
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
        logger.exception(error)

def ingestSourceMeta(inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable, inputFilenamePrefix, inputLocationType, inputUnits):
    ''' This function takes data source, source name, source archive, source variable, filename prefix, location type, data type, and 
        units  as input. It ingest these variables into the source meta table (drf_source_obs_meta). The variables in this table are then 
        used as inputs in runObsIngest.py.
        Parameters
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). 
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa...). 
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa...). 
            inputSourceVariable: string
                Source variable, such as water_level. 
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob.
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). 
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars).
        Returns 
            None
    '''

    logger.info('Ingest source meta for data source '+inputDataSource+', with source name '+inputSourceName+', source archive '+inputSourceArchive+
                ', and location type'+ inputLocationType)

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'], 
                             autocommit=True) as conn:
            cur = conn.cursor()

            # Run query
            cur.execute("""INSERT INTO drf_source_obs_meta(data_source, source_name, source_archive, source_variable, filename_prefix, 
                                       location_type, units)
                           VALUES (%(datasource)s, %(sourcename)s, %(sourcearchive)s, %(sourcevariable)s, %(filenamevariable)s, %(locationtype)s, 
                                   %(units)s)""",
                        {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 'sourcevariable': inputSourceVariable, 
                         'filenamevariable': inputFilenamePrefix, 'locationtype': inputLocationType, 'units': inputUnits})

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.exception(error)

def ingestStations(ingestDir):
    ''' This function takes as input an ingest directory. The input directory is used to search for geom stations files
        that are to be ingested. The ingest directory is used to define the path of the file to be ingested. The 
        ingest directory is the directory path in the apsviz-timeseriesdb database container.
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. 
        Returns
            None
    '''

    # Create list of geom files, to be ingested by searching the input directory for geom files.
    inputFiles = glob.glob(ingestDir+"stations/geom_*.csv")

    # Define the ingest path and file using the ingest directory and the geom file name

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'], 
                             autocommit=True) as conn:
            cur = conn.cursor()

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
        logger.exception(error)

def ingestSourceData(ingestDir):
    ''' This function takes as input an ingest directory. It uses the input directory to search for source CSV files, that where
        created by the createIngestObsSourceMeta.py program. It uses the ingest directory to define the path of the file that is to
        be ingested. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files.
        Returns
            None
    '''

    # Create list of source files, to be ingested by searching the input directory for source files.
    inputFiles = glob.glob(ingestDir+"source_*.csv")

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'], 
                             autocommit=True) as conn:
            cur = conn.cursor()

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
        logger.exception(error)

def getHarvestDataFileMeta(inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable):
    ''' This function takes a data source, source name, and source archive as inputs and uses them to query 
        the drf_harvest_obs_file_meta table, creating a DataFrame that contains a list of data files to 
        ingest. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
        Parameters
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...).
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa...).
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa...).
        Returns
            DataFrame
    '''

    try:
        # Create connection to database, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD']) as conn:
            cur = conn.cursor()

            # Run query
            cur.execute("""SELECT dir_path, file_name
                           FROM drf_harvest_obs_file_meta
                           WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                           source_archive = %(sourcearchive)s AND source_variable = %(sourcevariable)s AND ingested = False
                           ORDER BY data_date_time""",
                        {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 
                         'sourcevariable': inputSourceVariable})

            # convert query output to Pandas dataframe
            df = pd.DataFrame(cur.fetchall(), columns=['dir_path','file_name'])
 
            # Close cursor and database connection
            cur.close()
            conn.close()

            return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.exception(error)

def ingestHarvestDataFileMeta(ingestDir):
    ''' This function takes as input an ingest directory. It uses the input directory to seach for harvest_data_files
        that need to be ingested. It uses the ingest directory to define the path of the harvest_file to ingesting.
        The ingest directory is the directory path in the apsviz-timeseriesdb database container.
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files.
        Returns
            None
    '''

    inputFiles = glob.glob(ingestDir+"harvest_data_files_*.csv")

    try:
        # Create connection to databaseset, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'], 
                             autocommit=True) as conn:
            cur = conn.cursor()

            for infoFile in inputFiles:
                # Run ingest query
                with open(infoFile, "r") as f:
                    with cur.copy("COPY drf_harvest_obs_file_meta (dir_path,file_name,processing_datetime,data_date_time,data_begin_time,data_end_time,data_source,source_name,source_archive,source_variable,location_type,timemark,ingested,overlap_past_file_date_time) FROM STDIN WITH (FORMAT CSV)") as copy:
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
        logger.exception(error)

def ingestRetainObsStationFileMeta(ingestDir):
    ''' This function takes as input an ingest directory. It uses the input directory to seach for harvest_data_files
        that need to be ingested. It uses the ingest directory to define the path of the retain_obs_station_file_metto ingesting.
        The ingest directory is the directory path in the apsviz-timeseriesdb database container.
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files.
        Returns
            None
    '''

    inputFiles = glob.glob(ingestDir+"retain_obs_meta_files_*.csv")

    try:
        # Create connection to databaseset, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'], 
                             autocommit=True) as conn:
            cur = conn.cursor()

            for infoFile in inputFiles:
                # Run ingest query
                with open(infoFile, "r") as f:
                    with cur.copy("COPY drf_retain_obs_station_file_meta (dir_path,file_name,data_source,source_name,source_archive,location_type,timemark,begin_date,end_date,ingested) FROM STDIN WITH (FORMAT CSV)") as copy:
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
        logger.exception(error)

def ingestData(ingestDir, inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable):
    ''' This function takes an ingest directory, data source, source name, source archive, and source variable as input,
        and uses them to run the getHarvestDataFileMeta function. The getHarvestDataFileMeta function produces a DataFrame 
        (dfDirFiles) that contains a list of data files, that are queried from the drf_harvest_obs_file_meta table. These 
        files are then ingested into the drf_gauge_data table. After the data has been ingested, from a file, the column 
        "ingested", in the drf_harvest_obs_file_meta table, is updated from False to True. The ingest directory is the 
        directory path in the apsviz-timeseriesdb database container.
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files.
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa...). Used by ingestSourceMeta,
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa...). Used by
                ingestSourceMeta, and ingestData.
            inputSourceVariable: string
                Source variable, such as water_level.
        Returns
            None
    '''

    logger.info('Begin ingesting data source '+inputDataSource+', with source name '+inputSourceName+', source variable '+inputSourceVariable+' and source archive '+inputSourceArchive)

    # Get DataFrame the contains list of data files that need to be ingested
    dfDirFiles = getHarvestDataFileMeta(inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable)

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'], 
                             autocommit=True) as conn:
            cur = conn.cursor()

            # Loop thru DataFrame ingesting each data file
            for index, row in dfDirFiles.iterrows():
                ingestFile = row[1]
                ingestPathFile = ingestDir+'data_copy_'+ingestFile
                logger.info('Ingest file: '+ingestPathFile)

                with open(ingestPathFile, "r") as f:
                    with cur.copy(sql.SQL("""COPY drf_gauge_data (source_id,timemark,time,{}) 
                                             FROM STDIN WITH (FORMAT CSV)""").format(sql.Identifier(inputSourceVariable))) as copy:
                        while data := f.read(100):
                            copy.write(data)

                # Remove duplicate times, in the observation data, from previous timemark files WHY water_level!
                # Get min and max times from observation data files
                minTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','variable_name'])['time'].min()
                maxTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','variable_name'])['time'].max()

                logger.info('Remove duplicate times for data source '+inputDataSource+', with source name '+inputSourceName
                            +', and input source archive: '+inputSourceArchive+' with start time of '+str(minTime)+' and end time of '+str(maxTime)+'.')

                # Delete duplicate times
                deleteDuplicateTimes(inputDataSource, inputSourceName, inputSourceArchive, minTime, maxTime)

                logger.info('Removed duplicate times for data source '+inputDataSource+', with source name '+inputSourceName
                            +', and input source archive: '+inputSourceArchive+' with start time of '+str(minTime)+' and end time of '+str(maxTime)+'.')

                # Run update 
                cur.execute("""UPDATE drf_harvest_obs_file_meta
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
        logger.exception(error)

def ingestRetainObsStationData(ingestDir, inputFilename):
    ''' This function takes an ingest directory and input dataset as input, and used them to ingest the data in the file, 
        specified by the file name, into the drf_retain_obs_station table.
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files.
            inputFilename: string
                The name of the input file. This is a full file name that is used when ingesting the Retain Obs Station data.
        Returns
            None
    '''

    ingestFilename = 'obs_station_data_copy_'+inputFilename
    logger.info('Begin ingesting apsViz station data from file '+ingestFilename+', in directory '+ingestDir+'.')

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'], 
                             autocommit=True) as conn:
            cur = conn.cursor()

            # Run ingest query to input data with the following variables: station_name,lat,lon,location_name,tz,gauge_owner,country,state,county,geom,timemark,
            # begin_date,end_date,data_source,source_archive into the drf_retain_obs_station table.
            with open(ingestDir+ingestFilename, "r") as f:
                with cur.copy("COPY drf_retain_obs_station (station_name,lat,lon,location_name,tz,gauge_owner,country,state,county,geom,timemark,begin_date,end_date,data_source,source_name,source_archive,location_type) FROM STDIN WITH (FORMAT CSV)") as copy:
                    while data := f.read(100):
                        copy.write(data)

            # Run update 
            cur.execute("""UPDATE drf_retain_obs_station_file_meta
                           SET ingested = True
                           WHERE file_name = %(update_file)s
                           """,
                        {'update_file': inputFilename})

            # Remove ingest data file after ingesting it.
            logger.info('Remove ingest data file: '+ingestDir+ingestFilename+' after ingesting it')
            os.remove(ingestDir+ingestFilename)

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.exception(error)

def createObsView():
    ''' This function takes not input, and creates the drf_gauge_station_source_data view.
        Parameters
            None
        Returns
            None
    '''

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'], 
                             autocommit=True) as conn:
            cur = conn.cursor()

            # Run query
            cur.execute("""CREATE or REPLACE VIEW drf_gauge_station_source_data AS
                           SELECT d.obs_id AS obs_id,
                                  s.source_id AS source_id,
                                  g.station_id AS station_id,
                                  g.station_name AS station_name,
                                  d.timemark AS timemark,
                                  d.time AS time,
                                  d.water_level AS water_level,
                                  d.wave_height AS wave_height,
                                  d.wind_speed AS wind_speed,
                                  d.air_pressure AS air_pressure,
                                  d.stream_elevation AS stream_elevation, 
                                  g.tz AS tz,
                                  g.gauge_owner AS gauge_owner,
                                  s.data_source AS data_source,
                                  s.source_name AS source_name,
                                  s.source_archive AS source_archive,
                                  s.units AS units,
                                  g.location_name AS location_name,
                                  g.apsviz_station AS apsviz_station,
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
        logger.exception(error)

# Main program function takes args as input, which contains the inputDir, inputTask, inputDataSource, inputSourceName, and inputSourceArchive values.
@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, and runs specified task.
        Parameters
            args: dictionary
                contains the parameters listed below.
            inputTask: string
                The type of task (ingestSourceMeta, ingestStations, ingestSourceData, ingestHarvestDataFileMeta, ingestData, createObsView. createModelView ) 
                to be perfomed. The type of inputTask can change what other types of inputs ingestTask.py requires. Below is a list of all inputs, with associated tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestStations, ingestSourceData,
                ingestHarvestDataFileMeta, and ingestData.
            inputFilename: string
                The name of the input file. This is a full file name that is used when ingesting ApsViz Station data. Used by ingestRetainObsStationdata.
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob.
                Used by ingestSourceMeta.
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa...). Used by ingestSourceMeta,
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa...). Used by
                ingestSourceMeta, and ingestData.
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            inputSourceVariable: string
                Source variable, such as water_level. Used by ingestSourceMeta, and ingestData.
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars).
                Used by ingestSourceMeta.
        Returns
            None
    '''

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'ingestObsTasks.log', level='DEBUG', rotation="5 MB")
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    inputTask = args.inputTask

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'ingestsourcemeta':
        inputDataSource = args.inputDataSource
        inputSourceName = args.inputSourceName
        inputSourceArchive = args.inputSourceArchive
        inputSourceVariable = args.inputSourceVariable
        inputFilenamePrefix = args.inputFilenamePrefix
        inputLocationType = args.inputLocationType
        inputUnits = args.inputUnits
        logger.info('Ingesting source meta: '+inputDataSource+', '+inputSourceName+', '+inputSourceArchive+', '+inputSourceVariable+', '+inputFilenamePrefix+', '+inputLocationType+','+inputUnits+'.')
        ingestSourceMeta(inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable, inputFilenamePrefix, inputLocationType, inputUnits)
        logger.info('ingested source meta: '+inputDataSource+', '+inputSourceName+', '+inputSourceArchive+', '+inputSourceVariable+', '+inputFilenamePrefix+', '+inputLocationType+','+inputUnits+'.')
    elif inputTask.lower() == 'ingeststations':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Ingesting station data.')
        ingestStations(ingestDir)
        logger.info('Ingested station data.')
    elif inputTask.lower() == 'ingestsourcedata':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Ingesting source data.')
        ingestSourceData(ingestDir)
        logger.info('ingested source data.')
    elif inputTask.lower() == 'ingestharvestdatafilemeta':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Ingesting input data file information.')
        ingestHarvestDataFileMeta(ingestDir)
        logger.info('Ingested input data file information.')
    elif inputTask.lower() == 'ingestretainobsstationfilemeta':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Ingesting input obs station file information.')
        ingestRetainObsStationFileMeta(ingestDir)
        logger.info('Ingested input obs station file information.')
    elif inputTask.lower() == 'ingestdata':
        ingestDir = os.path.join(args.ingestDir, '')
        inputDataSource = args.inputDataSource
        inputSourceName = args.inputSourceName
        inputSourceArchive = args.inputSourceArchive
        inputSourceVariable = args.inputSourceVariable
        logger.info('Ingesting data from data source '+inputDataSource+', with source name '+inputSourceName+', and source variable '+inputSourceVariable+', from source archive '+inputSourceArchive+'.')
        ingestData(ingestDir, inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable)
        logger.info('Ingested data from data source '+inputDataSource+', with source name '+inputSourceName+', and source variable '+inputSourceVariable+', from source archive '+inputSourceArchive+'.')
    elif inputTask.lower() == 'ingestretainobsstationdata':
        ingestDir = args.ingestDir
        inputFilename = args.inputFilename
        logger.info('Ingesting Retain Obs station data from file '+inputFilename+', in directory '+ingestDir+'.')
        ingestRetainObsStationData(ingestDir, inputFilename)
        logger.info('Ingested Retain Obs station data from file '+inputFilename+', in directory '+ingestDir+'.')
    elif inputTask.lower() == 'createobsview':
        logger.info('Creating obs view.')
        createObsView()
        logger.info('Created obs view.')
    elif inputTask.lower() == 'createmodelview':
        logger.info('Creating model view.')
        createModelView()
        logger.info('Created model view.')

# Run main function takes inputDir, inputTask, inputDataSource, inputSourceName, and inputSourceArchive as input.
if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            inputTask: string
                The type of task (ingestSourceMeta, ingestStations, ingestSourceData, ingestHarvestDataFileMeta, ingestData, 
                createObsView, createModelView ) to be perfomed. The type of inputTask can change what other types of inputs
                ingestTask.py requires. Below is a list of all inputs, with associated tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestStations, ingestSourceData,
                ingestHarvestDataFileMeta, and ingestData.
            inputFilename: string
                The name of the input file. This is a full file name that is used when ingesting ApsViz Station data. Used by 
                ingestRetainObsStationdata.
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob. 
                Used by ingestSourceMeta.
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer...). Used 
                by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa...). Used by ingestSourceMeta, 
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa...). Used by 
                ingestSourceMeta, and ingestData.
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            inputSourceVariable: string
                Source variable, such as water_level. Used by ingestSourceMeta, and ingestData.
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars).
                Used by ingestSourceMeta.
        Returns
            None
    '''         

    # parse input arguments
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['ingestSourceMeta','ingestStations','ingestSourceData', 
                                                                                                                'ingestHarvestDataFileMeta','ingestRetainObsStationFileMeta','ingestData',
                                                                                                                'ingestRetainObsStationData','createObsView','createModelView'], 
                                                                                                                required=True)

    # get runScript argument to use in if statement
    args = parser.parse_known_args()[0]
    if args.inputTask.lower() == 'ingestsourcemeta':
        parser.add_argument("--inputDataSource", help="Input data source to be processed", action="store", dest="inputDataSource", required=True)
        parser.add_argument("--inputSourceName", help="Input source name to be processed", action="store", dest="inputSourceName", choices=['ncem','noaa','ndbc'], required=True)
        parser.add_argument("--inputSourceArchive", help="Input source archive the data is from", action="store", dest="inputSourceArchive", required=True)
        parser.add_argument("--inputSourceVariable", help="Input source variables", action="store", dest="inputSourceVariable", required=True)
        parser.add_argument("--inputFilenamePrefix", help="Input filename variables", action="store", dest="inputFilenamePrefix", required=True)
        parser.add_argument("--inputLocationType", help="Input location type to be processed", action="store", dest="inputLocationType", required=True)
        parser.add_argument("--inputUnits", help="Input units", action="store", dest="inputUnits", required=True)
    elif args.inputTask.lower() == 'ingeststations':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'ingestsourcedata':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'ingestharvestdatafilemeta':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'ingestretainobsstationfilemeta':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'ingestdata':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
        parser.add_argument("--inputDataSource", help="Input data source to be processed", action="store", dest="inputDataSource", required=True)
        parser.add_argument("--inputSourceName", help="Input source name to be processed", action="store", dest="inputSourceName", choices=['ncem','noaa','ndbc'], required=True)
        parser.add_argument("--inputSourceArchive", help="Input source archive the data is from", action="store", dest="inputSourceArchive", required=True)
        parser.add_argument("--inputSourceVariable", help="Input source variables", action="store", dest="inputSourceVariable", required=True)
    elif args.inputTask.lower() == 'ingestretainobsstationdata':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
        parser.add_argument("--inputFilename", help="Input filename to be processed", action="store", dest="inputFilename", required=True)

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

