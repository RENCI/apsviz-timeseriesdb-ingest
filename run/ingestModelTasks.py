#)!/usr/bin/env python
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

def ingestSourceMeta(inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable, inputSourceInstance, inputForcingMetaclass, 
                     inputFilenamePrefix, inputLocationType, inputUnits):
    ''' This function takes data source, source name, source archive, source variable, filename prefix, location type, data type, and 
        units  as input. It ingest these variables into the source meta table (drf_source_model_meta). The variables in this table are then 
        used as inputs in runModelIngest.py.
        Parameters
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). 
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...). 
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...). 
            inputSourceVariable: string
                Source variable, such as water_level. Used by ingestSourceMeta, and ingestData.
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
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
                'source_instance '+inputSourceInstance+', and location type'+ inputLocationType)

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
            cur.execute("""INSERT INTO drf_source_model_meta(data_source, source_name, source_archive, source_variable, source_instance, forcing_metaclass, filename_prefix, location_type, units)
                           VALUES (%(datasource)s, %(sourcename)s, %(sourcearchive)s, %(sourcevariable)s,%(sourceinstance)s,%(forcingmetaclass)s, %(filenamevariable)s, %(locationtype)s, %(units)s)""",
                        {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 'sourcevariable': inputSourceVariable, 
                         'sourceinstance':inputSourceInstance, 'forcingmetaclass':inputForcingMetaclass, 'filenamevariable': inputFilenamePrefix, 
                         'locationtype': inputLocationType, 'units': inputUnits})

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def ingestSourceData(ingestPath):
    ''' This function takes as input an ingest directory. It uses the input directory to search for source CSV files, that where
        created by the createIngestModelSourceMeta.py program. It uses the ingest directory to define the path of the file that is to
        be ingested. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
        Parameters
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
        Returns
            None
    '''

    # Create list of source files, to be ingested by searching the input directory for source files.
    inputFiles = glob.glob(ingestPath+"source_*.csv")

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
                    with cur.copy("COPY drf_model_source (station_id,data_source,source_instance,source_name,forcing_metaclass,source_archive,units) FROM STDIN WITH (FORMAT CSV)") as copy:
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

def getHarvestDataFileMeta(inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass):
    ''' This function takes a data source, source name, and source archive as inputs and uses them to query 
        the drf_harvest_model_file_meta table, creating a DataFrame that contains a list of data files to 
        ingest. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
        Parameters
            inputDataSource: string
                Unique identifier of data source (e.g. NAMFORECAST_NCSC_SAB_V1.23...).
            inputSourceName: string
                Organization that owns original source data (e.g., adcirc...).
            inputSourceArchive: string
                Where the original data source is archived (e.g., renci, psc, uga...).
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
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
                           FROM drf_harvest_model_file_meta
                           WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND source_archive = %(sourcearchive)s AND 
                           source_instance = %(sourceinstance)s AND forcing_metaclass = %(forcingmetaclass)s AND ingested = False
                           ORDER BY data_date_time""",
                        {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive,
                         'sourceinstance': inputSourceInstance, 'forcingmetaclass': inputForcingMetaclass})

            # convert query output to Pandas dataframe
            df = pd.DataFrame(cur.fetchall(), columns=['dir_path','file_name'])
 
            # Close cursor and database connection
            cur.close()
            conn.close()

            return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def ingestHarvestDataFileMeta(ingestPath):
    ''' This function takes as input an ingest directory. It uses the input directory to seach for harvest_data_files
        that need to be ingested. It uses the ingest directory to define the path of the harvest_file to ingesting.
        The ingest directory is the directory path in the apsviz-timeseriesdb database container.
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
        Returns
            None
    '''

    # Get input files for ingesting
    inputFiles = glob.glob(ingestPath+"harvest_data_files_*.csv")

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
                    with cur.copy("COPY drf_harvest_model_file_meta (dir_path,file_name,model_run_id,processing_datetime,data_date_time,data_begin_time,data_end_time,data_source,source_name,source_archive,source_instance,forcing_metaclass,advisory,timemark,ingested,overlap_past_file_date_time) FROM STDIN WITH (FORMAT CSV)") as copy:
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

def ingestApsVizStationFileMeta(ingestPath):
    ''' This function takes an ingest directory, and filename as input. It uses the input filename, along with the input
        directory, to ingest the specified file into the drf_apsviz_station_file_meta directory. The ingest directory 
        is the directory path in the apsviz-timeseriesdb database container.
        Parameters
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
        Returns
            None
    '''

    # Get input files for ingesting
    inputFiles = glob.glob(ingestPath+"harvest_meta_files_*.csv")

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
                    with cur.copy("COPY drf_apsviz_station_file_meta (dir_path,file_name,data_date_time,data_source,source_name,source_archive,source_instance,forcing_metaclass,grid_name,model_run_id,timemark,location_type,csvurl,ingested) FROM STDIN WITH (FORMAT CSV)") as copy:
                        while data := f.read(100):
                            copy.write(data)

                # Remove harvest meta file after ingesting it.
                logger.info('Remove apsVis station meta file: '+infoFile+' after ingesting it')
                os.remove(infoFile)

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def ingestData(ingestPath, inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable, inputSourceInstance, inputForcingMetaclass):
    ''' This function takes an ingest directory, data source, source name, source archive, and source variable as input,
        and uses them to run the getHarvestDataFileMeta function. The getHarvestDataFileMeta function produces a DataFrame 
        (dfDirFiles) that contains a list of data files, that are queried from the drf_harvest_model_file_meta table. These 
        files are then ingested into the drf_model_data table. After the data has been ingested, from a file, the column 
        "ingested", in the drf_harvest_model_file_meta table, is updated from False to True. The ingest directory is the 
        directory path in the apsviz-timeseriesdb database container.
        Parameters
             ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
            inputDataSource: string
                Unique identifier of data source (e.g., NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Model that produced the source data (e.g., adcirc...). Used by ingestSourceMeta,
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., renci, psc, uga...). Used by
                ingestSourceMeta, and ingestData.
            inputSourceVariable: string
                Source variable, such as water_level. Used by ingestSourceMeta, and ingestData.
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
        Returns
            None
    '''

    logger.info('Begin ingesting data source '+inputDataSource+', with source name '+inputSourceName+', source variable '+inputSourceVariable+' and source archive '+inputSourceArchive)
    # Get DataFrame the contains list of data files that need to be ingested
    dfDirFiles = getHarvestDataFileMeta(inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetaclass)

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
                ingestPathFile = ingestPath+'data_copy_'+ingestFile
                logger.info('Ingest file: '+ingestPathFile)

                with open(ingestPathFile, "r") as f:
                    with cur.copy(sql.SQL("""COPY drf_model_data (source_id,timemark,time,{}) 
                                             FROM STDIN WITH (FORMAT CSV)""").format(sql.Identifier(inputSourceVariable))) as copy:
                        while data := f.read(100):
                            copy.write(data)

                # Get min and max times from adcirc data files
                minTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','variable_name'])['time'].min()
                maxTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','variable_name'])['time'].max()

                logger.info('Data source '+inputDataSource+', with source name '+inputSourceName+', and source archive '+inputSourceArchive
                            +', with min time: '+str(minTime)+' and max time '+str(maxTime)+' does not need duplicate times removed.')

                # Run update 
                cur.execute("""UPDATE drf_harvest_model_file_meta
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

def ingestApsVizStationData(ingestPath, inputFilename):
    ''' This function takes an ingest directory and input dataset as input, and used them to ingest the data in the file, 
        specified by the file name, into the drf_apsviz_station table.
        Parameters
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
            inputFilename: string
                The name of the input file. This is a full file name that is used when ingesting ApsViz Station data. Used by
                ingestApsVizStationFileMeta, and ingestApsVizStationData.
        Returns
            None
    '''

    ingestFilename = 'meta_copy_'+inputFilename
    logger.info('Begin ingesting apsViz station data from file '+ingestFilename+', in directory '+ingestPath+'.')

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                             user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                             host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                             port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                             password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'], 
                             autocommit=True) as conn:
            cur = conn.cursor()
    
            # Run ingest query
            with open(ingestPath+ingestFilename, "r") as f:
                with cur.copy("COPY drf_apsviz_station (station_name,lat,lon,location_name,tz,gauge_owner,country,state,county,geom,timemark,model_run_id,data_source,source_name,source_instance,source_archive,forcing_metaclass,location_type,grid_name,csvurl) FROM STDIN WITH (FORMAT CSV)") as copy:
                    while data := f.read(100):
                        copy.write(data)

            # Run update 
            cur.execute("""UPDATE drf_apsviz_station_file_meta
                           SET ingested = True
                           WHERE file_name = %(update_file)s
                           """,
                        {'update_file': inputFilename})

            # Remove ingest data file after ingesting it.
            logger.info('Remove ingest data file: '+ingestPath+ingestFilename+' after ingesting it')
            os.remove(ingestPath+ingestFilename)

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def createModelView():
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
            cur.execute("""CREATE or REPLACE VIEW drf_model_station_source_data AS
                           SELECT d.model_id AS model_id,,
                                  s.source_id AS source_id,
                                  g.station_id AS station_id,
                                  g.station_name AS station_name,
                                  d.timemark AS timemark,
                                  d.time AS time,
                                  d.water_level AS water_level,
                                  d.wave_height AS wave_height,
                                  g.tz AS tz,
                                  g.gauge_owner AS gauge_owner,
                                  s.data_source AS data_source,
                                  s.source_name AS source_name,
                                  s.source_archive AS source_archive,
                                  s.source_instance AS source_instance,
                                  s.forcing_metaclass AS forcing_metaclass,
                                  s.units AS units,
                                  g.location_name AS location_name,
                                  g.apsviz_station AS apsviz_station,
                                  g.location_type AS location_type,
                                  g.country AS country,
                                  g.state AS state,
                                  g.county AS county,
                                  g.geom AS geom
                           FROM drf_model_data d
                           INNER JOIN drf_model_source s ON s.source_id=d.source_id
                           INNER JOIN drf_gauge_station g ON s.station_id=g.station_id""")

            # Close cursor and database connection
            cur.close()
            conn.close()

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# Main program function takes args as input, which contains the inputDir, inputTask, inputDataSource, inputSourceName, and inputSourceArchive values.
@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, and runs specified task.
        Parameters
            args: dictionary
                contains the parameters listed below.
            inputTask: string
                The type of task (ingestSourceMeta, ingestSourceData, ingestHarvestDataFileMeta, ingestApsVizStationFileMeta,
                ingestData, ingestApsVizStationData, createModelView ) to be perfomed. The type of inputTask can change what other types of inputs
                ingestTask.py requires. Below is a list of all inputs, with associated tasks.
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path. Used by ingestStations, ingestSourceData, ingestHarvestDataFileMeta, ingestApsVizStationFileMeta, ingestData, 
                and ingestApsVizStationData.
            inputFilename: string
                The name of the input file. This is a full file name that is used when ingesting ApsViz Station data. Used by
                ingestApsVizStationFileMeta, and ingestApsVizStationData.
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob.
                Used by ingestSourceMeta.
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...). Used by ingestSourceMeta,
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...). Used by
                ingestSourceMeta, and ingestData.
            inputSourceVariable: string
                Source variable, such as water_level. Used by ingestSourceMeta, and ingestData.
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, ingestData, and getHarvestDataFileMeta.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars).
                Used by ingestSourceMeta.
        Returns
            None
    '''

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'ingestModelTasks.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    inputTask = args.inputTask
    
    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'ingestsourcemeta':
        inputDataSource = args.inputDataSource
        inputSourceName = args.inputSourceName
        inputSourceArchive = args.inputSourceArchive
        inputSourceVariable = args.inputSourceVariable
        inputSourceInstance = args.inputSourceInstance
        inputForcingMetaclass = args.inputForcingMetaclass
        inputFilenamePrefix = args.inputFilenamePrefix
        inputLocationType = args.inputLocationType
        inputUnits = args.inputUnits
        logger.info('Ingesting source meta: '+inputDataSource+', '+inputSourceName+', '+inputSourceArchive+', '+inputSourceVariable+', '+inputFilenamePrefix+', '+inputLocationType+','+inputUnits+'.')
        ingestSourceMeta(inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable, inputSourceInstance, inputForcingMetaclass, inputFilenamePrefix, inputLocationType, inputUnits)
        logger.info('ingested source meta: '+inputDataSource+', '+inputSourceName+', '+inputSourceArchive+', '+inputSourceVariable+', '+inputFilenamePrefix+', '+inputLocationType+','+inputUnits+'.')
    elif inputTask.lower() == 'ingestsourcedata':
        ingestPath = os.path.join(args.ingestPath, '')
        logger.info('Ingesting source data.')
        ingestSourceData(ingestPath)
        logger.info('ingested source data.')
    elif inputTask.lower() == 'ingestharvestdatafilemeta':
        ingestPath = os.path.join(args.ingestPath, '')
        logger.info('Ingesting input data file information.')
        ingestHarvestDataFileMeta(ingestPath)
        logger.info('Ingested input data file information.')
    elif inputTask.lower() == 'ingestapsvizstationfilemeta':
        ingestPath = os.path.join(args.ingestPath, '')
        logger.info('Ingesting input apsViz station meta file information.')
        ingestApsVizStationFileMeta(ingestPath)
        logger.info('Ingested input apsViz station meta file information.')
    elif inputTask.lower() == 'ingestdata':
        ingestPath = os.path.join(args.ingestPath, '')
        inputDataSource = args.inputDataSource
        inputSourceName = args.inputSourceName
        inputSourceArchive = args.inputSourceArchive
        inputSourceVariable = args.inputSourceVariable
        inputSourceInstance = args.inputSourceInstance
        inputForcingMetaclass = args.inputForcingMetaclass
        logger.info('Ingesting data from data source '+inputDataSource+', with source name '+inputSourceName+', and source variable '+inputSourceVariable+', from source archive '+inputSourceArchive+' with source instance '+inputSourceInstance+'.')
        ingestData(ingestPath, inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable, inputSourceInstance, inputForcingMetaclass)
        logger.info('Ingested data from data source '+inputDataSource+', with source name '+inputSourceName+', and source variable '+inputSourceVariable+', from source archive '+inputSourceArchive+' with source instance '+inputSourceInstance+'.')
    elif inputTask.lower() == 'ingestapsvizstationdata':
        ingestPath = args.ingestPath
        inputFilename = args.inputFilename
        logger.info('Ingesting apsViz station data '+inputFilename+' files, in directory '+ingestPath+'.')
        ingestApsVizStationData(ingestPath, inputFilename)
        logger.info('Ingested apsViz station data '+inputFilename+' file, in directory '+ingestPath+'.')
    elif inputTask.lower() == 'createmodelview':
        logger.info('Creating model view.')
        createModelView()
        logger.info('Created model view.')

# Run main function takes inputDir, inputTask, inputDataSource, inputSourceName, and inputSourceArchive as input.
if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            inputTask: string
                The type of task (ingestSourceMeta, ingestSourceData, ingestHarvestDataFileMeta, ingestApsVizStationFileMeta, 
                ingestData, ingestApsVizStationData, createModelView ) to be perfomed. The type of inputTask can change what other types of inputs
                ingestTask.py requires. Below is a list of all inputs, with associated tasks.
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path. Used by ingestStations, ingestSourceData, ingestHarvestDataFileMeta, ingestApsVizStationFileMeta, ingestData, 
                and ingestApsVizStationData.
            inputFilename: string
                The name of the input file. This is a full file name that is used when ingesting ApsViz Station data. Used by 
                ingestApsVizStationFileMeta, and ingestApsVizStationData.
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob. 
                Used by ingestSourceMeta.
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, 
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...). Used by ingestSourceMeta, 
                and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...). Used by 
                ingestSourceMeta, and ingestData.
            inputSourceVariable: string
                Source variable, such as water_level. Used by ingestSourceMeta, and ingestData.
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, ingestData, and getHarvestDataFileMeta.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS). Used by ingestSourceMeta.
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars).
                Used by ingestSourceMeta.
        Returns
            None
    '''         

    # parse input arguments
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['ingestSourceMeta','ingestSourceData','ingestHarvestDataFileMeta',
                                                                                                                'ingestApsVizStationFileMeta','ingestData','ingestApsVizStationData',
                                                                                                                'createModelView'], 
                                                                                                                required=True)

    # get runScript argument to use in if statement
    args = parser.parse_known_args()[0]
    if args.inputTask.lower() == 'ingestsourcemeta':
        parser.add_argument("--inputDataSource", help="Input data source to be processed", action="store", dest="inputDataSource", required=True)
        parser.add_argument("--inputSourceName", help="Input source name to be processed", action="store", dest="inputSourceName", choices=['adcirc','ncem','noaa','ndbc'], required=True)
        parser.add_argument("--inputSourceArchive", help="Input source archive the data is from", action="store", dest="inputSourceArchive", required=True)
        parser.add_argument("--inputSourceVariable", help="Input source variables", action="store", dest="inputSourceVariable", required=True)
        parser.add_argument("--inputSourceInstance", help="Input source variables", action="store", dest="inputSourceInstance", required=True)
        parser.add_argument("--inputForcingMetaclass", help="Input forcing metaclass", action="store", dest="inputForcingMetaclass", required=True)
        parser.add_argument("--inputFilenamePrefix", help="Input filename variables", action="store", dest="inputFilenamePrefix", required=True)
        parser.add_argument("--inputLocationType", help="Input location type to be processed", action="store", dest="inputLocationType", required=True)
        parser.add_argument("--inputUnits", help="Input units", action="store", dest="inputUnits", required=True)
    elif args.inputTask.lower() == 'ingestsourcedata':
        parser.add_argument("--ingestPath", "--ingestPath", help="Ingest directory path, including the modelRunID", action="store", dest="ingestPath", required=True)
    elif args.inputTask.lower() == 'ingestharvestdatafilemeta':
        parser.add_argument("--ingestPath", "--ingestPath", help="Ingest directory path, including the modelRunID", action="store", dest="ingestPath", required=True)
    elif args.inputTask.lower() == 'ingestapsvizstationfilemeta':
        parser.add_argument("--ingestPath", "--ingestPath", help="Ingest directory path, including the modelRunID", action="store", dest="ingestPath", required=True)
    elif args.inputTask.lower() == 'ingestdata':
        parser.add_argument("--ingestPath", "--ingestPath", help="Ingest directory path, including the modelRunID", action="store", dest="ingestPath", required=True)
        parser.add_argument("--inputDataSource", help="Input data source to be processed", action="store", dest="inputDataSource", required=True)
        parser.add_argument("--inputSourceName", help="Input source name to be processed", action="store", dest="inputSourceName", choices=['adcirc','ncem','noaa','ndbc'], required=True)
        parser.add_argument("--inputSourceArchive", help="Input source archive the data is from", action="store", dest="inputSourceArchive", required=True)
        parser.add_argument("--inputSourceVariable", help="Input source variables", action="store", dest="inputSourceVariable", required=True)
        parser.add_argument("--inputSourceInstance", help="Input source variables", action="store", dest="inputSourceInstance", required=True)
        parser.add_argument("--inputForcingMetaclass", help="Input forcing metaclass", action="store", dest="inputForcingMetaclass", required=True)
    elif args.inputTask.lower() == 'ingestapsvizstationdata':
        parser.add_argument("--ingestPath", "--ingestPath", help="Ingest directory path, including the modelRunID", action="store", dest="ingestPath", required=True)
        parser.add_argument("--inputFileName", "--inputFilename", help="Input filename for apzViz station meta file", action="store", dest="inputFilename", required=True)

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

