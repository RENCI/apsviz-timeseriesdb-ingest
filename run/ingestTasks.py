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

# This function is used to delete duplicate records in the observation data. The observation data has duplicate records with the 
# same timestamp, but different timemarks because they are from different harvest data files.
def deleteDuplicateTimes(inputDataSource, inputSourceName, inputSourceArchive, minTime, maxTime):
    try:
        with psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'],
                             host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'],
                             password=os.environ['SQL_GAUGE_PASSWORD'], autocommit=True) as conn:
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
def ingestSourceMeta(inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable, inputFilenamePrefix, inputLocationType, inputDataType, inputUnits):
    logger.info('Ingest source meta for data source '+inputDataSource+', with source name '+inputSourceName+', source archive '+inputSourceArchive+
                ', and location type'+ inputLocationType)

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            # Run query
            cur.execute("""INSERT INTO drf_source_meta(data_source, source_name, source_archive, source_variable, filename_prefix, location_type, data_type, units)
                           VALUES (%(datasource)s, %(sourcename)s, %(sourcearchive)s, %(sourcevariable)s, %(filenamevariable)s, %(locationtype)s, %(datatype)s,  %(units)s)""",
                        {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 'sourcevariable': inputSourceVariable, 'filenamevariable': inputFilenamePrefix, 'locationtype': inputLocationType, 'datatype': inputDataType, 'units': inputUnits})

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
        with psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD'], autocommit=True) as conn:
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
        with psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD'], autocommit=True) as conn:
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
        with psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD']) as conn:
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
# harvest_data_files that need to be ingested. It uses the ingest directory to define the path of the harvest_file
# to ingesting. The ingest directory is the directory path in the apsviz-timeseriesdb database container.
def ingestHarvestDataFileMeta(ingestDir):
    inputFiles = glob.glob(ingestDir+"harvest_data_files_*.csv")

    try:
        # Create connection to databaseset, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD'], autocommit=True) as conn:
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

# This function takes an ingest directory, and input filename  as input. It uses the input filename, along with the input directory, 
# to ingest the specified file into the drf_apsviz_station_file_meta directory. The ingest directory is the directory path in the 
# apsviz-timeseriesdb database container.
def ingestApsVizStationFileMeta(ingestDir, inputFilename):
    try:
        # Create connection to databaseset, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            # Run ingest query
            with open(ingestDir+inputFilename, "r") as f:
                with cur.copy("COPY drf_apsviz_station_file_meta (dir_path,file_name,data_date_time,data_source,source_name,source_archive,model_run_id,timemark,variable_type,csvurl,ingested) FROM STDIN WITH (FORMAT CSV)") as copy:
                    while data := f.read(100):
                        copy.write(data)

            # Remove harvest meta file after ingesting it.
            logger.info('Remove apsVis station meta file: '+inputFilename+' after ingesting it')
            os.remove(ingestDir+inputFilename)

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
def ingestData(ingestDir, databaseDir, inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable):
    logger.info('Begin ingesting data source '+inputDataSource+', with source name '+inputSourceName+', source variable '+inputSourceVariable+' and source archive '+inputSourceArchive)
    # Get DataFrame the contains list of data files that need to be ingested
    dfDirFiles = getHarvestDataFileMeta(inputDataSource, inputSourceName, inputSourceArchive)

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

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
                if inputSourceName != 'adcirc':
                    # Get min and max times from observation data files
                    minTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','variable_name'])['time'].min()
                    maxTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','variable_name'])['time'].max()

                    logger.info('Remove duplicate times for data source '+inputDataSource+', with source name '+inputSourceName
                                +', and input source archive: '+inputSourceArchive+' with start time of '+str(minTime)+' and end time of '+str(maxTime)+'.')

                    # Delete duplicate times
                    deleteDuplicateTimes(inputDataSource, inputSourceName, inputSourceArchive, minTime, maxTime)

                    logger.info('Removed duplicate times for data source '+inputDataSource+', with source name '+inputSourceName
                                +', and input source archive: '+inputSourceArchive+' with start time of '+str(minTime)+' and end time of '+str(maxTime)+'.')

                elif inputSourceName == 'adcirc':
                    # Get min and max times from adcirc data files
                    minTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','variable_name'])['time'].min()
                    maxTime = pd.read_csv(ingestPathFile, names=['source_id','timemark','time','variable_name'])['time'].max()

                    if inputDataSource[0:7] == 'nowcast':
                        logger.info('Remove duplicate times for data source '+inputDataSource+', with source name '+inputSourceName
                                    +', and input source archive: '+inputSourceArchive+' with start time of '+str(minTime)+' and end time of '+str(maxTime)+'.')

                        # Delete duplicate times
                        deleteDuplicateTimes(inputDataSource, inputSourceName, inputSourceArchive, minTime, maxTime)

                        logger.info('Removed duplicate times for data source '+inputDataSource+', with source name '+inputSourceName
                                    +', and input source archive: '+inputSourceArchive+' with start time of '+str(minTime)+' and end time of '+str(maxTime)+'.')
                    else:
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

# This function takes an ingest directory and input dataset as input, and uses them to run the getHarvestDataFileMeta
# function. The getHarvestDataFileMeta function produces a DataFrame (dfDirFiles) 
# that contains a list of data files, that are queried from the drf_harvest_data_file_meta table. These files are then 
# ingested into the drf_gauge_data table. After the data has been ingested, from a file, the column "ingested", in the 
# drf_harvest_data_file_meta table, is updated from False to True. The ingest directory is the directory path in the 
# apsviz-timeseriesdb database container.
def ingestApsVizStationData(ingestDir, inputFilename):
    ingestFilename = 'meta_copy_'+inputFilename
    logger.info('Begin ingesting apsViz station data from file '+ingestFilename+', in directory '+ingestDir+'.')

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            # Run ingest query
            with open(ingestDir+ingestFilename, "r") as f:
                with cur.copy("COPY drf_apsviz_station (station_name,lat,lon,name,units,tz,owner,state,county,site,node,geom,timemark,model_run_id,variable_type,csvurl) FROM STDIN WITH (FORMAT CSV)") as copy:
                    while data := f.read(100):
                        copy.write(data)

            # Run update 
            cur.execute("""UPDATE drf_apsviz_station_file_meta
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
        logger.info(error)

# This function takes not input, and creates the drf_gauge_station_source_data view.
def createView():
    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD'], autocommit=True) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

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
                                  d.flow_volume AS flow_volume, 
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

    inputTask = args.inputTask

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'ingestsourcemeta':
        inputDataSource = args.inputDataSource
        inputSourceName = args.inputSourceName
        inputSourceArchive = args.inputSourceArchive
        inputSourceVariable = args.inputSourceVariable
        inputFilenamePrefix = args.inputFilenamePrefix
        inputLocationType = args.inputLocationType
        inputDataType = args.inputDataType
        inputUnits = args.inputUnits
        logger.info('Ingesting source meta: '+inputDataSource+', '+inputSourceName+', '+inputSourceArchive+', '+inputSourceVariable+', '+inputFilenamePrefix+', '+inputLocationType+','+inputUnits+'.')
        ingestSourceMeta(inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable, inputFilenamePrefix, inputLocationType, inputDataType, inputUnits)
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
    elif inputTask.lower() == 'ingestapsvizstationfilemeta':
        ingestDir = os.path.join(args.ingestDir, '')
        inputFilename = args.inputFilename
        logger.info('Ingesting input apsViz station meta file information.')
        ingestApsVizStationFileMeta(ingestDir, inputFilename)
        logger.info('Ingested input apsViz station meta file information.')
    elif inputTask.lower() == 'ingestdata':
        ingestDir = os.path.join(args.ingestDir, '')
        if args.databaseDir:
            databaseDir = os.path.join(args.databaseDir, '')
        else:
            databaseDir = ''
        inputDataSource = args.inputDataSource
        inputSourceName = args.inputSourceName
        inputSourceArchive = args.inputSourceArchive
        inputSourceVariable = args.inputSourceVariable
        logger.info('Ingesting data from data source '+inputDataSource+', with source name '+inputSourceName+', and source variable '+inputSourceVariable+', from source archive '+inputSourceArchive+'.')
        ingestData(ingestDir, databaseDir, inputDataSource, inputSourceName, inputSourceArchive, inputSourceVariable)
        logger.info('Ingested data from data source '+inputDataSource+', with source name '+inputSourceName+', and source variable '+inputSourceVariable+', from source archive '+inputSourceArchive+'.')
    elif inputTask.lower() == 'ingestapsvizstationdata':
        ingestDir = args.ingestDir
        inputFilename = args.inputFilename
        logger.info('Ingesting apsViz station data from file '+inputFilename+', in directory '+ingestDir+'.')
        ingestApsVizStationData(ingestDir, inputFilename)
        logger.info('Ingested apsViz station data from file '+inputFilename+', in directory '+ingestDir+'.')
    elif inputTask.lower() == 'createview':
        logger.info('Creating view.')
        createView()
        logger.info('Created view.')

# Run main function takes inputDir, databaseDir, inputTask, inputDataSource, inputSourceName, and inputSourceArchive as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['ingestSourceMeta','ingestStations','ingestSourceData', 'ingestHarvestDataFileMeta','ingestApsVizStationFileMeta','ingestData','ingestApsVizStationData','createView'], required=True)

    # get runScript argument to use in if statement
    args = parser.parse_known_args()[0]

    if args.inputTask.lower() == 'ingestsourcemeta':
        parser.add_argument("--inputDataSource", help="Input data source to be processed", action="store", dest="inputDataSource", required=True)
        parser.add_argument("--inputSourceName", help="Input source name to be processed", action="store", dest="inputSourceName", choices=['adcirc','ncem','noaa','ndbc'], required=True)
        parser.add_argument("--inputSourceArchive", help="Input source archive the data is from", action="store", dest="inputSourceArchive", choices=['renci','contrails','noaa','ndbc'], required=True)
        parser.add_argument("--inputSourceVariable", help="Input source variables", action="store", dest="inputSourceVariable", required=True)
        parser.add_argument("--inputFilenamePrefix", help="Input filename variables", action="store", dest="inputFilenamePrefix", required=True)
        parser.add_argument("--inputLocationType", help="Input location type to be processed", action="store", dest="inputLocationType", required=True)
        parser.add_argument("--inputDataType", help="Input data type to be processed", action="store", dest="inputDataType", required=True)
        parser.add_argument("--inputUnits", help="Input units", action="store", dest="inputUnits", required=True)
    elif args.inputTask.lower() == 'ingeststations':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'ingestsourcedata':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'ingestharvestdatafilemeta':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'ingestapsvizstationfilemeta':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
        parser.add_argument("--inputFileName", "--inputFilename", help="Input filename for apzViz station meta file", action="store", dest="inputFilename", required=True)
    elif args.inputTask.lower() == 'ingestdata':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
        parser.add_argument("--inputDataSource", help="Input data source to be processed", action="store", dest="inputDataSource", required=True)
        parser.add_argument("--databaseDIR", "--databaseDir", help="Database directory path", action="store", dest="databaseDir", required=False)
        parser.add_argument("--inputSourceName", help="Input source name to be processed", action="store", dest="inputSourceName", choices=['adcirc','ncem','noaa','ndbc'], required=True)
        parser.add_argument("--inputSourceArchive", help="Input source archive the data is from", action="store", dest="inputSourceArchive", choices=['renci','contrails','noaa','ndbc'], required=True)
        parser.add_argument("--inputSourceVariable", help="Input source variables", action="store", dest="inputSourceVariable", required=True)
    elif args.inputTask.lower() == 'ingestapsvizstationdata':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
        parser.add_argument("--inputFilename", help="Input filename to be processed", action="store", dest="inputFilename", required=True)

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

