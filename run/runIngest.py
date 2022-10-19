#!/usr/bin/env python
# coding: utf-8

# import modules
import os
import sys
import argparse
import psycopg2
import subprocess
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

# import .env file
load_dotenv()

# Run createIngestStationMeta.py to ingest station data from the original station data tables (i.e. dbo_gages_all, ndbc_stations, and noaa_stations), 
# into the drf_gauge_station table. Ingesting the original station data was a step when installing the apsviz-timeseriesdb repo:
#     https://github.com/RENCI/apsviz-timeseriesdb 
def runStation():
    # Create list of program commands
    program_list = [['python','createIngestStationMeta.py','--outputDir','/data/DataIngesting/DAILY_INGEST/','--outputFile','noaa_stationdata_tidal_meta.csv'],
                    ['python','createIngestStationMeta.py','--outputDir','/data/DataIngesting/DAILY_INGEST/','--outputFile','ndbc_stationdata_tidal_meta.csv'],
                    ['python','createIngestStationMeta.py','--outputDir','/data/DataIngesting/DAILY_INGEST/','--outputFile','contrails_stationdata_coastal_meta.csv'],
                    ['python','createIngestStationMeta.py','--outputDir','/data/DataIngesting/DAILY_INGEST/','--outputFile','contrails_stationdata_river_meta.csv'],
                    ['python','ingestTasks.py','--inputDir','/data/DataIngesting/DAILY_INGEST/','--ingestDir','/home/DataIngesting/DAILY_INGEST/','--inputTask','Station']]

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        subprocess.call(program)
        logger.info('Ran '+" ".join(program))

# This function is used by the runSourceData(), runHarvestFile(), runDataCreate(), and runDataIngest() functions to query the drf_source_meta table, in the 
# database, and get argparse input for those function
def getSourceMeta():
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT data_source, source_name, source_archive, location_type FROM drf_source_meta""")

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['data_source', 'source_name', 'source_archive', 'location_type'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg2.DatabaseError) as error:
        logger.info(error)

# This function runs createIngestSourceMeta.py which creates source data files that are then ingested into the drf_gauge_source table, 
# in the database, by running ingestTasks.py using --inputTask Sourc_data.
def runSourceData():
    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createIngestSourceMeta.py','--outputDir','/data/DataIngesting/DAILY_INGEST/','--outputFile',row['source_name']+'_stationdata_'+row['source_archive']+'_'+row['location_type']+'_'+row['data_source']+'_meta.csv'])

    program_list.append(['python','ingestTasks.py','--inputDir','/data/DataIngesting/DAILY_INGEST/','--ingestDir','/home/DataIngesting/DAILY_INGEST/','--inputTask','Source_data'])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        subprocess.call(program)
        logger.info('Ran '+" ".join(program))

# This function runs createHarvestFileMeta.py, which creates harvest meta data files, that are ingested into the drf_harvest_data_file_meta table, 
# in the database, by running ingestTasks.py using --inputTask File.
def runHarvestFile():
    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createHarvestFileMeta.py','--inputDir','/data/DataHarvesting/DAILY_HARVESTING/','--outputDir','/data/DataIngesting/DAILY_INGEST/','--inputDataSource', row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive']])

    program_list.append(['python','ingestTasks.py','--inputDir','/data/DataIngesting/DAILY_INGEST/','--ingestDir','/home/DataIngesting/DAILY_INGEST/','--inputTask','File'])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        subprocess.call(program)
        logger.info('Ran '+" ".join(program))

# This function runs createIngestData.py, which ureates gauge data, from the original harvest data files, that will be ingested into the 
# database using the runDataIngest function. 
def runDataCreate():
    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createIngestData.py','--outputDir','/data/DataIngesting/DAILY_INGEST/','--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive']])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        subprocess.call(program)
        logger.info('Ran '+" ".join(program))

# This function runs ingestTasks.py with --inputTask Data, ingest gauge data into the drf_gauge_data table, in the database.
def runDataIngest():
    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','ingestTasks.py','--ingestDir','/home/DataIngesting/DAILY_INGEST/','--inputTask','Data','--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive']])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        subprocess.call(program)
        logger.info('Ran '+" ".join(program))

# This function runs ingestTasks.py with --inputTask View, creating a view (drf_gauge_station_source_data) that combines the drf_gauge_station, 
# drf_gauge_source, and drf_gauge_data tables.
def runCreateView():
    # Create list of program commands
    program_list = [['python','ingestTasks.py','--inputTask','View']]

    for program in program_list:
        logger.info('Run '+" ".join(program))
        subprocess.call(program)
        logger.info('Ran '+" ".join(program))

# Main program function takes args as input, which contains the inputDir, ingestDir, inputTask, inputDataSource, inputSourceName, and inputSourceArchive values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'runIngest.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Get input task
    inputTask = args.inputTask

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'station':
        logger.info('Run station data.')
        runStation()
        logger.info('Ran station data.')
    elif inputTask.lower() == 'source_data':
        logger.info('Run source data.')
        runSourceData()
        logger.info('Ran source data.')
    elif inputTask.lower() == 'file':
        logger.info('Run input file information.')
        runHarvestFile()
        logger.info('Ran input file information.')
    elif inputTask.lower() == 'datacreate':
        logger.info('Run data create.')
        runDataCreate()
        logger.info('Ran data create.')
    elif inputTask.lower() == 'dataingest':
        logger.info('Run data ingest.')
        runDataIngest()
        logger.info('Ran data ingest.')
    elif inputTask.lower() == 'view':
        logger.info('Run create view.')
        runCreateView()
        logger.info('Ran create view.')

# Run main function 
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['Station','Source_data','File','DataCreate','DataIngest','View'], required=True)

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

