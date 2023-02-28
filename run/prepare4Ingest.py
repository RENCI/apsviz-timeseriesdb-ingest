#!/usr/bin/env python
# coding: utf-8

# import modules
import os
import sys
import argparse
import shutil
import psycopg
import subprocess
import pandas as pd
from loguru import logger

# This function moves the station data files in /nru/home/stations to the directory /data/DataIngesting/DAILY_INGEST/, and then ingests them 
# into the drf_gauge_station table. 
def runIngestStations(ingestDir):
    # Move station meta files to the /data/DataIngesting/DAILY_INGEST/
    logger.info('Copy stations directory to '+ingestDir)
    shutil.copytree('/home/nru/stations', ingestDir+'stations', dirs_exist_ok=True)

    # Create list of program commands
    program_list = [['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','IngestStations']]

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

# This programs reads the source meta information from source_meta.csv, and ingests it into the drf_source_meta table. The drf_source_meta
# table is used by runIngestSource(), by runing getSourceMeta(), to get input variables for createIngestSourceMeta.py
def runIngestSourceMeta():
    # Create list of program commands for ingesting source meta
    df = pd.read_csv('/home/nru/source_meta.csv', index_col=False)

    program_list = []
    # data_source,source_name,source_archive,location_type
    for index, row in df.iterrows():
        program_list.append(['python','ingestTasks.py','--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],'--inputSourceVariable',row['source_variable'],'--inputFilenamePrefix',row['filename_prefix'],'--inputLocationType',row['location_type'],'--inputUnits',row['units'],'--inputTask','Source_meta'])

    # Run programe list using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

# This function is used by the runIngestSource() function to query the drf_source_meta table, in the 
# database, and get argparse input for those function
def getSourceMeta():
    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT data_source, source_name, source_archive, source_variable, filename_prefix, location_type, units FROM drf_source_meta""")

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['data_source', 'source_name', 'source_archive', 'source_variable', 'filename_prefix', 'location_type', 'units'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function runs createIngestSourceMeta.py which creates source data files that are then ingested into the drf_gauge_source table, 
# in the database, by running ingestTasks.py using --inputTask Sourc_data.
def runIngestSource(ingestDir):
    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createIngestSourceMeta.py','--ingestDir',ingestDir,'--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],'--inputUnits',row['units'],'--inputLocationType',row['location_type']])

    program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestSource'])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

# This function runs ingestTasks.py with --inputTask View, creating a view (drf_gauge_station_source_data) that combines the drf_gauge_station, 
# drf_gauge_source, and drf_gauge_data tables.
def runCreateView():
    # Create list of program commands
    program_list = [['python','ingestTasks.py','--inputTask','View']]
 
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

# This functions ingest the stations, creates and ingest the source in sequence
def runSequenceIngest(ingestDir):
    runCreateView()
    runIngestStations(ingestDir)
    runIngestSourceMeta()
    runIngestSource(ingestDir)

# Main program function takes args as input, which contains the ingestDir, inputTask as variables.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'prepare4Ingest.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Check if ingestDir argument exist. This argument is used in runIngestStations.
    if args.ingestDir is None:
        ingestDir = ''
    elif args.ingestDir is not None:
        ingestDir = os.path.join(args.ingestDir, '')
    else:
        sys.exit('Incorrect ingestDir')

    # Get input task
    inputTask = args.inputTask

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'ingeststations':
        logger.info('Run ingest station data.')
        runIngestStations(ingestDir)
        logger.info('Ran ingest station data.')
    elif inputTask.lower() == 'ingestsourcemeta':
        logger.info('Run source meta.')
        runIngestSourceMeta()
        logger.info('Ran source meta.')
    elif inputTask.lower() == 'ingestsource':
        logger.info('Run source data.')
        runIngestSource(ingestDir)
        logger.info('Ran source data.')
    elif inputTask.lower() == 'view':
        logger.info('Run create view.')
        runCreateView()
        logger.info('Ran create view.')
    elif inputTask.lower() == 'sequenceingest':
        logger.info('Run sequence ingest.')
        runSequenceIngest(ingestDir)
        logger.info('Ran sequence ingest.')

# Run main function 
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=False)
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['IngestStations','IngestSourceMeta','IngestSource','View', 'SequenceIngest'], required=True)

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

