#!/usr/bin/env python
# coding: utf-8

# import modules
import os
import sys
import argparse
import shutil
import psycopg2
import subprocess
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

# import .env file
load_dotenv()

# This function is used by the runHarvestFile(), runDataCreate(), and runDataIngest() functions to query the drf_source_meta table, in the
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

# This function runs createHarvestFileMeta.py, which creates harvest meta data files, that are ingested into the drf_harvest_data_file_meta table, 
# in the database, by running ingestTasks.py using --inputTask File.
def runHarvestFile(harvestDir, ingestDir, databaseDir):
    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createHarvestFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource', row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive']])

    program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--databaseDir',databaseDir,'--inputTask','File'])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        subprocess.call(program)
        logger.info('Ran '+" ".join(program))

# This function runs createIngestData.py, which ureates gauge data, from the original harvest data files, that will be ingested into the 
# database using the runDataIngest function. 
def runDataCreate(ingestDir):
    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createIngestData.py','--ingestDir',ingestDir,'--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive']])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        subprocess.call(program)
        logger.info('Ran '+" ".join(program))

# This function runs ingestTasks.py with --inputTask Data, ingest gauge data into the drf_gauge_data table, in the database.
def runDataIngest(ingestDir, databaseDir):
    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--databaseDir',databaseDir,'--inputTask','Data','--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive']])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        subprocess.call(program)
        logger.info('Ran '+" ".join(program))

# This functions creates and ingest the harvest files, and data in sequence
def runSequenceIngest(harvestDir, ingestDir, databaseDir):
    runHarvestFile(harvestDir, ingestDir, databaseDir)
    runDataCreate(ingestDir)
    runDataIngest(ingestDir, databaseDir)

# Main program function takes args as input, which contains the harvestDir, ingestDir, databaseDir, and inputTask variables
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'runIngest.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Check if inputDir argument exist. This argument is used in runIngestStations.
    if args.harvestDir is None:
        harvestDir = ''
    elif args.harvestDir is not None:
        harvestDir = os.path.join(args.harvestDir, '')
    else:
        sys.exit('Incorrect harvestDir')

    # Check if ingestDir argument exist. This argument is used in runCreateStations.
    if args.ingestDir is None:
        ingestDir = ''
    elif args.ingestDir is not None:
        ingestDir = os.path.join(args.ingestDir, '')
    else:
        sys.exit('Incorrect ingestDir')

    # Check if ingestDir argument exist. This argument is used in runIngestStations.
    if args.databaseDir is None:
        databaseDir = ''
    elif args.databaseDir is not None:
        databaseDir = os.path.join(args.databaseDir, '')
    else:
        sys.exit('Incorrect databaseDir')

    # Get input task
    inputTask = args.inputTask

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'file':
        logger.info('Run input file information.')
        runHarvestFile(harvestDir, ingestDir, databaseDir)
        logger.info('Ran input file information.')
    elif inputTask.lower() == 'datacreate':
        logger.info('Run data create.')
        runDataCreate(ingestDir)
        logger.info('Ran data create.')
    elif inputTask.lower() == 'dataingest':
        logger.info('Run data ingest.')
        runDataIngest(ingestDir, databaseDir)
        logger.info('Ran data ingest.')
    elif inputTask.lower() == 'sequenceingest':
        logger.info('Run sequence ingest.')
        runSequenceIngest(harvestDir, ingestDir, databaseDir)
        logger.info('Ran sequence ingest.')


# Run main function 
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--harvestDIR", "--harvestDir", help="Harvest directory path", action="store", dest="harvestDir", required=False)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=False)
    parser.add_argument("--databaseDIR", "--databaseDir", help="Database directory path", action="store", dest="databaseDir", required=False)
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['File','DataCreate','DataIngest','SequenceIngest'], required=True)

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

