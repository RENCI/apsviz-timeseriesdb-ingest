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

def runIngestStations(ingestDir):
    ''' This function moves the station data files in /nru/home/stations to the directory /data/ast-run-ingester/, 
        and then ingests them into the drf_gauge_station table.
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
        Returns
            None, but it runs ingestTasks.py, which ingest station data from CSV files in /home/nru/stations into the 
            drf_gauge_station table.
    '''

    # Move station meta files to the /data/DataIngesting/DAILY_INGEST/
    logger.info('Copy stations directory to '+ingestDir)
    shutil.copytree('/home/nru/stations', ingestDir+'stations', dirs_exist_ok=True)

    # Create list of program commands
    program_list = [['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestStations']]

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

# This programs reads the source meta information from source_meta.csv, and ingests it into the drf_source_meta table. The drf_source_meta
# table is used by runIngestSourceData(), by runing getSourceMeta(), to get input variables for createIngestSourceMeta.py
def runIngestSourceMeta():
    ''' This programs reads the source meta information from source_meta.csv, and ingests it into the drf_source_meta table. 
        The drf_source_meta table is used by runIngestSourceData(), by runing getSourceMeta(), to get input variables for 
        createIngestSourceMeta.py.
        Parameters
            None
        Returns
            None, but it runs ingestTasks.py, which ingest source meta data, from the CSV file /home/nru/source_meta.csv into the
            drf_source_meta table.
    '''

    # Create list of program commands for ingesting source meta
    df = pd.read_csv('/home/nru/source_meta.csv', index_col=False)

    program_list = []
    # data_source,source_name,source_archive,location_type
    for index, row in df.iterrows():
        program_list.append(['python','ingestTasks.py','--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],'--inputSourceVariable',row['source_variable'],'--inputFilenamePrefix',row['filename_prefix'],'--inputLocationType',row['location_type'],'--dataType',row['data_type'],'--inputUnits',row['units'],'--inputTask','ingestSourceMeta'])

    # Run programe list using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def getSourceMeta():
    ''' Returns a DataFrame containing source meta-data from the drf_source_meta table.
        Parameters
            None
        Returns
            DataFrame
    '''

    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], host=os.environ['APSVIZ_GAUGES_DB_HOST'], port=os.environ['APSVIZ_GAUGES_DB_PORT'], password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'])
        cur = conn.cursor()

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

def runIngestSourceData(ingestDir):
    ''' This function runs createIngestSourceMeta.py which creates source data files that are then ingested into the drf_gauge_source 
        table, in the database, by running ingestTasks.py using --inputTask ingestSourceData.
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
        Returns
            None, but it runs createIngestSourceMeta.py, which creates CSV files containing source meta-data, and then runs 
            ingestTasks.py which ingest the CSV file into the drf_gauge_source table.
    '''

    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createIngestSourceMeta.py','--ingestDir',ingestDir,'--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],'--inputUnits',row['units'],'--inputLocationType',row['location_type']])

    program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestSourceData'])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runCreateView():
    ''' This function runs ingestTasks.py with --inputTask createView, creating a view (drf_gauge_station_source_data) that combines 
        the drf_gauge_station, drf_gauge_source, and drf_gauge_data tables.
        Parameters
            None
        Returns
            None
    ''' 

    # Create list of program commands
    program_list = [['python','ingestTasks.py','--inputTask','createView']]
 
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runSequenceIngest(ingestDir):
    ''' Runs the runCreateView(), runIngestStations(), runIngestSourceMetat(), and runIngestSourceData() functions in sequence. 
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
        Returns
            None, but the functions it calls return values, described above.
    '''

    runCreateView()
    runIngestStations(ingestDir)
    runIngestSourceMeta()
    runIngestSourceData(ingestDir)

@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, and runs specified task.
        Parameters
            args: dictionary
                contains the parameters listed below
            inputTask: string
                The type of task ('IngestStations','ingestSourceMeta','ingestSourceData','createView', 'SequenceIngest'
                SequenceIngest) to be perfomed.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
        Returns
            None
    '''

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'prepare4Ingest.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Get input task
    inputTask = args.inputTask

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'ingeststations':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Run ingest station data.')
        runIngestStations(ingestDir)
        logger.info('Ran ingest station data.')
    elif inputTask.lower() == 'ingestsourcemeta':
        logger.info('Run source meta.')
        runIngestSourceMeta()
        logger.info('Ran source meta.')
    elif inputTask.lower() == 'ingestsourcedata':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Run source data.')
        runIngestSourceData(ingestDir)
        logger.info('Ran source data.')
    elif inputTask.lower() == 'createview':
        logger.info('Run create view.')
        runCreateView()
        logger.info('Ran create view.')
    elif inputTask.lower() == 'sequenceingest':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Run sequence ingest.')
        runSequenceIngest(ingestDir)
        logger.info('Ran sequence ingest.')

# Run main function 
if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            inputTask: string
                The type of task ('IngestStations','ingestSourceMeta','ingestSourceData','createView', 'SequenceIngest'
                SequenceIngest) to be perfomed. The type of inputTaks can change what other types of inputs prepare4Ingest.py
                requires. Below is a list of all inputs, with associated tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestStations, ingestSourceData, and sequenceIngest.
        Returns
            None
    '''         

    parser = argparse.ArgumentParser()

    # Non optional argument, input task 
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['IngestStations','ingestSourceMeta',
                        'ingestSourceData','createView', 'SequenceIngest'], required=True)

    # get runScript argument to use in if statement
    args = parser.parse_known_args()[0]

    # Optional arguments
    if args.inputTask.lower() == 'ingeststations':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'ingestsourcedata':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'sequenceingest':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
 
    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

