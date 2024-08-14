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
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestObsFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
        Returns
            None, but it runs ingestObsTasks.py, which ingest station data from CSV files in /home/nru/stations into the 
            drf_gauge_station table.
    '''

    # Move station meta files to the /data/DataIngesting/DAILY_INGEST/
    logger.info('Copy stations directory to '+ingestDir)
    shutil.copytree('/home/nru/stations', ingestDir+'stations', dirs_exist_ok=True)

    # Create list of program commands
    program_list = [['python','ingestObsTasks.py','--ingestDir',ingestDir,'--inputTask','ingestStations']]

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

# This programs reads the source meta information from source_obs_meta.csv, and ingests it into the drf_source_obs_meta table. The drf_source_obs_meta
# table is used by runIngestObsSourceData(), by runing getSourceObsMeta(), to get input variables for createIngestObsSourceMeta.py
def runIngestObsSourceMeta():
    ''' This programs reads the source meta information from source_obs_meta.csv, and ingests it into the drf_source_obs_meta table. 
        The drf_source_obs_meta table is used by runIngestObsSourceData(), by runing getSourceObsMeta(), to get input variables for 
        createIngestObsSourceMeta.py.
        Parameters
            None
        Returns
            None, but it runs ingestObsTasks.py, which ingest source meta data, from the CSV file /home/nru/source_obs_meta.csv into the
            drf_source_obs_meta table.
    '''

    # Create list of program commands for ingesting source meta
    df = pd.read_csv('/home/nru/source_obs_meta.csv', index_col=False)

    program_list = []
    # data_source,source_name,source_archive,location_type
    for index, row in df.iterrows():
        program_list.append(['python','ingestObsTasks.py','--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],
                             '--inputSourceArchive',row['source_archive'],'--inputSourceVariable',row['source_variable'],
                             '--inputFilenamePrefix',row['filename_prefix'],'--inputLocationType',row['location_type'],
                             '--inputUnits',row['units'],'--inputTask','ingestSourceMeta'])

    # Run programe list using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def getSourceObsMeta():
    ''' Returns a DataFrame containing source meta-data from the drf_source_obs_meta table.
        Parameters
            None
        Returns
            DataFrame
    '''

    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], 
                               user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                               host=os.environ['APSVIZ_GAUGES_DB_HOST'], 
                               port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                               password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'])
        cur = conn.cursor()

        # Run query
        cur.execute("""SELECT data_source, source_name, source_archive, source_variable, filename_prefix, location_type, units FROM drf_source_obs_meta""")

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['data_source', 'source_name', 'source_archive', 'source_variable', 'filename_prefix', 'location_type', 'units'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.exception(error)

def runIngestObsSourceData(ingestDir):
    ''' This function runs createIngestObsSourceMeta.py which creates source data files that are then ingested into the drf_gauge_source 
        table, in the database, by running ingestObsTasks.py using --inputTask ingestObsSourceData.
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestObsFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
        Returns
            None, but it runs createIngestObsSourceMeta.py, which creates CSV files containing source meta-data, and then runs 
            ingestObsTasks.py which ingest the CSV file into the drf_gauge_source table.
    '''

    # get source meta
    df = getSourceObsMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createIngestObsSourceMeta.py','--ingestDir',ingestDir,'--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],'--inputUnits',row['units'],'--inputLocationType',row['location_type']])

    program_list.append(['python','ingestObsTasks.py','--ingestDir',ingestDir,'--inputTask','ingestSourceData'])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runCreateObsView():
    ''' This function runs ingestObsTasks.py with --inputTask createObsView, creating a view (drf_gauge_station_source_data) that combines 
        the drf_gauge_station, drf_gauge_source, and drf_gauge_data tables.
        Parameters
            None
        Returns
            None
    ''' 

    # Create list of program commands
    program_list = [['python','ingestObsTasks.py','--inputTask','createObsView']]
 
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runCreateModelView():
    ''' This function runs ingestModelTasks.py with --inputTask createModelView, creating a view (drf_gauge_station_source_data) that combines 
        the drf_gauge_station, drf_model_source, drf_model_instance, and drf_model_data tables.
        Parameters
            None
        Returns
            None
    ''' 

    # Create list of program commands
    program_list = [['python','ingestModelTasks.py','--inputTask','createModelView']]
 
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runSequenceIngest(ingestDir):
    ''' Runs the runCreateObsView(), runIngestStations(), runIngestObsSourceMeta(), and runIngestObsSourceData() functions in sequence. 
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestObsFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
        Returns
            None, but the functions it calls return values, described above.
    '''

    runCreateObsView()
    runIngestStations(ingestDir)
    runIngestObsSourceMeta()
    runIngestObsSourceData(ingestDir)
    runCreateModelView()

@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, and runs specified task.
        Parameters
            args: dictionary
                contains the parameters listed below
            inputTask: string
                The type of task ('IngestStations','ingestSourceMeta','ingesObsSourceData','createObsView','createModelView,'SequenceIngest'
                SequenceIngest) to be perfomed.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestObsFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
        Returns
            None
    '''

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'prepare4Ingest.log', level='DEBUG', rotation="1 MB")
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
        runIngestObsSourceMeta()
        logger.info('Ran source meta.')
    elif inputTask.lower() == 'ingestobssourcedata':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Run source data.')
        runIngestObsSourceData(ingestDir)
        logger.info('Ran source data.')
    elif inputTask.lower() == 'createobsview':
        logger.info('Run create obs view.')
        runCreateObsView()
        logger.info('Ran create obs view.')
    elif inputTask.lower() == 'createmodelview':
        logger.info('Run create model view.')
        runCreateModelView()
        logger.info('Ran create model view.')
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
                The type of task ('IngestStations','ingestObsSourceMeta','ingestObsSourceData','createObsView','createModelView',createModelView,'SequenceIngest'
                SequenceIngest) to be perfomed. The type of inputTaks can change what other types of inputs prepare4Ingest.py
                requires. Below is a list of all inputs, with associated tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestStations, ingestObsSourceData, and sequenceIngest.
        Returns
            None
    '''         

    parser = argparse.ArgumentParser()

    # Non optional argument, input task 
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['IngestStations','ingestSourceMeta',
                        'ingestObsSourceData','createObsView','createModelView','SequenceIngest'], required=True)

    # get runScript argument to use in if statement
    args = parser.parse_known_args()[0]

    # Optional arguments
    if args.inputTask.lower() == 'ingeststations':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'ingestobssourcedata':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'sequenceingest':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
 
    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

