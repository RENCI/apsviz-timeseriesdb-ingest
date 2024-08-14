#!/usr/bin/env python
# coding: utf-8

# import modules
import os
import sys
import re
import argparse
import shutil
import psycopg
import subprocess
import pandas as pd
from loguru import logger

def getSourceMeta():
    ''' Returns DataFrame containing source meta-data queried from the drf_source_obs_meta table. 
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
        cur.execute("""SELECT data_source, source_name, source_archive, source_variable, filename_prefix, location_type, units 
                       FROM drf_source_obs_meta 
                       ORDER BY filename_prefix""")

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

def getRetainObsStationInfo():
    ''' Returns DataFrame containing variables from the drf_retain_obs_station_file_meta table. It takes a model run ID as input.
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
        cur.execute("""SELECT dir_path,file_name,data_source,source_name,source_archive,
                              timemark,location_type,begin_date,end_date,ingested
                       FROM drf_retain_obs_station_file_meta
                       WHERE ingested = False""")

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['dir_path','file_name','data_source','source_name','source_archive',
                                                   'timemark','location_type','begin_date','end_date','ingested'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.exception(error)

def runHarvestFile(harvestDir, ingestDir):
    ''' This function runs createHarvestObsFileMeta.py, which creates harvest meta data files, that are ingested into the 
        drf_harvest_obs_file_meta table, in the database, by running ingestObsTasks.py using --inputTask ingestHarvestDataFileMeta.
        Parameters
            harvestDir: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, and SequenceIngest.
        Returns
            None, but it create harvest meta data files, that are then ingested into the drf_harvest_obs_file_meta table.
    '''

    # get source meta
    df = getSourceMeta()

    # Create list of program commands to ingest meta-data on harvest files
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createHarvestObsFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource', row['data_source'],
                             '--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],'--inputSourceVariable',row['source_variable'],
                             '--inputFilenamePrefix',row['filename_prefix'],'--inputLocationType',row['location_type']])

    # Ingest meta-data on harvest files created above
    program_list.append(['python','ingestObsTasks.py','--ingestDir',ingestDir,'--inputTask','ingestHarvestDataFileMeta'])
    
    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

    # Create list of program commands to ingest meta-data on the obs meta files, which contain station information, into the drf_retain_obs_station_file_meta table
    program_list = []

    # Loop through all obs data sources
    for index, row in df.iterrows():
        # Add meta to filename_prefix, so it can be used to find meta files.
        meta_filename_prefix = "stationdata_meta".join(row['filename_prefix'].split('stationdata'))

        # Create list of command line calls to createRetainObsStationFileMeta.py
        program_list.append(['python','createRetainObsStationFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource',
                             row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],
                             '--inputLocationType',row['location_type'],'--inputFilenamePrefix',meta_filename_prefix])

    # Ingest meta-data on harvest files created above
    program_list.append(['python','ingestObsTasks.py','--ingestDir',ingestDir,'--inputTask','ingestRetainObsStationFileMeta'])

    # Run list of program commands using subprocess
    if len(program_list) > 0:
        for program in program_list:
            logger.info('Run '+" ".join(program))
            output = subprocess.run(program, shell=False, check=True)
            logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))
    else:
        logger.info('Program list has 0 length, so continue')

def runDataCreate(ingestDir):
    ''' This function runs createIngestObsData.py, which ureates gauge data, from the original harvest data files, that will be 
        ingested into the database using the runDataIngest function.
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, and SequenceIngest.
        Returns
            None, but it runs createIngestObsData.py, which returns a CSV file
    '''

    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createIngestObsData.py','--ingestDir',ingestDir,'--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],
                             '--inputSourceArchive',row['source_archive']])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runDataIngest(ingestDir):
    ''' This function runs ingestObsTasks.py with --inputTask ingestData, ingest gauge data into the drf_gauge_data table, in the database. 
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, and SequenceIngest.   
        Returns
            None, but it runs ingestTask.py, which ingest data into the drf_gauge_station table.
    '''

    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','ingestObsTasks.py','--ingestDir',ingestDir,'--inputTask','ingestData','--inputDataSource',row['data_source'],
                             '--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'], '--inputSourceVariable',row['source_variable']])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runRetainObsStationCreateIngest(ingestDir):
    ''' This function creates and ingests the obs station data from the obs meta files, adding a timemark, begin_date, end_date, 
        data_source, and source_archive. It gets this information from the from the drf_retain_obs_station_file_meta table, by running the 
        getRetainObsStationInfo() function
        Parameters 
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. 
        Returns
            None, but it runs createIngestRetainObsStationData.py, which outputs a CSV file, and then it runs ingestTask.py
            which ingest the CSV files into the drf_retain_obs_station table.
    ''' 

    logger.info('Create Retain Obs station file data, to be ingested into the drf_retain_obs_station table ')
    df = getRetainObsStationInfo() 

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        timeMark = "T".join(str(row['timemark']).split('+')[0].split(' '))
        beginDate = "T".join(str(row['begin_date']).split('+')[0].split(' '))
        endDate = "T".join(str(row['end_date']).split('+')[0].split(' '))
        
        program_list.append(['python','createIngestRetainObsStationData.py','--harvestDir',row['dir_path'],'--ingestDir',ingestDir,
                             '--inputFilename',row['file_name'],'--timeMark',timeMark,'--inputDataSource',row['data_source'],
                             '--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],'--inputLocationType',row['location_type'],
                             '--beginDate',beginDate,'--endDate',endDate])
        
        logger.info('Create ingest command for Retain Obs station file data, with filename '+row['file_name']+', to ingest into the drf_retain_obs_station table ')

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+' with output returncode '+str(output.returncode))

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','ingestObsTasks.py','--inputFilename',row['file_name'],'--ingestDir',ingestDir,'--inputTask','ingestRetainObsStationData'])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runSequenceIngest(harvestDir, ingestDir):
    ''' Runs the runHarvestFile(), runDataCreate(), and runDataIngest() functions in sequence. 
        Parameters
            harvestDir: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, and SequenceIngest.
        Returns
            None, but the functions it calls return values, described above.
    '''

    runHarvestFile(harvestDir, ingestDir)
    runDataCreate(ingestDir)
    runDataIngest(ingestDir)

    logger.info('Ingest station meta data into the drf_retain_obs_station table')
    runRetainObsStationCreateIngest(ingestDir)

@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, and runs specified task.
        Parameters
            args: dictionary
                contains the parameters listed below.
            inputTask: string
                The type of task (ingestHarvestDataFileMeta, DataCreate, DataIngest, SequenceIngest) to be perfomed. The type of inputTaks 
                can change what other types of inputs runInget.py requires. Below is a list of all inputs, with associated tasks.
            harvestDir: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, and SequenceIngest.
        Returns
            None
    '''         

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'runObsIngest.log', level='DEBUG', rotation="5 MB")
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Get input task
    inputTask = args.inputTask

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'ingestharvestdatafilemeta':
        harvestDir = os.path.join(args.harvestDir, '')
        ingestDir = os.path.join(args.ingestDir, '') 
        logger.info('Run input file information.')
        runHarvestFile(harvestDir, ingestDir)
        logger.info('Ran input file information.')
    elif inputTask.lower() == 'datacreate':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Run data create.')
        runDataCreate(ingestDir)
        logger.info('Ran data create.')
    elif inputTask.lower() == 'dataingest':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Run data ingest.')
        runDataIngest(ingestDir)
        logger.info('Ran data ingest.')
    elif inputTask.lower() == 'runretainobsstationcreateingest':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Run Retain Obs station create-ingest.')
        runRetainObsStationCreateIngest(ingestDir)
        logger.info('Ran Retain Obs station create-ingest.')
    elif inputTask.lower() == 'sequenceingest':
        harvestDir = os.path.join(args.harvestDir, '')
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Run sequence ingest.')
        runSequenceIngest(harvestDir, ingestDir)
        logger.info('Ran sequence ingest.')

# Run main function 
if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            inputTask: string
                The type of task (ingestHarvestDataFileMeta, DataCreate, DataIngest, SequenceIngest) to be perfomed. The type of inputTask 
                can change what other types of inputs runInget.py requires. Below is a list of all inputs, with associated tasks. 
            harvestDir: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate, 
                DataIngest, SequenceIngest.
        Returns
            None
    '''         

    parser = argparse.ArgumentParser()

    # Non optional argument
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['ingestHarvestDataFileMeta',
                        'DataCreate','DataIngest','runRetainObsStationCreateIngest','SequenceIngest'], required=True)

    # get runScript argument to use in if statement
    args = parser.parse_known_args()[0]

    # Optional argument
    if args.inputTask.lower() == 'ingestharvestdatafilemeta':
        parser.add_argument("--harvestDIR", "--harvestDir", help="Harvest directory path", action="store", dest="harvestDir", required=True)
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'datacreate':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'dataingest':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'runretainobsstationcreateingest':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'sequenceingest':
        parser.add_argument("--harvestDIR", "--harvestDir", help="Harvest directory path", action="store", dest="harvestDir", required=True)
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    else:
        logger.info(args.inputTask+' not correct')

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

