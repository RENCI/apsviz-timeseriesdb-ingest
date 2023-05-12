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
import getDashboardMeta as gdm
from loguru import logger

# This function is used by the runIngestSource() function to query the drf_source_meta table, in the
# database, and get argparse input for those function
def getSourceMeta():
    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD'])
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

# This function runs createHarvestFileMeta.py, which creates harvest meta data files, that are ingested into the drf_harvest_data_file_meta table, 
# in the database, by running ingestTasks.py using --inputTask ingestHarvestDataFileMeta.
def runHarvestFile(harvestDir, ingestDir, modelRunID):
    if modelRunID != None:
        # Sources from drf_source_meta will not be used in this run
        logger.info('Process data for ADCIRC model run: '+modelRunID)

        # Get input file name, grid, timemark, and stormtrack
        inputPathFileGrid = gdm.getInputFile(harvestDir,ingestDir,modelRunID)
        inputFile = inputPathFileGrid[0].split('/')[-1]
        grid = inputPathFileGrid[1]
        advisory = inputPathFileGrid[2]
        timemark = inputPathFileGrid[3]
        stormtrack = inputPathFileGrid[4]

        # Split input file name and extract source meta variables from it
        inputFileParts = inputFile.split('_')
        stormnum = inputFileParts[1]
        scenario = inputFileParts[3]
        source_name = inputFileParts[0]
        source_archive = inputFileParts[2].lower()
        modeltype = inputFile.split(grid)[1].split('_')[1]
        if stormtrack == 'notrack':
            data_source = scenario+'_'+grid
            filename_prefix = source_name+'_'+stormnum+'_'+source_archive.upper()+'_'+scenario+'_'+grid+'_'+modeltype
            filename_prefix_ta = filename_prefix+'_'+timemark
        else:
            data_source = stormnum+'_'+scenario+'_'+grid
            filename_prefix = source_name+'_'+stormnum+'_'+source_archive.upper()+'_'+scenario+'_'+grid+'_'+modeltype
            filename_prefix_ta = filename_prefix+'_'+advisory

        # Define other source meta variables
        source_variable = 'water_level'
        location_type = 'tidal'
        units = 'm'
    
        # Check to see if source exists
        dfcheck = gdm.checkSourceMeta(filename_prefix)
    
        if dfcheck.empty:
            # Log results
            logger.info('The following source does not exist in the drf_source_meta table:\n '+
                        data_source+','+source_name+','+source_archive+','+source_variable+','+
                        filename_prefix_ta+','+location_type+','+units)
            program_list = []
            program_list.append(['python','ingestTasks.py','--inputDataSource',data_source,'--inputSourceName',source_name,'--inputSourceArchive',
                                 source_archive,'--inputSourceVariable',source_variable,'--inputFilenamePrefix',filename_prefix,'--inputLocationType',
                                 location_type,'--inputUnits',units,'--inputTask','ingestSourceMeta'])
            program_list.append(['python','createIngestSourceMeta.py','--ingestDir',ingestDir,'--inputDataSource',data_source,'--inputSourceName',
                                 source_name,'--inputSourceArchive',source_archive,'--inputUnits',units,'--inputLocationType',location_type])
            program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestSourceData'])
            program_list.append(['python','createHarvestFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource', 
                                 data_source,'--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputFilenamePrefix',filename_prefix_ta])
            program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestHarvestDataFileMeta'])
 
            # Run list of program commands using subprocess
            for program in program_list:
                logger.info('Run '+" ".join(program))
                output = subprocess.run(program, shell=False, check=True)
                logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

        else:
            # log results
            logger.info(data_source+','+source_name+','+source_archive+','+source_variable+','+
                        filename_prefix_ta+','+location_type+','+units)
            program_list = []
            program_list.append(['python','createHarvestFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource', data_source,'--inputSourceName',
                                 source_name,'--inputSourceArchive',source_archive,'--inputFilenamePrefix',filename_prefix_ta])
            program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestHarvestDataFileMeta'])

            # Run list of program commands using subprocess
            for program in program_list:
                logger.info('Run '+" ".join(program))
                output = subprocess.run(program, shell=False, check=True)
                logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

    else:
        # get source meta
        df = getSourceMeta()
        # Need to either remove ADCIRC forecast source from drf_source_meta, or add a column to indicate it's an ADCIRC forecast source.

        # Create list of program commands
        program_list = []
        for index, row in df.iterrows():
            program_list.append(['python','createHarvestFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource', row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],'--inputFilenamePrefix',row['filename_prefix']])

        program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestHarvestDataFileMeta'])

        # Run list of program commands using subprocess
        for program in program_list:
            logger.info('Run '+" ".join(program))
            output = subprocess.run(program, shell=False, check=True)
            logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

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
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

# This function runs ingestTasks.py with --inputTask ingestData, ingest gauge data into the drf_gauge_data table, in the database.
def runDataIngest(ingestDir):
    # get source meta
    df = getSourceMeta()

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestData','--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'], '--inputSourceVariable',row['source_variable']])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

# This functions creates and ingest the harvest files, and data in sequence
def runSequenceIngest(harvestDir, ingestDir, modelRunID):
    runHarvestFile(harvestDir, ingestDir, modelRunID)
    runDataCreate(ingestDir)
    runDataIngest(ingestDir)

# Main program function takes args as input, which contains the harvestDir, ingestDir, and inputTask variables
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'runIngest.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Check if modelRunID argument exist. This argument is used in runHarvestFile.
    if hasattr(args, 'modelRunID') == True:
        modelRunID = args.modelRunID
    elif hasattr(args, 'modelRunID') == False:
        modelRunID = None
    else:
        logger('Something went wrong with the model run ID')
        sys.exit(1) 

    # Get input task
    inputTask = args.inputTask

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'ingestharvestdatafilemeta':
        harvestDir = os.path.join(args.harvestDir, '')
        ingestDir = os.path.join(args.ingestDir, '') 
        logger.info('Run input file information.')
        runHarvestFile(harvestDir, ingestDir, modelRunID)
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
    elif inputTask.lower() == 'sequenceingest':
        harvestDir = os.path.join(args.harvestDir, '')
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Run sequence ingest.')
        runSequenceIngest(harvestDir, ingestDir, modelRunID)
        logger.info('Ran sequence ingest.')


# Run main function 
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Non optional argument
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['ingestHarvestDataFileMeta','DataCreate','DataIngest','SequenceIngest'], required=True)

    # get runScript argument to use in if statement
    args = parser.parse_known_args()[0]

    # Optional argument
    if args.inputTask.lower() == 'ingestharvestdatafilemeta':
        parser.add_argument("--modelRunID", "--modelRunId", help="Model run ID for ADCIRC forecast data", action="store", dest="modelRunID", required=False)
        parser.add_argument("--harvestDIR", "--harvestDir", help="Harvest directory path", action="store", dest="harvestDir", required=True)
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'datacreate':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'dataingest':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'sequenceingest':
        parser.add_argument("--modelRunID", "--modelRunId", help="Model run ID for ADCIRC forecast data", action="store", dest="modelRunID", required=False)
        parser.add_argument("--harvestDIR", "--harvestDir", help="Harvest directory path", action="store", dest="harvestDir", required=True)
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    else:
        logger.info(args.inputTask+' not correct')

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

