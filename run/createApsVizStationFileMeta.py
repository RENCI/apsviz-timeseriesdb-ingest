#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import glob
import sys
import os
import re
import datetime
import psycopg
import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger

# This function takes source information as input, and returns a DataFrame containing a list of files, from table drf_harvest_data_file_meta, 
# that have been ingested.
def getOldApsVizStationFiles(inputDataSource, inputSourceName, inputSourceArchive, modelRunID):
    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_GAUGE_PASSWORD'])
        cur = conn.cursor()
       
        # Set enviromnent 
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT * FROM drf_apsviz_station_file_meta
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                       source_archive = %(sourcearchive)s AND ingested = True AND model_run_id = %(modelrunid)s""", 
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive, 'modelrunid': modelRunID})
          
        # convert query output to Pandas dataframe 
        df = pd.DataFrame(cur.fetchall(), columns=['file_id', 'dir_path', 'file_name', 'data_date_time', 
                                                   'data_source', 'source_name', 'source_archive', 'model_run_id', 
                                                   'timemark', 'variable_type', 'csvurl', 'ingested'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return DataFrame
        return(df)

    # If exception log error    
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes as input the harvest directory path, data source, source name, source archive, and a file name prefix.
# It uses them to create a file list that is then ingested into the drf_harvest_data_file_meta table, and used to ingest the
# data files.
def createFileList(harvestDir,ingestDir,inputDataSource,inputSourceName,inputSourceArchive,inputFilename,modelRunID,timeMark,variableType,csvURL,dataDateTime):
    # [--harvestDir,--ingestDir,--inputDataSource,--inputSourceName,--inputSourceArchive,--inputFilename,--modelRunID,--timeMark,--variableType,--csvURL,--dataDateTime]

    # Define outputList variable
    outputList = []

    # Define ingested as False
    ingested = False

    # NEED TO START HERE TOMORROW MORNING. THERE IS STILL A LOT OF WORK TODO.
    outputList.append([harvestDir,inputFilename,dataDateTime,inputDataSource,inputSourceName,inputSourceArchive,modelRunID,timeMark,variableType,csvURL,ingested]) 

    # Convert outputList to a DataFrame
    dfnew = pd.DataFrame(outputList, columns=['dir_path','file_name','data_date_time','data_source','source_name','source_archve',
                                              'model_run_id','timemark','source_variable','csv_url','ingested'])

    # Get DataFrame of existing list of files, in the database, that have been ingested.
    dfold = getOldApsVizStationFiles(inputDataSource, inputSourceName, inputSourceArchive, modelRunID)

    # Create DataFrame of list of current files that are not already ingested in table drf_harvest_data_file_meta.
    df = dfnew.loc[~dfnew['file_name'].isin(dfold['file_name'])]

    if len(df.values) == 0:
        logger.info('No new files for data source '+inputDataSource+', with source name '+inputSourceName+', from the '+inputSourceArchive+' archive, with model run ID '+modelRunID)
        first_time = np.nan
        last_time = np.nan
    else:
        logger.info('There are '+str(len(df.values))+' new files for data source '+inputDataSource+', with source name '+inputSourceName+', from the '+inputSourceArchive+' archive, with model run ID '+modelRunID)
        # Get first time, and last time from the list of files. This will be used in the filename, to enable checking for time overlap in files
        first_time = df['data_date_time'].iloc[0]
        last_time = df['data_date_time'].iloc[-1] 

    # Return DataFrame first time, and last time
    return(df, first_time, last_time)

# Main program function takes args as input, which contains the ingestDir, and outputFile values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createHarvestFileMeta.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    harvestDir = os.path.join(args.harvestDir, '')
    ingestDir = os.path.join(args.ingestDir, '')
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputFilename = args.inputFilename
    modelRunID = args.modelRunID
    timeMark = args.timeMark
    variableType = args.variableType
    csvURL = args.csvURL
    dataDateTime = args.dataDateTime

    logger.info('Start processing source station data for source '+inputDataSource+', source name '+inputSourceName+', and source archive '+inputSourceArchive+', with filename prefix '+inputFilename+'.')

    # Get DataFrame file list, and time variables by running the createFileList function
    # [--harvestDir,--ingestDir,--inputDataSource,--inputSourceName,--inputSourceArchive,--inputFilename,--modelRunID,--timeMark,--variableType,--csvURL,--dataDateTime]
    df, first_time, last_time = createFileList(harvestDir,ingestDir,inputDataSource,inputSourceName,inputSourceArchive,inputFilename,
                                               modelRunID,timeMark,variableType,csvURL,dataDateTime)

    if pd.isnull(first_time) and pd.isnull(last_time):
        logger.info('No new files for station meta data source '+inputDataSource+', source name '+inputSourceName+', and source archive '+inputSourceArchive+'.')
    else:
        # Get current date    
        current_date = datetime.date.today()

        # Create output file name
        outputFile = 'harvest_meta_files_'+inputFilename

        # Write DataFrame containing list of files to a csv file
        df.to_csv(ingestDir+outputFile, index=False, header=False)
        logger.info('Finished processing source station meta data for file '+inputFilename+' with model run ID '+modelRunID+'.')

# Run main function takes ingestDir, and outputFile as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--harvestDIR", "--harvestDir", help="Input directory path", action="store", dest="harvestDir", required=True)    
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--inputDataSource", help="Input data source name", action="store", dest="inputDataSource", required=True)
    parser.add_argument("--inputSourceName", help="Input source name", action="store", dest="inputSourceName", choices=['adcirc','noaa','ndbc','ncem'], required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", choices=['noaa','ndbc','contrails','renci'], required=True)
    parser.add_argument("--inputFilename", help="Input file name", action="store", dest="inputFilename", required=True)
    parser.add_argument("--modelRunID", help="Input model run ID", action="store", dest="modelRunID", required=True)
    parser.add_argument("--timeMark", help="Timemark value for model run ID", action="store", dest="timeMark", required=True)
    parser.add_argument("--variableType", help="Input source variable name", action="store", dest="variableType", required=True)
    parser.add_argument("--csvURL", help="Input URL to get CSV output file", action="store", dest="csvURL", required=True)
    parser.add_argument("--dataDateTime", help="Data date time from input harvest file", action="store", dest="dataDateTime", required=True)

    # [--harvestDir,--ingestDir,--inputDataSource,--inputSourceName,--inputSourceArchive,--inputFilename,--modelRunID,--timeMark,--variableType,--csvURL,--dataDateTime]

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

