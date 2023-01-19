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

# This function queries the drf_harvest_data_file_meta table using a file_name, an pulls out the 
# file_name, and if the file_name exists in the table.
def getFileDateTime(inputFile):
    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT dir_path, file_name, ingested, overlap_past_file_date_time
                       FROM drf_harvest_data_file_meta
                       WHERE file_name = %(input_file)s
                       ORDER BY file_name""",
                    {'input_file': inputFile})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['dir_path', 'file_name', 'ingested', 'overlap_past_file_date_time'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes source as input, and returns a DataFrame containing a list of files, from table drf_harvest_data_file_meta, 
# that have been ingested.
def getOldHarvestFiles(inputDataSource, inputSourceName, inputSourceArchive):
    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['SQL_DATABASE'], user=os.environ['SQL_USER'], host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], password=os.environ['SQL_PASSWORD'])
        cur = conn.cursor()
       
        # Set enviromnent 
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT * FROM drf_harvest_data_file_meta
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND
                       source_archive = %(sourcearchive)s AND ingested = True""", 
                    {'datasource': inputDataSource, 'sourcename': inputSourceName, 'sourcearchive': inputSourceArchive})
       
        # convert query output to Pandas dataframe 
        df = pd.DataFrame(cur.fetchall(), columns=['file_id', 'dir_paht', 'file_name', 'data_date_time', 
                                                   'data_begin_time', 'data_end_time', 'data_source', 'source_name', 
                                                   'source_archive', 'ingested', 'overlap_past_file_date_time'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return DataFrame
        return(df)

    # If exception log error    
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

# This function takes an input directory path and input dataset, and uses them to create a file list
# that is ingested into the drf_harvest_data_file_meta table, and used to ingest the data files.
def createFileList(harvestDir, inputDataSource, inputSourceName, inputSourceArchive):
    # Define inputDataset
    if inputSourceName == 'adcirc':
        inputDataset = inputSourceName+'_stationdata'
    else:
        inputDataset = inputSourceArchive+'_stationdata'

    # Search for files in the harvestDir that have inputDataset name in them, and generate a list of files found
    if inputDataSource == 'tidal_gauge':
        dirInputFiles = glob.glob(harvestDir+inputDataset+"_water_level_*.csv")
        if len(dirInputFiles) == 0:
            logger.info('Search for old NOAA file name')
            dirInputFiles = glob.glob(harvestDir+inputDataset+"_[!meta]*.csv")
        else:
            logger.info('Search for new NOAA file name which includes water_level in the name')
    elif inputDataSource == 'tidal_predictions':
        dirInputFiles = glob.glob(harvestDir+inputDataset+"_predictions_*.csv")
    elif inputDataSource == 'air_barometer':
        dirInputFiles = glob.glob(harvestDir+inputDataset+"_air_pressure_*.csv")
    elif inputDataSource == 'ocean_buoy':
        dirInputFiles = glob.glob(harvestDir+inputDataset+"_wave_height_*.csv")
    elif inputDataSource == 'wind_anemometer':
        dirInputFiles = glob.glob(harvestDir+inputDataset+"_wind_speed_*.csv")
    elif inputDataSource == 'coastal_gauge':
        dirInputFiles = glob.glob(harvestDir+inputDataset+"_coastal_*.csv")
        if len(dirInputFiles) == 0:
            logger.info('Search for old Contrails file name which uses COASTAL in the name')
            dirInputFiles = glob.glob(harvestDir+inputDataset+"_COASTAL_*.csv")
        else:
            logger.info('Search for new contails file format which uses coastal the name')
    elif inputDataSource == 'river_gauge':
        dirInputFiles = glob.glob(harvestDir+inputDataset+"_river_*.csv")
        if len(dirInputFiles) == 0:
            logger.info('Search for old contrails file name which uses RIVERS in the name')
            dirInputFiles = glob.glob(harvestDir+inputDataset+"_RIVERS_*.csv")
        else:
            logger.info('Search for new contrails file format which uses river in the name')
    else:
        logger.info('Get inputDataSource name '+inputDataSource)
        dirInputFiles = glob.glob(harvestDir+inputDataset+"_"+inputSourceArchive.upper()+"_"+inputDataSource.upper()+"_*.csv")
 
    # Define outputList variable
    outputList = []

    # Loop through dirOutputFiles, generate new variables and add them to outputList
    for dirInputFile in dirInputFiles:
        dir_path = dirInputFile.split(inputDataset)[0]
        file_name = Path(dirInputFile).parts[-1] 

        datetimes = re.findall(r'(\d+-\d+-\d+T\d+:\d+:\d+)',file_name) 
        data_date_time = datetimes[0]

        df = pd.read_csv(dirInputFile)
        data_begin_time = df['TIME'].min()
        data_end_time = df['TIME'].max()

        if pd.isnull(data_begin_time) and pd.isnull(data_end_time):
            ingested = 'True'
        else:
            ingested = 'False'

        overlap_past_file_date_time = 'False'

        outputList.append([dir_path,file_name,data_date_time,data_begin_time,data_end_time,inputDataSource,inputSourceName,inputSourceArchive,ingested,overlap_past_file_date_time]) 

    # Convert outputList to a DataFrame
    dfnew = pd.DataFrame(outputList, columns=['dir_path', 'file_name', 'data_date_time', 'data_begin_time', 'data_end_time', 'data_source', 'source_name', 'source_archve', 'ingested', 'overlap_past_file_date_time'])

    # Get DataFrame of existing list of files, in the database, that have been ingested.
    dfold = getOldHarvestFiles(inputDataSource, inputSourceName, inputSourceArchive)

    # Create DataFrame of list of current files that are not already ingested in table drf_harvest_data_file_meta.
    df = dfnew.loc[~dfnew['file_name'].isin(dfold['file_name'])]

    if len(df.values) == 0:
        logger.info('No new files for data source '+inputDataSource+', with source name '+inputSourceName+', from the '+inputSourceArchive+' archive')
        first_time = np.nan
        last_time = np.nan
    else:
        logger.info('There are '+str(len(df.values))+' new files for data source '+inputDataSource+', with source name '+inputSourceName+', from the '+inputSourceArchive+' archive')
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

    logger.info('Start processing source data for data source '+inputDataSource+', source name '+inputSourceName+', and source archive '+inputSourceArchive+'.')

    # Get DataFrame file list, and time variables by running the createFileList function
    df, first_time, last_time = createFileList(harvestDir, inputDataSource, inputSourceName, inputSourceArchive)

    if pd.isnull(first_time) and pd.isnull(last_time):
        logger.info('No new files for data source '+inputDataSource+', source name '+inputSourceName+', and source archive '+inputSourceArchive+'.')
    else:
        # Get current date    
        current_date = datetime.date.today()

        # Create output file name
        if inputSourceName == 'adcirc':
            outputFile = 'harvest_files_'+inputSourceName+'_stationdata_'+inputSourceArchive+'_'+inputDataSource+'_'+first_time.strip()+'_'+last_time.strip()+'_'+current_date.strftime("%b-%d-%Y")+'.csv'
        else:
            outputFile = 'harvest_files_'+inputSourceArchive+'_stationdata_'+inputDataSource+'_'+first_time.strip()+'_'+last_time.strip()+'_'+current_date.strftime("%b-%d-%Y")+'.csv'

        # Write DataFrame containing list of files to a csv file
        df.to_csv(ingestDir+outputFile, index=False, header=False)
        logger.info('Finished processing source data for data source '+inputDataSource+', source name '+inputSourceName+', and source archive '+inputSourceArchive+'.')

# Run main function takes ingestDir, and outputFile as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--harvestDIR", "--harvestDir", help="Input directory path", action="store", dest="harvestDir", required=True)    
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--inputDataSource", help="Input data source name", action="store", dest="inputDataSource", choices=['namforecast_hsofs','nowcast_hsofs','namforecast_ec95d','nowcast_ec95d','tidal_gauge','tidal_predictions','ocean_buoy','coastal_gauge','river_gauge','wind_anemometer','air_barometer'], required=True)
    parser.add_argument("--inputSourceName", help="Input source name", action="store", dest="inputSourceName", choices=['adcirc','noaa','ndbc','ncem'], required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", choices=['noaa','ndbc','contrails','renci'], required=True)

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

