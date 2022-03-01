#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse, glob, sys, os, datetime, psycopg2
import pandas as pd
from pathlib import Path
from loguru import logger

# This function queries the drf_harvest_data_file_meta table using a file_name, an pulls out the 
# file_name, and if the file_name exists in the table.
def getFileDateTime(inputFile):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
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

    # If exception print error
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# This function takes source as input, and returns a DataFrame containing a list of files, from table drf_harvest_data_file_meta, 
# that have been ingested.
def getOldHarvestFiles(source):
    try:
        # Create connection to database and get cursor
        conn = psycopg2.connect("dbname='apsviz_gauges' user='apsviz_gauges' host='localhost' port='5432' password='apsviz_gauges'")
        cur = conn.cursor()
       
        # Set enviromnent 
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

        # Run query
        cur.execute("""SELECT * FROM drf_harvest_data_file_meta
                       WHERE source = %(source)s AND ingested = True""", 
                    {'source': source})
       
        # convert query output to Pandas dataframe 
        df = pd.DataFrame(cur.fetchall(), columns=['file_id', 'dir_paht', 'file_name', 'data_date_time', 
                                                   'data_begin_time', 'data_end_time', 'source', 'content_info', 
                                                   'ingested', 'overlap_past_file_date_time'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return DataFrame
        return(df)

    # If exception print error    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

# This function takes an input directory path and input dataset, and uses them to create a file list
# that is ingested into the drf_harvest_data_file_meta table, and used to ingest the data files.
def createFileList(inputDir, inputDataset):
    # Search for files in the inputDir that have inputDataset name in them, and generate a list of files found
    dirInputFiles = glob.glob(inputDir+inputDataset+"*.csv")
  
    # Define dirOutputFiles variable
    dirOutputFiles = []

    # Loop through dirInputFiles list, fine all files that do not have meta in their name, and add them to dirOutputFiles  
    for dirInputFile in dirInputFiles:
        if dirInputFile.find('meta') == -1:
            #print(dirInputFile)
            dirOutputFiles.append(dirInputFile)
        else:
            continue

    # Define outputList variable
    outputList = []

    # Loop through dirOutputFiles, generate new variables and add them to outputList
    for dirOutputFile in dirOutputFiles:
        dir_path = dirOutputFile.split(inputDataset)[0]
        file_name = Path(dirOutputFile).parts[-1] 
        data_date_time = file_name.split('_')[-1].split('.')[0]

        df = pd.read_csv(dirOutputFile)
        data_begin_time = df['TIME'].min()
        data_end_time = df['TIME'].max()

        source = inputDataset.split('_')[0]

        if source == 'adcirc': 
            content_info = file_name.split('_')[2]+'_'+file_name.split('_')[3]
        elif source == 'contrails':
            content_info = file_name.split('_')[2]
        elif source == 'noaa':
            content_info = 'None'

        ingested = 'False'
        overlap_past_file_date_time = 'False'

        outputList.append([dir_path,file_name,data_date_time,data_begin_time,data_end_time,source,content_info,ingested,overlap_past_file_date_time]) 

    # Convert outputList to a DataFrame
    dfnew = pd.DataFrame(outputList, columns=['dir_path', 'file_name', 'data_date_time', 'data_begin_time', 'data_end_time', 'source', 'content_info', 'ingested', 'overlap_past_file_date_time'])

    # Get DataFrame of existing list of files, in the database, that have been ingested.
    dfold = getOldHarvestFiles(source)

    # Create DataFrame of list of current files that are not already ingested in table drf_harvest_data_file_meta.
    df = dfnew.loc[~dfnew['file_name'].isin(dfold['file_name'])]

    # Get first time, and last time from the list of files. This will be used in the filename, to enable checking for time overlap in files
    first_time = df['data_date_time'].iloc[0]
    last_time = df['data_date_time'].iloc[-1] 

    # Return DataFrame first time, and last time
    return(df, first_time, last_time)

# Main program function takes args as input, which contains the outputDir, and outputFile values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createHarvestFileMeta.log', level='DEBUG')

    # Extract args variables
    inputDir = os.path.join(args.inputDir, '')
    outputDir = os.path.join(args.outputDir, '')
    inputDataset = args.inputDataset

    logger.info('Start processing source data for dataset '+inputDataset+'.')

    # Get DataFrame file list, and time variables by running the createFileList function
    df, first_time, last_time = createFileList(inputDir, inputDataset)

    # Get current date    
    current_date = datetime.date.today()

    # Create output file name
    outputFile = 'harvest_files_'+inputDataset+'_'+first_time.strip()+'_'+last_time.strip()+'_'+current_date.strftime("%b-%d-%Y")+'.csv'

    # Write DataFrame containing list of files to a csv file
    df.to_csv(outputDir+outputFile, index=False)
    logger.info('Finished processing source data for dataset '+inputDataset+'.')

# Run main function takes outputDir, and outputFile as input.
if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--inputDIR", "--inputDir", help="Input directory path", action="store", dest="inputDir", required=True)    
    parser.add_argument("--outputDIR", "--outputDir", help="Output directory path", action="store", dest="outputDir", required=True)
    parser.add_argument("--inputDataset", help="Input dataset name", action="store", dest="inputDataset", choices=['adcirc_stationdata','contrails_stationdata','noaa_stationdata'], required=True)

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

