#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import sys
import os
import datetime
import pandas as pd
from pathlib import Path
from loguru import logger

# This function takes as input the harvest directory path, data source, source name, source archive, and a file name prefix.
# It uses them to create a file list that is then ingested into the drf_harvest_model_file_meta table, and used to ingest the
# data files. This function also returns first_time, and last_time which are used in cross checking the data.
def createFileList(dirInputFile, modelRunID, inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, inputForcingMetclass, 
                   inputAdvisory, inputTimemark):
    ''' Returns a DataFrame containing a list of files, with meta-data, to be ingested in to table drf_harvest_model_file_meta. It also returns
        first_time, and last_time used for cross checking.
        Parameters
            dirInputFile: string
                Directory path and harvest file to be ingested.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db.
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
            inputAdvisory: string
                The model start time for synoptic runs, and the storm advisory number for tropical runs
            inputTimemark: string
                Model run ID timemark, or the start time of the model. This is only for ADCIRC data.
        Returns
            DataFrame, first_time, last_time
    '''

    # Define db model_run_id from input modelRunID
    model_run_id = modelRunID

    # Defind dir_path and file_namne from dirInputFile
    path_file = Path(dirInputFile)
    dir_path = os.path.join(str(path_file.parent), '')
    file_name = path_file.name
 
    # Define data_date_time and processing_datetime
    data_date_time = inputTimemark
    processing_datetime = datetime.datetime.today().isoformat().split('.')[0]

    # Read data file and extract data_begin_time and data_end_time
    df = pd.read_csv(dirInputFile)
    data_begin_time = df['TIME'].min()
    data_end_time = df['TIME'].max()

    # Define ingested and overlap_past_file_date_time
    ingested = 'False'
    overlap_past_file_date_time = 'False'

    outputList = []
    outputList.append([dir_path,file_name,model_run_id,processing_datetime,data_date_time,data_begin_time,data_end_time,inputDataSource,inputSourceName,
                       inputSourceArchive,inputSourceInstance,inputForcingMetclass,inputAdvisory,inputTimemark,ingested,overlap_past_file_date_time]) 

    # Convert outputList to a DataFrame
    df = pd.DataFrame(outputList, columns=['dir_path', 'file_name', 'model_run_id', 'processing_datetime', 'data_date_time', 'data_begin_time', 'data_end_time', 
                                            'data_source', 'source_name', 'source_archve', 'source_instance' , 'forcing_metclass', 'advisory', 
                                            'timemark', 'ingested', 'overlap_past_file_date_time'])


    # Return DataFrame first time, and last time
    return(df, file_name)

@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, runs createFileList, and writes output to CSV file.
        The CSV file will be ingest into table drf_apsviz_station_file_meta during runHarvestFile() is run in runModelIngest.py
        Parameters
            args: dictionary 
                contains the parameters listed below
            dirInputFile: string
                Directory path and harvest file to be ingested. 
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db. Used by createFileList(),
                getOldHarvestFiles() and getFileDateTime(). 
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
            inputAdvisory: string
                The model start time for synoptic runs, and the storm advisory number for tropical runs
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob. 
        Returns
            CSV file
    '''
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'runModelIngest.log', level='DEBUG', rotation="1 MB")
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    dirInputFile = args.dirInputFile
    ingestPath = os.path.join(args.ingestPath, '')
    modelRunID = args.modelRunID
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputSourceInstance = args.inputSourceInstance
    inputForcingMetclass = args.inputForcingMetclass
    inputAdvisory = args.inputAdvisory
    inputTimemark = args.inputTimemark

    if not os.path.exists(ingestPath):
        os.mkdir(ingestPath)
        logger.info("Directory %s created!" % ingestPath)
    else:
        logger.info("Directory %s already exists" % ingestPath)

    logger.info('Start processing source data for data source '+inputDataSource+', source name '+inputSourceName+', and source archive '+inputSourceArchive+
                ', with modelRunID '+modelRunID+'.')

    # Get DataFrame file list, and time variables by running the createFileList function
    df, file_name = createFileList(dirInputFile, modelRunID, inputDataSource, inputSourceName, inputSourceArchive, inputSourceInstance, 
                                   inputForcingMetclass, inputAdvisory, inputTimemark)

    # Write data file to ingest directory.
    outputFile = 'harvest_data_files_'+file_name

    # Write DataFrame containing list of files to a csv file
    df.to_csv(ingestPath+outputFile, index=False, header=False)
    logger.info('Finished processing source data for model run id '+modelRunID+' data source '+inputDataSource+', source name '+inputSourceName+
                ', source archive '+inputSourceArchive+', with modelRunID '+modelRunID+'.')

if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            dirInputFile: string
                Directory path and harvest file to be ingested. 
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db. Used by createFileList(),
                getOldHarvestFiles() and getFileDateTime(). 
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
            inputAdvisory: string
                The model start time for synoptic runs, and the storm advisory number for tropical runs
            inputTimemark: string
                Model run ID timemark, or the start time of the model. This is only for ADCIRC data.
        Returns
            None
    '''

    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--dirInputFile", "--dirInputFile", help="Directory path and harvest file to be ingested", action="store", dest="dirInputFile", required=True)    
    parser.add_argument("--ingestPath", "--ingestPath", help="Ingest directory path, including the modelRunID", action="store", dest="ingestPath", required=True)
    parser.add_argument("--modelRunID", "--modelRunId", help="Model run ID for ADCIRC forecast data", action="store", dest="modelRunID", required=False)
    parser.add_argument("--inputDataSource", help="Input data source name", action="store", dest="inputDataSource", required=True)
    parser.add_argument("--inputSourceName", help="Input source name", action="store", dest="inputSourceName", choices=['adcirc','noaa','ndbc','ncem'], required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", required=True)
    parser.add_argument("--inputSourceInstance", help="Input source variables", action="store", dest="inputSourceInstance", required=True)
    parser.add_argument("--inputForcingMetclass", help="Input forcing metclass", action="store", dest="inputForcingMetclass", required=True)
    parser.add_argument("--inputAdvisory", help="Input advisoty date or number", action="store", dest="inputAdvisory", required=True)
    parser.add_argument("--inputTimemark", help="Input timemark", action="store", dest="inputTimemark", required=False)

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

