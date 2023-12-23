#!/usr/bin/env python
# coding: utf-8

# import modules
import os
import sys
import glob
import re
import argparse
import shutil
import psycopg
import subprocess
import pandas as pd
import getDashboardMeta as gdm
from datetime import datetime
from loguru import logger

def getSourceMeta(dataSource, sourceName, sourceArchive, sourceInstance, forcingMetaclass):
    ''' Returns DataFrame containing source meta-data queried from the drf_source_model_meta table. 
        Parameters
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). 
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...). 
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...). 
            inputSourceVariable: string
                Source variable, such as water_level. Used by ingestSourceMeta, and ingestData.
            inputSourceInstance: string
                Source instance, such as ncsc123_gfs_sb55.01. Used by ingestSourceMeta, and ingestData.
            inputForcingMetaclass: string
                ADCIRC model forcing class, such as synoptic or tropical. Used by ingestSourceMeta, and ingestData.
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
        cur.execute("""SELECT data_source, source_name, source_archive, source_variable, source_instance, filename_prefix, location_type, units 
                       FROM drf_source_model_meta 
                       WHERE data_source = %(datasource)s AND source_name = %(sourcename)s AND source_archive = %(sourcearchive)s AND 
                                           source_instance = %(sourceinstance)s AND forcing_metaclass = %(forcingmetaclass)s
                       ORDER BY filename_prefix""", 
                       {'datasource': dataSource, 'sourcename': sourceName, 'sourcearchive': sourceArchive, 'sourceinstance': sourceInstance, 
                        'forcingmetaclass': forcingMetaclass,})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['data_source', 'source_name', 'source_archive', 'source_variable', 'source_instance', 
                                                   'filename_prefix', 'location_type', 'units'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def getApsVizStationInfo(modelRunID):
    ''' Returns DataFrame containing variables from the drf_apsviz_station_file_meta table. It takes a model run ID as input.
        Parameters
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db.
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
        cur.execute("""SELECT dir_path, file_name, data_date_time, data_source, source_name, source_archive, source_instance, forcing_metaclass,
                              grid_name, model_run_id, timemark, location_type, csvurl, ingested
                       FROM drf_apsviz_station_file_meta
                       WHERE model_run_id = %(modelrunid)s""", {'modelrunid': modelRunID})
    
        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['dir_path','file_name','data_date_time','data_source','source_name','source_archive','source_instance',
                                                   'forcing_metaclass','grid_name','model_run_id','timemark','location_type','csvurl','ingested']) 
        
        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def runHarvestFile(harvestPath, ingestPath, modelRunID):
    ''' This function runs createHarvestModelFileMeta.py, which creates harvest meta data files, that are ingested into the 
        drf_harvest_model_file_meta table, in the database, by running ingestModelTasks.py using --inputTask ingestHarvestDataFileMeta.
        Parameters
            harvestPath: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this path.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db.
        Returns
            None, but it create harvest meta data files, that are then ingested into the drf_harvest_model_file_meta table.
    '''

    # Sources from drf_source_model_meta will not be used in this run
    logger.info('Process data for ADCIRC model run: '+modelRunID)

    # Get input file name, grid_name, timemark, forcing_metaclass, storm, and workflow_type
    df = gdm.getADCIRCRunPropertyVariables(modelRunID)

    source_name = df['suite.model'].values[0]
    grid_name = df['ADCIRCgrid'].values[0].upper()
    advisory = df['advisory'].values[0] 
    forcingEnsemblename = df['forcing.ensemblename'].values[0]
    forcingMetaclass = df['forcing.metclass'].values[0]
    source_instance = df['instancename'].values[0]
    storm = df['storm'].values[0]
    stormname = df['stormname'].values[0]
    stormnumber = df['stormnumber'].values[0]
    source_archive = df['physical_location'].values[0]
    time_currentdate = df['time.currentdate'].values[0]
    time_currentcycle = df['time.currentcycle'].values[0]
    workflowType = df['workflow_type'].values[0]

    timemark = ":".join(datetime(int('20'+time_currentdate[0:2]),int(time_currentdate[2:4]),
                                 int(time_currentdate[4:6]),int(time_currentcycle)).isoformat().split(':')[:-1])
    data_date_time = timemark

    # Get ADCIRC forecast filenames
    filelist = glob.glob(harvestPath+'/'+source_name+'_'+storm+'_'+source_archive.upper()+'_'+forcingEnsemblename.upper()+'_*.csv')

    for filenamepath in filelist:
        # Define inputFile 
        inputFile = filenamepath.split('/')[-1]

        # Define variables that differ between synoptic and tropical runs
        if forcingMetaclass == 'synoptic':
            logger.info('Input file '+inputFile+' data is not from a hurricane, so data source only consists of the forcingEnsemblename and grid name')
            forecast_data_source = forcingEnsemblename.upper()+'_'+grid_name
            forecast_obs_station_type = inputFile.split('_')[-4]
            forecast_prefix = source_name+'_'+storm+'_'+source_archive.upper()+'_'+forcingEnsemblename.upper()+'_'+grid_name+'_FORECAST_'+forecast_obs_station_type
            #forecast_prefix_ta = forecast_prefix+'_'+timemark
            nowcast_data_source = 'NOWCAST_'+grid_name
            nowcast_prefix = source_name+'_'+storm+'_'+source_archive.upper()+'_NOWCAST_'+grid_name+'_NOWCAST_'+forecast_obs_station_type
            #nowcast_prefix_ta = nowcast_prefix+'_'+timemark 
        else:
            logger.info('Input file '+inputFile+' data is from a hurricane, so data source consists of the storm, forcingEnsemblename and grid name')
            forecast_data_source = storm+'_'+forcingEnsemblename+'_'+grid_name
            forecast_obs_station_type = inputFile.split('_')[-4]
            forecast_prefix = source_name+'_'+storm+'_'+source_archive.upper()+'_'+forcingEnsemblename+'_'+grid_name+'_FORECAST_'+forecast_obs_station_type
            #forecast_prefix_ta = forecast_prefix+'_'+advisory
            nowcast_data_source = storm+'_NOWCAST_'+grid_name.upper()
            nowcast_prefix = source_name+'_'+storm+'_'+source_archive.upper()+'_NOWCAST_'+grid_name+'_NOWCAST_'+forecast_obs_station_type
            #nowcast_prefix_ta = nowcast_prefix+'_'+advisory

        # Define other source meta variables
        if forecast_obs_station_type == 'NOAASTATIONS':
            logger.info('Forecast obs station type: '+forecast_obs_station_type+' so defining additional parameters.')
            source_variable = 'water_level'
            location_type = 'tidal'
            units = 'm'
            csv_url = os.environ['UI_DATA_URL']
        elif forecast_obs_station_type == 'CONTRAILSCOASTAL':
            logger.info('Forecast obs station type: '+forecast_obs_station_type+' so defining additional parameters.')
            source_variable = 'water_level'
            location_type = 'coastal'
            units = 'm'
            csv_url = os.environ['UI_DATA_URL']
        elif forecast_obs_station_type == 'CONTRAILSRIVERS':
            logger.info('Forecast obs station type: '+forecast_obs_station_type+' so defining additional parameters.')
            source_variable = 'water_level'
            location_type = 'river'
            units = 'm'
            csv_url = os.environ['UI_DATA_URL']
        elif forecast_obs_station_type == 'NDBCBUOYS':
            logger.info('Forecast obs station type: '+forecast_obs_station_type+' so defining additional parameters.')
            source_variable = 'wave_height'
            location_type = 'ocean'
            units = 'm'
            csv_url = os.environ['UI_DATA_URL']
        else:
            logger.info('Forecast obs station type: '+forecast_obs_station_type+' is incorrect.')
            sys.exit(1)

        # Check to see if forecast source exists
        dfcheck = gdm.checkModelSourceMeta(forecast_prefix)
   
        # IN THE FUTURE LOOK INTO USING FILENANAME_PREFIX AS INPUT VARIABLE INSTEAD OF USING ALL THE OTHER VARIABLE. WOULD NEED TO ADD INTANCE NAME TO IT.
        # Check if dfcheck is empty, for forecast. If it is a new source is added to the drf_source_model_meta table ,and then drf_model_source table. 
        if dfcheck.empty:
            # Log results
            logger.info('The following source does not exist in the drf_source_model_meta table:\n '+
                        forecast_data_source+','+source_name+','+source_archive+','+source_variable+','+
                        forecast_prefix+','+location_type+','+units)
            program_list = []

            # Forecast files
            program_list.append(['python','ingestModelTasks.py','--inputDataSource',forecast_data_source,'--inputSourceName',source_name,'--inputSourceArchive',
                                 source_archive,'--inputSourceInstance',source_instance,'--inputForcingMetaclass',forcingMetaclass,'--inputSourceVariable',
                                 source_variable,'--inputFilenamePrefix',forecast_prefix,'--inputLocationType',location_type, '--inputUnits',units,'--inputTask','ingestSourceMeta'])
            program_list.append(['python','createIngestModelSourceMeta.py','--ingestPath',ingestPath,'--inputDataSource',forecast_data_source,'--inputSourceName',
                                 source_name,'--inputSourceArchive',source_archive,'--inputSourceInstance',source_instance,'--inputForcingMetaclass',forcingMetaclass,
                                 '--inputUnits',units,'--inputLocationType',location_type])
            program_list.append(['python','ingestModelTasks.py','--ingestPath',ingestPath,'--inputTask','ingestSourceData'])
            program_list.append(['python','createHarvestModelFileMeta.py','--harvestPath',harvestPath,'--ingestPath',ingestPath,'--inputDataSource',forecast_data_source,
                                 '--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputSourceInstance',source_instance,
                                 '--inputForcingMetaclass',forcingMetaclass,'--inputFilenamePrefix',forecast_prefix,'--inputTimemark',timemark])
            #program_list.append(['python','ingestModelTasks.py','--ingestPath',ingestPath,'--inputTask','ingestHarvestDataFileMeta'])

            # Nowcast files
            program_list.append(['python','createHarvestModelFileMeta.py','--harvestPath',harvestPath,'--ingestPath',ingestPath,'--inputDataSource',nowcast_data_source,
                                 '--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputSourceInstance',source_instance,
                                 '--inputForcingMetaclass',forcingMetaclass,'--inputFilenamePrefix',nowcast_prefix,'--inputTimemark',timemark])
            #program_list.append(['python','ingestModelTasks.py','--ingestPath',ingestPath,'--inputTask','ingestHarvestDataFileMeta'])

            # Meta files
            logger.info('Model type is FORECAST, so meta file, containing station location harvest file meta has to be ingested')
            apsviz_station_meta_filename = 'adcirc_meta_'+"_".join(inputFile.split('_')[1:])
            program_list.append(['python','createApsVizStationFileMeta.py','--harvestPath',harvestPath,'--ingestPath',ingestPath,'--inputDataSource',
                                 forecast_data_source,'--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputSourceInstance',source_instance,
                                 '--inputForcingMetaclass',forcingMetaclass,'--inputFilename',apsviz_station_meta_filename,'--gridName',grid_name,'--modelRunID',modelRunID,
                                 '--timeMark',timemark,'--inputLocationType',location_type,'--csvURL',csv_url,'--dataDateTime',data_date_time])
            program_list.append(['python','ingestModelTasks.py','--ingestPath',ingestPath,'--inputFilename', 'harvest_meta_files_'+apsviz_station_meta_filename,
                                 '--inputTask','ingestApsVizStationFileMeta'])

            # Run list of program commands using subprocess
            for program in program_list:
                logger.info('Run '+" ".join(program))
                output = subprocess.run(program, shell=False, check=True)
                logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

        else:
            # log results
            logger.info(forecast_data_source+','+source_name+','+source_archive+','+source_variable+','+
                        forecast_prefix+','+location_type+','+units)
            program_list = []

            # Forecast files
            program_list.append(['python','createHarvestModelFileMeta.py','--harvestPath',harvestPath,'--ingestPath',ingestPath,'--inputDataSource',forecast_data_source,
                                 '--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputSourceInstance',source_instance,
                                 '--inputForcingMetaclass',forcingMetaclass,'--inputFilenamePrefix',forecast_prefix,'--inputTimemark',timemark])
            #program_list.append(['python','ingestModelTasks.py','--ingestPath',ingestPath,'--inputTask','ingestHarvestDataFileMeta'])

            # Nowcast files
            program_list.append(['python','createHarvestModelFileMeta.py','--harvestPath',harvestPath,'--ingestPath',ingestPath,'--inputDataSource',forecast_data_source,
                                 '--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputSourceInstance',source_instance,
                                 '--inputForcingMetaclass',forcingMetaclass,'--inputFilenamePrefix',nowcast_prefix,'--inputTimemark',timemark])
            #program_list.append(['python','ingestModelTasks.py','--ingestPath',ingestPath,'--inputTask','ingestHarvestDataFileMeta'])

            # Meta files
            apsviz_station_meta_filename = 'adcirc_meta_'+"_".join(inputFile.split('_')[1:])
            program_list.append(['python','createApsVizStationFileMeta.py','--harvestPath',harvestPath,'--ingestPath',ingestPath,'--inputDataSource',
                                 forecast_data_source,'--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputSourceInstance',source_instance,
                                 '--inputForcingMetaclass',forcingMetaclass,'--inputFilename',apsviz_station_meta_filename,'--gridName',grid_name,'--modelRunID',modelRunID,
                                 '--timeMark',timemark,'--inputLocationType',location_type,'--csvURL',csv_url,'--dataDateTime',data_date_time])
            program_list.append(['python','ingestModelTasks.py','--ingestPath',ingestPath,'--inputFilename', 'harvest_meta_files_'+apsviz_station_meta_filename,
                                 '--inputTask','ingestApsVizStationFileMeta'])

            # Run list of program commands using subprocess
            for program in program_list:
                logger.info('Run '+" ".join(program))
                output = subprocess.run(program, shell=False, check=True)
                logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))
    
    program_list.append(['python','ingestModelTasks.py','--ingestPath',ingestPath,'--inputTask','ingestHarvestDataFileMeta'])
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runDataCreate(ingestPath, modelRunID):
    ''' This function runs createIngestModelData.py, which ureates gauge data, from the original harvest data files, that will be 
        ingested into the database using the runDataIngest function.
        Parameters
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db.
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this path.
        Returns
            None, but it runs createIngestModelData.py, which returns a CSV file
    '''

    # NEED TO CHANGE THIS SO IT QUERIES THE drf_harvest_model_file_meta TABLE. THE modelRunID WILL RETURN MULTIPLE ROWS BECAUSE OF MULTIPLE 
    # FILES FOR ONE modelRunID, BUT SHOULD ONLY NEED SINGLE VALUES FOR dataSource, sourceName, sourceArchive, sourceInstance, AND forcingMetaclass.
    # QUESTIONS FOR THE FUTURE IS HOW WE WILL DEAL WITH MULTIPLE VARIABLE COMMING FROM SINGLE STATION. CURRENTLY, FOR ADCIRC DATA ONLY ONE VARIABLE
    # EXIST FOR A STATION, EITHER water_level OR wave_height.
    dfh = gdm.getADCIRCRunPropertyVariables(modelRunID)

    # Define input variable for ingestModelTasks.py
    dataSource = dfh['data_source']
    sourceName = dfh['source_name']
    sourceArchive = dfh['source_archive']
    sourceInstance = dfh['source_instance']
    forcingMetaclass = dfh['forcing_metaclass']

    # Get source_variable from 
    dfv = getSourceMeta(dataSource, sourceName, sourceArchive, sourceInstance, forcingMetaclass)
    sourceVariable = dfv['source_variable']

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createIngestModelData.py','--ingestPath',ingestPath,'--inputDataSource','--inputDataSource', dataSource,
                             '--inputSourceName', sourceName,'--inputSourceArchive', sourceArchive, '--inputSourceVariable', sourceVariable,
                             '--inputSourceInstance' ,sourceInstance, 'inputSourceInstance', forcingMetaclass])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runDataIngest(ingestPath, modelRunID):
    ''' This function runs ingestModelTasks.py with --inputTask ingestData, ingest gauge data into the drf_model_data table, in the database. 
        Parameters
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db.
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this path.   
        Returns
            None, but it runs ingestTask.py, which ingest data into the drf_gauge_station table.
    '''

    # NEED TO CHANGE THIS SO IT QUERIES THE drf_harvest_model_file_meta TABLE. THE modelRunID WILL RETURN MULTIPLE ROWS BECAUSE OF MULTIPLE 
    # FILES FOR ONE modelRunID, BUT SHOULD ONLY NEED SINGLE VALUES FOR dataSource, sourceName, sourceArchive, sourceInstance, AND forcingMetaclass.
    # QUESTIONS FOR THE FUTURE IS HOW WE WILL DEAL WITH MULTIPLE VARIABLE COMMING FROM SINGLE STATION. CURRENTLY, FOR ADCIRC DATA ONLY ONE VARIABLE
    # EXIST FOR A STATION, EITHER water_level OR wave_height.
    df = gdm.getADCIRCRunPropertyVariables(modelRunID)

    # Define input variable for ingestModelTasks.py
    dataSource = df['data_source']
    sourceName = df['source_name']
    sourceArchive = df['source_archive']
    sourceInstance = df['source_instance']
    forcingMetaclass = df['forcing_metaclass']

    # Get source_variable from 
    dfv = getSourceMeta(dataSource, sourceName, sourceArchive, sourceInstance, forcingMetaclass)
    sourceVariable = dfv['source_variable']

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','ingestModelTasks.py','--ingestPath',ingestPath,'--inputTask','ingestData','--inputDataSource', dataSource,
                             '--inputSourceName', sourceName,'--inputSourceArchive', sourceArchive, '--inputSourceVariable', sourceVariable,
                             'inputSourceInstance' ,sourceInstance,'inputSourceInstance', forcingMetaclass])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runApsVizStationCreateIngest(ingestPath, modelRunID):
    ''' This function creates and ingests the apsViz station data from the adcirc meta files, adding a timemark, model_run_id, 
        variable type, and csv URL. It gets this information from the from the drf_apsviz_station_file_meta table, by running the 
        getApsVizStationInfo() function
        Parameters 
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this path. 
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db. 
        Returns
            None, but it runs createIngestApsVizStationData.py, which outputs a CSV file, and then it runs ingestTask.py
            which ingest the CSV files into the drf_apsviz_station table.
    ''' 

    logger.info('Create apsViz station file data, for model run ID '+modelRunID+', to be ingested into the apsviz_station table ')
    df = getApsVizStationInfo(modelRunID) 

    # Create list of all location types
    all_location_types = ','.join(df['location_type'].unique().tolist())

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        # dir_path, file_name, data_date_time, data_source, source_name, source_archive, model_run_id, csvurl, ingested
        program_list.append(['python','createIngestApsVizStationData.py','--harvestPath',row['dir_path'],'--ingestPath',ingestPath,
                             '--inputFilename',row['file_name'],'--timeMark',str(row['timemark']),'--modelRunID',row['model_run_id'],
                             '--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],
                             '--inputLocationType',row['location_type'],'--allLocationTypes',all_location_types,'--gridName',row['grid_name'],'--csvURL',row['csvurl']])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+' with output returncode '+str(output.returncode))

    logger.info('Ingest apsViz station file data, for model run ID '+modelRunID+', into the apsviz_station table ')

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','ingestModelTasks.py','--ingestPath',ingestPath,'--inputFilename',row['file_name'],'--inputTask','ingestApsVizStationData'])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runSequenceIngest(harvestPath, ingestPath, modelRunID):
    ''' Runs the runHarvestFile(), runDataCreate(), and runDataIngest() functions in sequence. If modelRunID has a value
        it also runs the runApsVizStationCreateIngest() function.
        Parameters
            harvestPath: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestPath: string
                Directory path to ingest data files, created from the harvest files, modelRunID subdirectory is included in this
                path. Used by ingestHarvestDataFileMeta, DataCreate, DataIngest, runApsVizStationCreateIngest, SequenceIngest.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db. Used by ingestHarvestDataFileMeta,
                runApsVizStationCreateIngest, and sequenceIngest.
        Returns
            None, but the functions it calls return values, described above.
    '''

    runHarvestFile(harvestPath, ingestPath, modelRunID)
    runDataCreate(ingestPath, modelRunID)
    runDataIngest(ingestPath, modelRunID)
    runApsVizStationCreateIngest(ingestPath, modelRunID) 

@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, and runs specified task.
        Parameters
            args: dictionary
                contains the parameters listed below.
            inputTask: string
                The type of task (ingestHarvestDataFileMeta, DataCreate, DataIngest, runApsVizStationCreateIngest,
                SequenceIngest) to be perfomed. The type of inputTaks can change what other types of inputs runInget.py
                requires. Below is a list of all inputs, with associated tasks.
            harvestDir: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db. Used by ingestHarvestDataFileMeta,
                runApsVizStationCreateIngest, and sequenceIngest.
        Returns
            None
    '''         

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'runModelIngest.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract modelRunID from args. The modelRunID is an input variable for the functions getApsVizStationInfo() and runHarvestFile(). It is 
    # useed to query the DB. 
    # It is also used, along with ingestDir, to define ingestPath.  The ingestPath defines the location of the files to be ingested, and  
    # is used by runHarvestFile(), runDataCreate(), runApsVizStationCreateIngest(), runDataCreate(), runDataIngest(), and runSequenceIngest()
    modelRunID = args.modelRunID
    ingestPath = os.path.join(args.ingestDir+modelRunID, '')

    # Get input task
    inputTask = args.inputTask

    # Check if inputTask if file, station, source, data or view, and run appropriate function
    if inputTask.lower() == 'ingestharvestdatafilemeta':
        harvestPath = os.path.join(args.harvestDir+modelRunID, '')
        logger.info('Run input file information.')
        runHarvestFile(harvestPath, ingestPath, modelRunID)
        logger.info('Ran input file information.')
    elif inputTask.lower() == 'datacreate':
        logger.info('Run data create.')
        runDataCreate(ingestPath, modelRunID)
        logger.info('Ran data create.')
    elif inputTask.lower() == 'dataingest':
        logger.info('Run data ingest.')
        runDataIngest(ingestPath, modelRunID)
        logger.info('Ran data ingest.')
    elif inputTask.lower() == 'runapsvizstationcreateingest':
        modelRunID = args.modelRunID
        logger.info('Run apsViz station create-ingest for model run ID '+modelRunID+'.')
        runApsVizStationCreateIngest(ingestPath, modelRunID)
        logger.info('Ran apsViz station create-ingest for model run ID '+modelRunID+'.')
    elif inputTask.lower() == 'sequenceingest':
        harvestPath = os.path.join(args.harvestDir+modelRunID, '')
        logger.info('Run sequence ingest.')
        runSequenceIngest(harvestPath, ingestPath, modelRunID)
        logger.info('Ran sequence ingest.')

# Run main function 
if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            inputTask: string
                The type of task (ingestHarvestDataFileMeta(), DataCreate(), DataIngest(), runApsVizStationCreateIngest(),
                SequenceIngest) to be perfomed. The type of inputTask can change what other types of inputs runInget.py
                requires. Below is a list of all inputs, with associated tasks. 
            harvestDir: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate, 
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db. Used by ingestHarvestDataFileMeta(),
                runDataCreate(), runDataIngest(), runApsVizStationCreateIngest(), and sequenceIngest(). 
        Returns
            None
    '''         

    parser = argparse.ArgumentParser()

    # None optional argument
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['ingestHarvestDataFileMeta',
                        'DataCreate','DataIngest','runApsVizStationCreateIngest','SequenceIngest'], required=True)
    parser.add_argument("--modelRunID", "--modelRunId", help="Model run ID for ADCIRC forecast data", action="store", dest="modelRunID", required=False)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)

    # get runScript argument to use in if statement
    args = parser.parse_known_args()[0]

    # Optional argument
    if args.inputTask.lower() == 'ingestharvestdatafilemeta':
        parser.add_argument("--harvestDIR", "--harvestDir", help="Harvest directory path", action="store", dest="harvestDir", required=True)
    elif args.inputTask.lower() == 'sequenceingest':
        parser.add_argument("--harvestDIR", "--harvestDir", help="Harvest directory path", action="store", dest="harvestDir", required=True)
    else:
        logger.info(args.inputTask+' not correct')

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

