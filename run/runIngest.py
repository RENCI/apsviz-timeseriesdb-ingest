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
import getDashboardMeta as gdm
from loguru import logger

def getSourceMeta(dataType):
    ''' Returns DataFrame containing source meta-data queried from the drf_source_meta table. 
        Parameters
            dataType: string
                Type of data, obs for observation data, such as noaa gauge data, and model for model such as ADCIRC.
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
        cur.execute("""SELECT data_source, source_name, source_archive, source_variable, filename_prefix, location_type, data_type, units FROM drf_source_meta 
                       WHERE data_type = %(datatype)s ORDER BY filename_prefix""", {'datatype': dataType})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['data_source', 'source_name', 'source_archive', 'source_variable', 'filename_prefix', 'location_type', 'data_type', 'units'])

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
        cur.execute("""SELECT dir_path,file_name,data_date_time,data_source,source_name,source_archive,grid_name,model_run_id,timemark,location_type,csvurl,ingested
                       FROM drf_apsviz_station_file_meta
                       WHERE model_run_id = %(modelrunid)s""", {'modelrunid': modelRunID})
    
        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['dir_path','file_name','data_date_time','data_source','source_name','source_archive','grid_name',
                                                   'model_run_id','timemark','location_type','csvurl','ingested']) 
        
        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def getRetainObsStationInfo():
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
        logger.info(error)

def runHarvestFile(harvestDir, ingestDir, modelRunID):
    ''' This function runs createHarvestDataFileMeta.py, which creates harvest meta data files, that are ingested into the 
        drf_harvest_data_file_meta table, in the database, by running ingestTasks.py using --inputTask ingestHarvestDataFileMeta.
        Parameters
            harvestDir: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db.
        Returns
            None, but it create harvest meta data files, that are then ingested into the drf_harvest_data_file_meta table.
    '''

    if modelRunID != None:
        # Sources from drf_source_meta will not be used in this run
        logger.info('Process data for ADCIRC model run: '+modelRunID)

        # Get input file name, grid_name, timemark, forcing_meta_class, storm, and workflow_type
        filelist, grid_name, advisory, timemark, forcing_meta_class, storm, workflow_type = gdm.getInputFileName(harvestDir,modelRunID)

        for filenamepath in filelist:
            # Define inputFile 
            inputFile = filenamepath.split('/')[-1]

            # Get data date time from inputFile
            datetimes = re.findall(r'(\d+-\d+-\d+T\d+:\d+:\d+)',inputFile) 
            data_date_time = datetimes[0]

            # Split input file name and extract source meta variables from it. The stormnum variable is the strom number (e.g., al03) 
            # in the case of a hurricane run, or the model type (e.g., nam or gfs) in the case of a synoptic run.
            inputFileParts = inputFile.split('_')
            stormnum = inputFileParts[1]
            scenario = inputFileParts[3]
            source_name = inputFileParts[0]
            source_archive = inputFileParts[2].lower()
            # NEED TO CHECK TO SEE IF NOWCAST FILE EXISTS!
            # NOTE: If ecflow launches a nowcast, all you will rcv are nowcast files. If ecflow/asgs pass a forecast but no nowcast exist, then you rcv only forecast files.
            if forcing_meta_class == 'synoptic':
                logger.info('Input file '+inputFile+' data is not from a hurricane, so data source only consists of the scenario and grid name')
                forecast_data_source = scenario+'_'+grid_name
                forecast_obs_station_type = inputFile.split('_'+timemark)[0].split('_')[-1]
                forecast_prefix = source_name+'_'+stormnum+'_'+source_archive.upper()+'_'+scenario+'_'+grid_name+'_FORECAST_'+forecast_obs_station_type
                forecast_prefix_ta = forecast_prefix+'_'+timemark
                nowcast_data_source = 'NOWCAST_'+grid_name
                nowcast_prefix = source_name+'_'+stormnum+'_'+source_archive.upper()+'_NOWCAST_'+grid_name+'_NOWCAST_'+forecast_obs_station_type
                nowcast_prefix_ta = nowcast_prefix+'_'+timemark 
            else:
                logger.info('Input file '+inputFile+' data is from a hurricane, so data source consists of the storm number, scenario and grid name')
                forecast_data_source = stormnum+'_'+scenario+'_'+grid_name
                forecast_obs_station_type = inputFile.split('_'+advisory+'_')[0].split('_')[-1]
                forecast_prefix = source_name+'_'+stormnum+'_'+source_archive.upper()+'_'+scenario+'_'+grid_name+'_FORECAST_'+forecast_obs_station_type
                forecast_prefix_ta = forecast_prefix+'_'+advisory
                nowcast_data_source = stormnum+'_NOWCAST_'+grid_name
                nowcast_prefix = source_name+'_'+stormnum+'_'+source_archive.upper()+'_NOWCAST_'+grid_name+'_NOWCAST_'+forecast_obs_station_type
                nowcast_prefix_ta = nowcast_prefix+'_'+advisory

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
            dfcheck = gdm.checkSourceMeta(forecast_prefix)
   
            # Check if dfcheck is empty, for forecast. If it is a new source is added to the drf_source_meta table ,and then drf_gauge_source table. 
            if dfcheck.empty:
                # Log results
                logger.info('The following source does not exist in the drf_source_meta table:\n '+
                            forecast_data_source+','+source_name+','+source_archive+','+source_variable+','+
                            forecast_prefix_ta+','+location_type+','+units)
                program_list = []
                program_list.append(['python','ingestTasks.py','--inputDataSource',forecast_data_source,'--inputSourceName',source_name,'--inputSourceArchive',
                                     source_archive,'--inputSourceVariable',source_variable,'--inputFilenamePrefix',forecast_prefix,'--inputLocationType',
                                     location_type,'--dataType','model','--inputUnits',units,'--inputTask','ingestSourceMeta'])
                program_list.append(['python','createIngestSourceMeta.py','--ingestDir',ingestDir,'--inputDataSource',forecast_data_source,'--inputSourceName',
                                     source_name,'--inputSourceArchive',source_archive,'--inputUnits',units,'--inputLocationType',location_type])
                program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestSourceData'])
                program_list.append(['python','createHarvestDataFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource', 
                                     forecast_data_source,'--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputFilenamePrefix',
                                     forecast_prefix_ta,'--inputTimemark',timemark])
                program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestHarvestDataFileMeta'])

	            # Check if model type is FORECAST and if so create harvest station file meta and ingest it into the drf_apsviz_stationn table. This table is used
                # to display the stations, that have data for a specific model run, in the apsViz map.
                logger.info('Model type is FORECAST, so meta file, containing station location harvest file meta has to be ingested')
                apsviz_station_meta_filename = 'adcirc_meta_'+"_".join(inputFile.split('_')[1:])
                program_list.append(['python','createApsVizStationFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource',
                                     forecast_data_source,'--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputFilename',
                                     apsviz_station_meta_filename,'--gridName',grid_name,'--modelRunID',modelRunID,'--timeMark',timemark+':00:00',
                                     '--inputLocationType',location_type,'--csvURL',csv_url,'--dataDateTime',data_date_time])
                program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputFilename', 'harvest_meta_files_'+apsviz_station_meta_filename,
                                     '--inputTask','ingestApsVizStationFileMeta'])

                # Run list of program commands using subprocess
                for program in program_list:
                    logger.info('Run '+" ".join(program))
                    output = subprocess.run(program, shell=False, check=True)
                    logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

            else:
                # log results
                logger.info(forecast_data_source+','+source_name+','+source_archive+','+source_variable+','+
                            forecast_prefix_ta+','+location_type+','+units)
                program_list = []
                program_list.append(['python','createHarvestDataFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource', forecast_data_source,
                                     '--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputFilenamePrefix',forecast_prefix_ta,'--inputTimemark', 
                                     timemark])
                program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestHarvestDataFileMeta'])

                # Check if model type is FORECAST and if so create harvest station file meta and ingest it into the drf_apsviz_stationn table. This table is used
                # to display the stations, that have data for a specific model run, in the apsViz map.
                logger.info('Model type is FORECAST, so meta file, containing station location harvest file meta has to be ingested')
                apsviz_station_meta_filename = 'adcirc_meta_'+"_".join(inputFile.split('_')[1:])
                program_list.append(['python','createApsVizStationFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource',
                                     forecast_data_source,'--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputFilename',
                                     apsviz_station_meta_filename,'--gridName',grid_name,'--modelRunID',modelRunID,'--timeMark',timemark+':00:00',
                                     '--inputLocationType',location_type,'--csvURL',csv_url,'--dataDateTime',data_date_time])
                program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputFilename', 'harvest_meta_files_'+apsviz_station_meta_filename,
                                     '--inputTask','ingestApsVizStationFileMeta'])

                # Run list of program commands using subprocess
                for program in program_list:
                    logger.info('Run '+" ".join(program))
                    output = subprocess.run(program, shell=False, check=True)
                    logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

            # Check to see if nowcast source exists
            dfcheck = gdm.checkSourceMeta(nowcast_prefix)

            # Check if dfcheck is empty, for nowcast data. If it is a new source is added to the drf_source_meta table ,and then drf_gauge_source table.
            if dfcheck.empty:
                # Log results
                logger.info('The following source does not exist in the drf_source_meta table:\n '+
                            nowcast_data_source+','+source_name+','+source_archive+','+source_variable+','+
                            nowcast_prefix_ta+','+location_type+','+units)
                program_list = []
                program_list.append(['python','ingestTasks.py','--inputDataSource',nowcast_data_source,'--inputSourceName',source_name,'--inputSourceArchive',
                                     source_archive,'--inputSourceVariable',source_variable,'--inputFilenamePrefix',nowcast_prefix,'--inputLocationType',
                                     location_type,'--dataType','model','--inputUnits',units,'--inputTask','ingestSourceMeta'])
                program_list.append(['python','createIngestSourceMeta.py','--ingestDir',ingestDir,'--inputDataSource',nowcast_data_source,'--inputSourceName',
                                     source_name,'--inputSourceArchive',source_archive,'--inputUnits',units,'--inputLocationType',location_type])
                program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestSourceData'])
                program_list.append(['python','createHarvestDataFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource',
                                     nowcast_data_source,'--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputFilenamePrefix',
                                     nowcast_prefix_ta,'--inputTimemark',timemark])
                program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestHarvestDataFileMeta'])

                # Run list of program commands using subprocess
                for program in program_list:
                    logger.info('Run '+" ".join(program))
                    output = subprocess.run(program, shell=False, check=True)
                    logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

            else:
                # log results
                logger.info(nowcast_data_source+','+source_name+','+source_archive+','+source_variable+','+
                            nowcast_prefix_ta+','+location_type+','+units)
                program_list = []
                program_list.append(['python','createHarvestDataFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource', nowcast_data_source,
                                     '--inputSourceName',source_name,'--inputSourceArchive',source_archive,'--inputFilenamePrefix',nowcast_prefix_ta,'--inputTimemark',
                                     timemark])
                program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestHarvestDataFileMeta'])

                # Run list of program commands using subprocess
                for program in program_list:
                    logger.info('Run '+" ".join(program))
                    output = subprocess.run(program, shell=False, check=True)

    else:
        # get source meta
        df = getSourceMeta('obs')

        # Create list of program commands to ingest meta-data on harvest files
        program_list = []
        for index, row in df.iterrows():
            program_list.append(['python','createHarvestDataFileMeta.py','--harvestDir',harvestDir,'--ingestDir',ingestDir,'--inputDataSource', row['data_source'],
                                 '--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'],'--inputFilenamePrefix',row['filename_prefix']])

        # Ingest meta-data on harvest files created above
        program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestHarvestDataFileMeta'])

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
        program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestRetainObsStationFileMeta'])

        # Run list of program commands using subprocess
        if len(program_list) > 0:
            for program in program_list:
                logger.info('Run '+" ".join(program))
                output = subprocess.run(program, shell=False, check=True)
                logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))
        else:
            logger.info('Program list has 0 length, so continue')

def runDataCreate(ingestDir, dataType):
    ''' This function runs createIngestData.py, which ureates gauge data, from the original harvest data files, that will be 
        ingested into the database using the runDataIngest function.
        Parameters
            dataType: string
                Type of data, obs for observation data, such as noaa gauge data, and model for model such as ADCIRC.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
        Returns
            None, but it runs createIngestData.py, which returns a CSV file
    '''

    # get source meta
    df = getSourceMeta(dataType)

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','createIngestData.py','--ingestDir',ingestDir,'--inputDataSource',row['data_source'],'--inputSourceName',row['source_name'],
                             '--inputSourceArchive',row['source_archive']])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runDataIngest(ingestDir, dataType):
    ''' This function runs ingestTasks.py with --inputTask ingestData, ingest gauge data into the drf_gauge_data table, in the database. 
        Parameters
            dataType: string
                Type of data, obs for observation data, such as noaa gauge data, and model for model such as ADCIRC.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.   
        Returns
            None, but it runs ingestTask.py, which ingest data into the drf_gauge_station table.
    '''

    # get source meta
    df = getSourceMeta(dataType)

    # Create list of program commands
    program_list = []
    for index, row in df.iterrows():
        program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputTask','ingestData','--inputDataSource',row['data_source'],
                             '--inputSourceName',row['source_name'],'--inputSourceArchive',row['source_archive'], '--inputSourceVariable',row['source_variable']])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runApsVizStationCreateIngest(ingestDir, modelRunID):
    ''' This function creates and ingests the apsViz station data from the adcirc meta files, adding a timemark, model_run_id, 
        variable type, and csv URL. It gets this information from the from the drf_apsviz_station_file_meta table, by running the 
        getApsVizStationInfo() function
        Parameters 
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. 
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
        program_list.append(['python','createIngestApsVizStationData.py','--harvestDir',row['dir_path'],'--ingestDir',ingestDir,
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
        program_list.append(['python','ingestTasks.py','--ingestDir',ingestDir,'--inputFilename',row['file_name'],'--inputTask','ingestApsVizStationData'])

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
        program_list.append(['python','ingestTasks.py','--inputFilename',row['file_name'],'--ingestDir',ingestDir,'--inputTask','ingestRetainObsStationData'])

    # Run list of program commands using subprocess
    for program in program_list:
        logger.info('Run '+" ".join(program))
        output = subprocess.run(program, shell=False, check=True)
        logger.info('Ran '+" ".join(program)+" with output returncode "+str(output.returncode))

def runSequenceIngest(harvestDir, ingestDir, dataType, modelRunID):
    ''' Runs the runHarvestFile(), runDataCreate(), and runDataIngest() functions in sequence. If modelRunID has a value
        it also runs the runApsVizStationCreateIngest() function.
        Parameters
            harvestDir: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate,
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
            dataType: string
                Type of data, obs for observation data, such as noaa gauge data, and model for model such as ADCIRC.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db. Used by ingestHarvestDataFileMeta,
                runApsVizStationCreateIngest, and sequenceIngest.
        Returns
            None, but the functions it calls return values, described above.
    '''

    runHarvestFile(harvestDir, ingestDir, modelRunID)
    runDataCreate(ingestDir, dataType)
    runDataIngest(ingestDir, dataType)

    if modelRunID != None:
        logger.info('Data type '+dataType+' needs apsViz stations ingest, into the drf_apsviz_station table, in case of modetype forecast, but not for nowcast')
        runApsVizStationCreateIngest(ingestDir, modelRunID) 
    else:
       logger.info('Data type '+dataType+' needs to have obs stations ingested into the drf_retain_obs_station table')
       runRetainObsStationCreateIngest(ingestDir)

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
            dataType: string
                Type of data, obs for observation data, such as noaa gauge data, and model for model such as ADCIRC. Used by
                DataCreate, DataIngest, and SequenceIngest.
        Returns
            None
    '''         

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
        dataType = args.dataType
        logger.info('Run data create.')
        runDataCreate(ingestDir, dataType)
        logger.info('Ran data create.')
    elif inputTask.lower() == 'dataingest':
        ingestDir = os.path.join(args.ingestDir, '')
        dataType = args.dataType
        logger.info('Run data ingest.')
        runDataIngest(ingestDir, dataType)
        logger.info('Ran data ingest.')
    elif inputTask.lower() == 'runapsvizstationcreateingest':
        ingestDir = os.path.join(args.ingestDir, '')
        modelRunID = args.modelRunID
        logger.info('Run apsViz station create-ingest for model run ID '+modelRunID+'.')
        runApsVizStationCreateIngest(ingestDir, modelRunID)
        logger.info('Ran apsViz station create-ingest for model run ID '+modelRunID+'.')
    elif inputTask.lower() == 'runretainobsstationcreateingest':
        ingestDir = os.path.join(args.ingestDir, '')
        logger.info('Run Retain Obs station create-ingest.')
        runRetainObsStationCreateIngest(ingestDir)
        logger.info('Ran Retain Obs station create-ingest.')
    elif inputTask.lower() == 'sequenceingest':
        harvestDir = os.path.join(args.harvestDir, '')
        ingestDir = os.path.join(args.ingestDir, '')
        dataType = args.dataType
        logger.info('Run sequence ingest.')
        runSequenceIngest(harvestDir, ingestDir, dataType, modelRunID)
        logger.info('Ran sequence ingest.')

# Run main function 
if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            inputTask: string
                The type of task (ingestHarvestDataFileMeta, DataCreate, DataIngest, runApsVizStationCreateIngest,
                SequenceIngest) to be perfomed. The type of inputTask can change what other types of inputs runInget.py
                requires. Below is a list of all inputs, with associated tasks. 
            harvestDir: string
                Directory path to harvest data files. Used by the ingestHarvestDataFileMeta, and ingestHarvestDataFileMeta tasks.
            ingestDir: string
                Directory path to ingest data files, created from the harvest files. Used by ingestHarvestDataFileMeta, DataCreate, 
                DataIngest, runApsVizStationCreateIngest, SequenceIngest.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db. Used by ingestHarvestDataFileMeta,
                runApsVizStationCreateIngest, and sequenceIngest. 
            dataType: string 
                Type of data, obs for observation data, such as noaa gauge data, and model for model such as ADCIRC. Used by
                DataCreate, DataIngest, and SequenceIngest.
        Returns
            None
    '''         

    parser = argparse.ArgumentParser()

    # Non optional argument
    parser.add_argument("--inputTask", help="Input task to be done", action="store", dest="inputTask", choices=['ingestHarvestDataFileMeta',
                        'DataCreate','DataIngest','runApsVizStationCreateIngest','runRetainObsStationCreateIngest','SequenceIngest'], required=True)

    # get runScript argument to use in if statement
    args = parser.parse_known_args()[0]

    # Optional argument
    if args.inputTask.lower() == 'ingestharvestdatafilemeta':
        parser.add_argument("--modelRunID", "--modelRunId", help="Model run ID for ADCIRC forecast data", action="store", dest="modelRunID", required=False)
        parser.add_argument("--harvestDIR", "--harvestDir", help="Harvest directory path", action="store", dest="harvestDir", required=True)
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'datacreate':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
        parser.add_argument("--dataType", "--datatype", help="Data type to be processed, model or obs", action="store", dest="dataType", required=True)
    elif args.inputTask.lower() == 'dataingest':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
        parser.add_argument("--dataType", "--datatype", help="Data type, model or obs", action="store", dest="dataType", required=True)
    elif args.inputTask.lower() == 'runapsvizstationcreateingest':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
        parser.add_argument("--modelRunID", "--modelRunId", help="Model run ID for ADCIRC forecast data", action="store", dest="modelRunID", required=True)
    elif args.inputTask.lower() == 'runapsvizstationcreateingest':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
        parser.add_argument("--modelRunID", "--modelRunId", help="Model run ID for ADCIRC forecast data", action="store", dest="modelRunID", required=True)
    elif args.inputTask.lower() == 'runretainobsstationcreateingest':
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
    elif args.inputTask.lower() == 'sequenceingest':
        parser.add_argument("--harvestDIR", "--harvestDir", help="Harvest directory path", action="store", dest="harvestDir", required=True)
        parser.add_argument("--ingestDIR", "--ingestDir", help="Ingest directory path", action="store", dest="ingestDir", required=True)
        parser.add_argument("--dataType", "--datatype", help="Data type, model or obs", action="store", dest="dataType", required=True)
        parser.add_argument("--modelRunID", "--modelRunId", help="Model run ID for ADCIRC forecast data", action="store", dest="modelRunID", required=False)
    else:
        logger.info(args.inputTask+' not correct')

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)

