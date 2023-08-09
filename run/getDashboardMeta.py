import sys
import os
import glob
import psycopg
import pandas as pd
from loguru import logger

# Add logger
logger.remove()
log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
logger.add(log_path+'getDashboardMeta.log', level='DEBUG')
logger.add(sys.stdout, level="DEBUG")
logger.add(sys.stderr, level="ERROR")

def getADCIRCFileNameVariables(modelRunID):
    ''' Returns DataFrame containing a list of variables (forcing.metclass, downloadurl, ADCIRCgrid, time.currentdate, time,currentcycle, advisory), 
        extracted from table ASGS_Mon_config_item, in the asgs_dashboard DB, using the public.get_adcirc_filename_variables 
        SQL function with modelRunID as input. These variables are used to construct filenames.
        Parameters
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
        Returns
            DataFrame
    '''

    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['ASGS_DB_DATABASE'], 
                             user=os.environ['ASGS_DB_USERNAME'], 
                             host=os.environ['ASGS_DB_HOST'], 
                             port=os.environ['ASGS_DB_PORT'], 
                             password=os.environ['ASGS_DB_PASSWORD']) as conn:
            cur = conn.cursor()

            # Run query
            cur.execute("""SELECT * FROM public.get_adcirc_filename_variables(_run_id := %(modelRunID)s);""", 
                        {'modelRunID':modelRunID})

            # convert query output to Pandas dataframe
            df = pd.DataFrame.from_dict(cur.fetchall()[0], orient='columns')

            # Close cursor and database connection
            cur.close()
            conn.close()

            # Return Pandas dataframe
            return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def getInputFileName(harvestDir,modelRunID):
    ''' Returns a file name, with directory path, that will be used to search for the file using glob. It uses 
        the getADCIRCFileNameVariables(modelRunID) function to get a list of variables, by using a modelRunID 
        to query the ASGS_Mon_config_item table in the  asgs_dashboard database. the variables are then used to 
        construct a filename. 
        Parameters
            harvestDir: string
                Directory path to harvest data files
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
        Returns
            File name, with directory path.
    '''

    # Get ADCIRC filename variables
    df = getADCIRCFileNameVariables(modelRunID)
    
    # Get forcing metaclass 
    forcingMetaClass = df['forcing.metclass'].values[0]
    workflowType = df['workflow_type'].values[0]

    # Check it run is from a hurricane. 
    if forcingMetaClass == 'synoptic':
        # Create storm variable with value of None since this is a synoptic run
        storm = None

        # Get downloadurl, and extract model run type
        model = df['downloadurl'].values[0].split('/')[-1].upper()
        
        # Get grid
        grid = df['ADCIRCgrid'].values[0].upper()

        # Get advisory number or date for synoptic runs
        advisory = df['advisory'].values[0]
        
        # Get time.currentdate, and timecurrentcycle, extract time variables, and create timemark
        # This step was add for ECFLOW runs
        currentDate = df['time.currentdate'].values[0]
        year = '20'+currentDate[0:2]
        month = currentDate[2:4]
        day = currentDate[4:6]
        hour = df['time.currentcycle'].values[0]
        timemark = year+'-'+month+'-'+day+'T'+hour
      
        # Search for file name, and return it
        filelist = glob.glob(harvestDir+'adcirc_[!meta]*_'+model+'_'+grid+'_'+model[3:]+'_*_'+timemark+'*.csv')
        if len(filelist) == 0:
            logger.info('The following file: adcirc_[!meta]*_'+model+'_'+grid+'_'+model[3:]+'_*_'+timemark+'*.csv was not found for model run ID: '+modelRunID)
            sys.exit(0)
        elif len(filelist) > 0:
            return(filelist, grid, advisory, timemark, forcingMetaClass, storm, workflowType)
        else:
            return(modelRunID)
    else:
        # Extract storm ID from forcingMetaClass
        storm = 'al'+df['stormnumber'].values[0].zfill(2)

        # Get downloadurl, and extract model run type
        model = df['downloadurl'].values[0].split('/')[-1].upper()
        
        # Get grid
        grid = df['ADCIRCgrid'].values[0].upper()
        
        # Get advisory number
        advisory = df['advisory'].values[0]
        
        # Get startTime, extracet time variables, and create timemark
        currentDate = df['time.currentdate'].values[0]
        year = '20'+currentDate[0:2]
        month = currentDate[2:4]
        day = currentDate[4:6]
        hour = df['time.currentcycle'].values[0]
        timemark = year+'-'+month+'-'+day+'T'+hour

        # Search for file name, and return it
        filelist = glob.glob(harvestDir+'adcirc_'+storm+'_*_'+model+'_'+grid+'_*_'+advisory+'_*.csv')
        if len(filelist) == 0:
            logger.info('The following file: adcirc_'+storm+'_*_'+model+'_'+grid+'_*_'+advisory+'_*.csv was not found for model run ID: '+modelRunID)
            sys.exit(0)
        elif len(filelist) > 0:
            return(filelist, grid, advisory, timemark, forcingMetaClass, storm, workflowType)
        else:
            return(modelRunID)

def checkSourceMeta(filename_prefix):
    ''' Returns a DataFrame, that contains source meta-data, queried from the drf_source_meta, using a filename_prefix. This function
        is used by the runHarvestFile() function, in runIngest.py, to see if a source exist. This is only done for ADCIRC source. If
        the source does not exist than runHarvestFile() has a method for adding one. 
        Parameters
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob.
        Returns
            DataFrame 
    '''

    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['APSVIZ_GAUGES_DB_DATABASE'], user=os.environ['APSVIZ_GAUGES_DB_USERNAME'], 
                               host=os.environ['APSVIZ_GAUGES_DB_HOST'], port=os.environ['APSVIZ_GAUGES_DB_PORT'], 
                               password=os.environ['APSVIZ_GAUGES_DB_PASSWORD'])
        cur = conn.cursor()

        # Run query
        cur.execute("""SELECT data_source, source_name, source_archive, source_variable, 
                              filename_prefix, location_type, units FROM drf_source_meta
                       WHERE filename_prefix = %(filename_prefix)s ORDER BY filename_prefix""",
                       {'filename_prefix':filename_prefix})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['data_source', 'source_name', 'source_archive', 
                                                   'source_variable', 'filename_prefix', 'location_type', 
                                                   'units'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

