import sys
import os

import psycopg
import pandas as pd
from loguru import logger

# Add logger
logger.remove()
log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
logger.add(log_path+'getDashboardMeta.log', level='DEBUG')
logger.add(sys.stdout, level="DEBUG")
logger.add(sys.stderr, level="ERROR")

def flatten(l):
    return [item for sublist in l for item in sublist]

def getADCIRCRunPropertyVariables(modelRunID):
    ''' Returns DataFrame containing a list of variables (forcing.metclass, downloadurl, ADCIRCgrid, time.currentdate, time,currentcycle, advisory), 
        extracted from table ASGS_Mon_config_item, in the asgs_dashboard DB, using the public.get_adcirc_filename_variables_test 
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
            cur.execute("""SELECT * FROM public.get_adcirc_run_property_variables(_run_id := %(modelRunID)s);""", 
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

def checkObsSourceMeta(filename_prefix):
    ''' Returns a DataFrame, that contains source meta-data, queried from the drf_source_obs_meta, using a filename_prefix. This function
        is used by the runHarvestFile() function, in runObsIngest.py, to see if a source exist. This is only done for ADCIRC source. If
        the source does not exist than runHarvestFile() has a method for adding one. 
        Parameters
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob.
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
        cur.execute("""SELECT data_source, source_name, source_archive, source_variable, 
                              filename_prefix, location_type, units 
                       FROM drf_source_obs_meta
                       WHERE filename_prefix = %(filename_prefix)s 
                       ORDER BY filename_prefix""",
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

def checkModelSourceMeta(filename_prefix, source_instance):
    ''' Returns a DataFrame, that contains source meta-data, queried from the drf_source_model_meta, using a filename_prefix. This function
        is used by the runHarvestFile() function, in runModelIngest.py, to see if a source exist. This is only done for ADCIRC source. If
        the source does not exist than runHarvestFile() has a method for adding one. 
        Parameters
            inputFilenamePrefix: string
                Prefix filename to data files that are being ingested. The prefix is used to search for the data files, using glob.
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
        cur.execute("""SELECT data_source, source_name, source_archive, source_variable, source_instance,
                              forcing_metclass, filename_prefix, location_type, units 
                       FROM drf_source_model_meta
                       WHERE filename_prefix = %(filename_prefix)s AND source_instance = %(source_instance)s
                       ORDER BY filename_prefix""",
                       {'filename_prefix':filename_prefix, 'source_instance': source_instance})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['data_source', 'source_name', 'source_archive', 'source_variable', 
                                                   'source_instance', 'forcing_metclass', 'filename_prefix', 'location_type', 
                                                   'units'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

