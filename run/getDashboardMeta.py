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

# Function that queries the ASGS_Mon_config_item table, in the asgs_dashboard database,
# using the public.get_adcirc_filename_variables SQL function with modelRunID as input, 
# and returns a DataFrame with values for track_raw_fst, downloadurl, ADCIRCgrid, RunStartTime,
# and advisory
def getADCIRCFileNameVariables(modelRunID):
    try:
        # Create connection to database, set autocommit, and get cursor
        with psycopg.connect(dbname=os.environ['SQL_ASGS_DATABASE'], user=os.environ['SQL_ASGS_USER'], 
                             host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], 
                             password=os.environ['SQL_ASGS_PASSWORD']) as conn:
            cur = conn.cursor()

            # Set enviromnent
            cur.execute("""SET CLIENT_ENCODING TO UTF8""")
            cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")

            # Run query
            cur.execute("""SELECT * FROM public.get_adcirc_filename_variables(_run_id := %(modelRunID)s);""", 
                        {'modelRunID':modelRunID})

            # convert query output to Pandas dataframe
            #df = pd.DataFrame(cur.fetchall(), columns=['track_raw_fst'])
            df = pd.DataFrame.from_dict(cur.fetchall()[0], orient='columns')
            #values = cur.fetchall()

            # Close cursor and database connection
            cur.close()
            conn.close()

            # Return Pandas dataframe
            return(df)
            #return(values)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        print(error)

# Function that takes as input a HarvestDir, ingestDir and modelRunID, 
# and uses the modelRunID to query the ASGS_Mon_config_item table in the 
# asgs_dashboard database to get variables that are then used to create 
# a ADCIRC timeseries filename, that is retuned along with a grid value
# timemark, and stormTrack
def getInputFile(harvestDir,ingestDir,modelRunID ):
    # Get ADCIRC filename variables
    df = getADCIRCFileNameVariables(modelRunID)
    
    # Get storm track
    stormTrack = df['track_raw_fst'].values[0]

    # Check it run is from a hurricane
    if stormTrack == 'notrack':
        # Get downloadurl, and extract model run type
        model = df['downloadurl'].values[0].split('/')[-1].upper()
        
        # Get grid
        grid = df['ADCIRCgrid'].values[0].upper()

        # Get advisory number or date for synoptic runs
        advisory = df['advisory'].values[0]
        
        # Get startTime, extracet time variables, and create timemark
        startTime = df['RunStartTime'].values[0]
        year = startTime[0:4]
        month = startTime[4:6]
        day = startTime[6:8]
        hour = startTime[8:11]
        timemark = year+'-'+month+'-'+day+'T'+hour
        
        # Search for file name, and return it
        filelist = glob.glob(harvestDir+'adcirc_[!meta]*_'+model+'_'+grid+'_'+model[3:]+'_'+timemark+'*.csv')
        if len(filelist) == 0:
            return('adcirc_[!meta]*_'+model+'_'+grid+'_'+model[3:]+'_'+timemark+'*.csv', modelRunID)
        elif len(filelist) == 1:
            return(filelist[0], grid, advisory, timemark, stormTrack)
        else:
            return(modelRunID)
    else:
        # Extract storm ID from stormTrack
        storm = stormTrack[0:4]

        # Get downloadurl, and extract model run type
        model = df['downloadurl'].values[0].split('/')[-1].upper()
        
        # Get grid
        grid = df['ADCIRCgrid'].values[0].upper()
        
        # Get advisory number
        advisory = df['advisory'].values[0]
        
        # Get startTime, extracet time variables, and create timemark
        startTime = df['RunStartTime'].values[0]
        year = startTime[0:4]
        month = startTime[4:6]
        day = startTime[6:8]
        hour = str(int(startTime[8:11])+1)
        timemark = year+'-'+month+'-'+day+'T'+hour

        # Search for file name, and return it
        filelist = glob.glob(harvestDir+'adcirc_'+storm+'_*_'+model+'_'+grid+'_*_'+advisory+'_*.csv')
        if len(filelist) == 0:
            return('adcirc_'+storm+'_*_'+model+'_'+grid+'_*_'+advisory+'_*.csv', modelRunID)
        elif len(filelist) == 1:
            return(filelist[0], grid, advisory, timemark, stormTrack)
        else:
            return(modelRunID)

# This function is used by the runIngestSource() function to query the drf_source_meta table, in the
# database, and get argparse input for those function
def checkSourceMeta(filename_prefix):
    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['SQL_GAUGE_DATABASE'], user=os.environ['SQL_GAUGE_USER'], 
                               host=os.environ['SQL_HOST'], port=os.environ['SQL_PORT'], 
                               password=os.environ['SQL_GAUGE_PASSWORD'])
        cur = conn.cursor()

        # Set enviromnent
        cur.execute("""SET CLIENT_ENCODING TO UTF8""")
        cur.execute("""SET STANDARD_CONFORMING_STRINGS TO ON""")
        cur.execute("""BEGIN""")

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
        print(error)

