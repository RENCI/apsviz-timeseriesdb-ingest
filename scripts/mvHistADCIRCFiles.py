import os
import sys
import glob
import shutil
import psycopg
import pandas as pd
import argparse
from loguru import logger

# This function queries the ASGS_Mon_config_item, in the asgs_dashboard DB, for instance ids using a uid
def getInstanceID(uid):
    ''' Return an instance_id from the ASGS_Mon_config_item table in the asgs_dashboard DB. It taks a uid as input.
        Parameters
            uid: string
                An ADCIRC runs identifier
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
            #instance_id = '4358'  = %(ADCIRCgrid)s
            cur.execute("""SELECT DISTINCT instance_id FROM "ASGS_Mon_config_item" 
                           WHERE uid = %(uid)s""", 
                        {'uid':uid})

            # convert query output to Pandas dataframe
            df = pd.DataFrame(cur.fetchall(), columns=['instance_id'])

            # Close cursor and database connection
            cur.close()
            conn.close()

            # Return Pandas dataframe
            return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

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

def createFileList(inputDirPath,inputFileNamePrefix,runIDsPath,inputSystem):
    ''' This function uses the ADCIRC harvest file names to find the modelRunID (instance_id+uid) for each files, and creates and list, 
        with the file name, modelRunID and other meta data, and writes that list to a csv file. The csv files are used by the copyFiles2SubDirectory
        to create sub-directories using the modelRunID and the directory name, and then copy the ADCIRC files, associated with that modelRunID,
        renameing the files using a shorter file name.
        Parameters
            inputDirPath: string
                The Directory path to where the ADCIRC harvest files are located
            inputFileNamePrefix: string
                The file name prefix of the ADCIRC files to be processed (e.g., adcirc_gfs_RENCI_GFSFORECAST_*, adcirc_al08_RENCI_OFCL_EC95D_FORECAST_* ...)
            runIDsPath: string
                The directory path for the runIDs csv file.
            inputSystem: string
                The system (e.g., dev, prod, aws) where the files are being processed. 
        Returns
            None
    '''
    inpathfiles = glob.glob(inputDirPath+inputFileNamePrefix)

    runIDs = []
    for inpathfile in inpathfiles:
        infile = inpathfile.split('/')[-1]
        parts = infile.split('_')
        dparts = "".join(parts[-2].split('-')).split('T')
        time_currentdate = dparts[0]
        time_currentcycle = dparts[1].split(':')[0]
        advisory = "".join("".join(parts[-3].split('-')).split(':')[0].split('T'))
        suite_model = parts[0]
        storm = parts[1]
        
        # Check for coamps
        if storm == 'coamps':
            storm = parts[1]+'_'+parts[2]
            physical_location = parts[3]
            forcing_ensemblename = parts[4].lower()            
        else:
            physical_location = parts[2]
            forcing_ensemblename = parts[3].lower()

        # Extract grid
        if forcing_ensemblename == 'nowcast':
            ADCIRCgrid = infile.split('_NOWCAST_')[1]
        else:
            ADCIRCgrid = infile.split('_FORECAST_')[0].split('_'+forcing_ensemblename.upper()+'_')[1]

        uid = advisory+'-'+forcing_ensemblename
    
        dfiid = getInstanceID(uid)

        for index, row in dfiid.iterrows():
            instance_id = str(row['instance_id'])
            run_id = instance_id+'-'+uid
        
            dfrpv = getADCIRCRunPropertyVariables(run_id)
            if dfrpv['suite.model'].values[0] == 'adcirc':
                if dfrpv['storm'].values[0] == 'none' or dfrpv['storm'].values[0] == 'None' :
                    if dfrpv['physical_location'].values[0] == physical_location and dfrpv['ADCIRCgrid'].values[0].upper() == ADCIRCgrid:
                        runIDs.append([run_id,inputDirPath,infile,dfrpv['suite.model'].values[0],suite_model,
                                       dfrpv['forcing.ensemblename'].values[0],forcing_ensemblename,
                                       dfrpv['storm'].values[0],storm,dfrpv['physical_location'].values[0],
                                       physical_location,dfrpv['advisory'].values[0],advisory,
                                       dfrpv['ADCIRCgrid'].values[0],ADCIRCgrid,dfrpv['forcing.metclass'].values[0],
                                       dfrpv['instancename'].values[0]])
                    else:
                        continue
                else:
                    if dfrpv['storm'].values[0] == storm and dfrpv['physical_location'].values[0] == physical_location and dfrpv['ADCIRCgrid'].values[0].upper() == ADCIRCgrid:
                        runIDs.append([run_id,inputDirPath,infile,dfrpv['suite.model'].values[0],suite_model,
                                       dfrpv['forcing.ensemblename'].values[0],forcing_ensemblename,
                                       dfrpv['storm'].values[0],storm,dfrpv['physical_location'].values[0],
                                       physical_location,dfrpv['advisory'].values[0],advisory,
                                       dfrpv['ADCIRCgrid'].values[0],ADCIRCgrid,dfrpv['forcing.metclass'].values[0],
                                       dfrpv['instancename'].values[0]])
                    else:
                        continue
            else:
                continue

    dfgfsfforecast = pd.DataFrame(runIDs, columns=['run_id','dir_path','file_name','model_db','model_file',
                                                   'ensemble_db','ensemble_file','storm_db','storm_file',
                                                   'location_db','location_file','advisory_db','advisory_file',
                                                   'ADCIRCgrid_db','ADCIRCgrid_file','forcing','instance'])

    dfgfsfforecast.to_csv(runIDsPath+inputFileNamePrefix.split('*')[0]+'runids_'+inputSystem+'.csv', index=False)

def copyFiles2SubDirectory(runIDsFile):
    ''' This function uses the csv files, produced by the createFileList function, to create sub-directories using the modelRunID and the 
        directory name, and then copy the ADCIRC files, associated with that modelRunID, renameing the files using a shorter file name.
        Parameters
            runIDsFile: string
                The csv that has the run IDs
        Returns
            None
    '''
    df = pd.read_csv(runIDsFile)

    for index, row in df.iterrows():
        modelRunID = row['run_id']
        inputDirPath = row['dir_path']
        forecastFileName = row['file_name']
    
        if not os.path.exists(inputDirPath+modelRunID):
            os.mkdir(inputDirPath+modelRunID)

        # Get nowcast filename
        parts = forecastFileName.split('_')
        stationType = parts[-4]

        # Copy forecast file to sub directory
        shutil.copyfile(inputDirPath+forecastFileName, inputDirPath+modelRunID+'/'+'FORECAST_'+stationType+'.csv')
        
        # Create meta file for forecast and copy to subdirectory
        parts.insert(1, 'meta')
        metaForecastFileName = "_".join(parts)
        shutil.copyfile(inputDirPath+metaForecastFileName, inputDirPath+modelRunID+'/'+'meta_FORECAST_'+stationType+'.csv')
        
        parts = forecastFileName.split('_')
        if parts[1] == 'coamps':
            parts[-1] = parts[-2]+'.csv'
            parts[-2] = '*'
            parts[4] = 'NOWCAST'
            parts[-5]= 'NOWCAST'
            nowcastFileName = "_".join(parts)
            nowcastPathFile = glob.glob(inputDirPath+nowcastFileName)
        else:
            parts[-1] = parts[-2]+'.csv'
            parts[-2] = '*'
            parts[3] = 'NOWCAST'
            parts[-5]= 'NOWCAST'
            nowcastFileName = "_".join(parts)
            nowcastPathFile = glob.glob(inputDirPath+nowcastFileName)
        
        # Check it nowcast file exists
        if len(nowcastPathFile) > 0:
            # Extract nowcastFileName from list
            nowcastFileName = nowcastPathFile[0].split('/')[-1]

            # Copy nowcast files to sub directory
            shutil.copyfile(inputDirPath+nowcastFileName, inputDirPath+modelRunID+'/'+'NOWCAST_'+stationType+'.csv')

            # Create meta file for nowcast and copy to subdirectory
            parts = nowcastFileName.split('_')
            parts.insert(1, 'meta')
            metaNowcastFileName = "_".join(parts)
            shutil.copyfile(inputDirPath+metaNowcastFileName, inputDirPath+modelRunID+'/'+'meta_NOWCAST_'+stationType+'.csv')


@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, and runs specified task.
        Parameters
            args: dictionary
                contains the parameters listed below.
            inputDirPath: string
                The Directory path to where the ADCIRC harvest files are located
            inputFileNamePrefix: string
                The file name prefix of the ADCIRC files to be processed (e.g., adcirc_gfs_RENCI_GFSFORECAST_*, adcirc_al08_RENCI_OFCL_EC95D_FORECAST_* ...)
            inputSystem: string
                The system (e.g., dev, prod, aws) where the files are being processed. 
        Returns
            None
    '''         

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'mvHistADCIRCFiles.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # extract args to variables
    inputDirPath = os.path.join(args.inputDirPath, '')
    inputFileNamePrefix = args.inputFileNamePrefix
    inputSystem = args.inputSystem

    # create runIDsPath and make the directory if it does not exist.
    runIDsPath = os.getcwd()+'/runIDs/'
    if not os.path.exists(runIDsPath): 
        os.mkdir(runIDsPath)
        logger.info("Directory %s created!" % runIDsPath)
    else:       
        logger.info("Directory %s already exists" % runIDsPath)

    # run createFileList() function
    logger.info('Run createFileList() for file name prefix '+inputFileNamePrefix+', on system '+inputSystem+'.')
    createFileList(inputDirPath,inputFileNamePrefix,runIDsPath,inputSystem)
    logger.info('Ran createFileList() for file name prefix '+inputFileNamePrefix+', on system '+inputSystem+'.')

    # create runIDsFile name
    runIDsFile = runIDsPath+inputFileNamePrefix[:-1]+'runids_'+inputSystem+'.csv'

    # run copyFiles2SubDirectory(runIDsFile)
    logger.info('Run copyFiles2SubDirectory() on runIDsFile '+runIDsFile)
    copyFiles2SubDirectory(runIDsFile)
    logger.info('Run copyFiles2SubDirectory() on runIDsFile '+runIDsFile)

# Run main function 
if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            inputDirPath: string
                The Directory path to where the ADCIRC harvest files are located
            inputFileNamePrefix: string
                The file name prefix of the ADCIRC files to be processed (e.g., adcirc_gfs_RENCI_GFSFORECAST_*, adcirc_al08_RENCI_OFCL_EC95D_FORECAST_* ...)
            inputSystem: string
                The system (e.g., dev, prod, aws) where the files are being processed. 
        Returns
            None
    '''         

    parser = argparse.ArgumentParser()

    # Non optional argument
    parser.add_argument("--inputDirPath", help="Harvest directory path", action="store", dest="inputDirPath", required=True)
    parser.add_argument("--inputFileNamePrefix", help="Harvest directory path", action="store", dest="inputFileNamePrefix", required=True)
    parser.add_argument("--inputSystem", help="The system (e.g., dev, prod, aws) where the files are being processed", action="store", dest="inputSystem", required=True)

    # Parse arguments
    args = parser.parse_args()

    # Run main
    main(args)
