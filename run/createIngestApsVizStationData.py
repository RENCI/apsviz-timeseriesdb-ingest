#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import os
import sys
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely import wkb, wkt
from loguru import logger

# This function takes as input the harvest directory path, ingest directory path, filename, data source, source name, and source archive.
# It returns a csv file that containes station location data for the drf_apsviz_station table. The function adds a timemark, that it gets
# from the input file name. The timemark values can be used to uniquely query an ADCIRC forecast model run. It also adds a model_run_id,
# variable_type, and csv_url. The model_run_id specifies what model run the data is from, the variable type spcifies the variable (i.e.,
# water_level), and the csvurl is the URL used to access that data from the apsViz interface.
def addApsVizStationFileMeta(harvestDir, ingestDir, inputFilename, modelRunID, timeMark, variableType, csvURL):
    # Create station meta filename from the input file name.
    apsviz_station_meta_filename = 'adcirc_'+"_".join(inputFilename.split('_')[1:])

    # Read input file, convert column name to lower case, rename station column to station_name, convert its data
    # type to string, and add timemark, model_run_id, variable_type, and csvurl columns
    df = pd.read_csv(harvestDir+apsviz_station_meta_filename)
    df.columns= df.columns.str.lower()
    df = df.rename(columns={'station': 'station_name'})
    df = df.astype({"station_name": str})
    df.insert(0, 'geom', '')
    df.insert(0,'timemark', '')
    df.insert(0,'model_run_id', '')
    df.insert(0,'variable_type', '')
    df.insert(0,'csvurl', '')

    # Reorder columns
    df = df.loc[:, ["station_name","lat","lon","name","units","tz","owner","state","county","site","node","geom","timemark","model_run_id","variable_type","csvurl"]]
   
    # Convert latitude and longitude to WKB geometry 
    coordinates = [Point(xy) for xy in zip(df.lon, df.lat)]
    geom = []
    for row in coordinates: 
        geom.append(wkb.dumps(wkt.loads(str(row)), hex=True, srid=4326))
    df['geom'] = geom
 
    # Add model_run_id, timemark, variable_type, and csvurl values to specifies columns in DataFrame
    df['timemark'] = timeMark
    df['model_run_id'] = modelRunID
    df['variable_type'] = variableType
    df['csvurl'] = csvURL

    df.to_csv(ingestDir+'meta_copy_'+apsviz_station_meta_filename, index=False, header=False)

# Main program function takes args as input, which contains the  ingestDir, inputDataSource, inputSourceName, and inputSourceArchive values.
@logger.catch
def main(args):
    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createIngestApsVizStationData.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    harvestDir = os.path.join(args.harvestDir, '')
    ingestDir = os.path.join(args.ingestDir, '')
    inputFilename = args.inputFilename 
    modelRunID = args.modelRunID
    timeMark = args.timeMark
    variableType = args.variableType
    csvURL = args.csvURL
        
    logger.info('Start processing data from data from '+harvestDir+inputFilename+', with output directory '+ingestDir+', model run ID '+
                modelRunID+', timemark '+timeMark+', variable type '+variableType+', and csvURL '+csvURL+'.')
    addApsVizStationFileMeta(harvestDir, ingestDir, inputFilename, modelRunID, timeMark, variableType, csvURL)
    logger.info('Finished processing data from data from '+harvestDir+inputFilename+', with output directory '+ingestDir+', model run ID '+
                modelRunID+', timemark '+timeMark+', variable type '+variableType+', and csvURL '+csvURL+'.')
 
# Run main function takes harvestDir, ingestDir, inputFilename, and timeMark as input.
if __name__ == "__main__": 
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    #'--modelRunID',row['model_run_id'],'--timeMark',row['timemark'],'--variableType', row['variable_type'],'--csvURL',row['csvurl']]
    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--harvestDIR", "--harvestDir", help="Input directory path", action="store", dest="harvestDir", required=True)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--inputFilename", help="Input file name containing meta data on apsViz stations", action="store", dest="inputFilename", required=True)
    parser.add_argument("--modelRunID", help="Model run ID for model run", action="store", dest="modelRunID", required=True)
    parser.add_argument("--timeMark", help="Time model run started", action="store", dest="timeMark", required=True)           
    parser.add_argument("--variableType", help="Type of variable (i.e., 'water_level')", action="store", dest="variableType", required=True)
    parser.add_argument("--csvURL", help="URL to SQL function that will retrieve a csv file", action="store", dest="csvURL", required=True)

    args = parser.parse_args() 
    main(args)

