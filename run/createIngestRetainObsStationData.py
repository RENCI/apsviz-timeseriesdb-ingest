#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import os
import sys
import psycopg
import pandas as pd
from shapely.geometry import Point
from shapely import wkb, wkt
from loguru import logger

def getGaugeStationInfo(stationNames):
    ''' Returns DataFrame containing variables from the drf_gauge_station table. It takes a list of station 
        names as input.
        Parameters
            stationNames: list
                List of station names
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
        cur.execute("""SELECT station_name, lat, lon, tz, gauge_owner, location_name, country, state, county, geom
                       FROM drf_gauge_station
                       WHERE station_name = ANY(%(station_names)s)""", {'station_names': stationNames})

        # convert query output to Pandas dataframe
        df = pd.DataFrame(cur.fetchall(), columns=['station_name', 'lat', 'lon', 'tz', 'gauge_owner', 
                                                   'location_name', 'country', 'state', 'county', 'geom'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # return DataFrame
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def addApsVizStationFileMeta(harvestDir, ingestDir, inputFilename, timeMark, modelRunID, inputDataSource, 
                             inputSourceArchive, gridName, csvURL):
    ''' Returns a csv file that containes station location data for the drf_apsviz_station table. The function adds a 
        timemark, that it gets from the input file name. The timemark values can be used to uniquely query an ADCIRC 
        forecast model run. It also adds a model_run_id, data_source, source_archive, grid_name, and csv_url. The 
        model_run_id specifies what model run the data is from, and the csvurl is the URL used to access that data 
        from the apsViz interface.
        Parameters
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputFilename: string
                The name of the input file
            timeMark: datatime 
                Date and time of the beginning of the model run for forecast runs, and end of the model run for 
                nowcast runs.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, 
                wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            gridName: string
                Name of grid being used in model run (e.g., ed95d, hsofs NCSC_SAB_v1.23...)
            csvURL:
                URL to SQL function that queries db and returns CSV file of data
        Returns
            CSV file
    '''

    # Create station meta filename from the input file name.
    apsviz_station_meta_filename = 'adcirc_'+"_".join(inputFilename.split('_')[1:])

    # Read input file, convert column name to lower case, rename station column to station_name, convert its data
    # type to string, and add timemark, model_run_id, and csvurl columns
    dfApsVizStations = pd.read_csv(harvestDir+apsviz_station_meta_filename)
    dfApsVizStations.columns= dfApsVizStations.columns.str.lower()
    dfApsVizStations = dfApsVizStations.rename(columns={'station': 'station_name'})
    dfApsVizStations = dfApsVizStations.astype({"station_name": str})
    
    # Get stations from drf_gauge_station for station names in dfApsVizStations
    df = getGaugeStationInfo(dfApsVizStations["station_name"].values.tolist())

    # Add new columns
    df.insert(0,'timemark', '')
    df.insert(0,'model_run_id', '')
    df.insert(0,'data_source', '')
    df.insert(0,'source_archive', '')
    df.insert(0,'grid_name', '')
    df.insert(0,'csvurl', '')

    # Reorder columns
    df = df.loc[:, ["station_name","lat","lon","location_name","tz","gauge_owner","country","state","county","geom","timemark",
                    "model_run_id","data_source","source_archive","grid_name","csvurl"]]
 
    # Add model_run_id, timemark, and csvurl values to specifies columns in DataFrame
    timemark = "T".join(timeMark.split(' ')).split('+')[0]+'Z'
    df['timemark'] = timemark
    df['model_run_id'] = modelRunID
    df['data_source'] = inputDataSource
    df['source_archive'] = inputSourceArchive
    df['grid_name'] = gridName

    # Create csvURL and add it to DataFrame
    for index, row in df.iterrows():
        csvURL = os.environ['UI_DATA_URL']+'/get_station_data?station_name='+row['station_name']+'&time_mark='+timemark+'&data_source='+inputDataSource
        df.at[index,'csvurl'] = csvURL

    # Write DataFrame to CSV file
    df.to_csv(ingestDir+'meta_copy_'+apsviz_station_meta_filename, index=False, header=False)

# Main program function takes args as input, which contains the  ingestDir, inputDataSource, inputSourceName, and inputSourceArchive values.
@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, runs createFileList, and writes output to CSV file.
        The CSV file will be ingest into table drf_apsviz_station_file_meta during runHarvestFile() is run in runIngest.py
        Parameters
            args: dictionary 
                contains the parameters listed below
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputFilename: string
                The name of the input file
            timeMark: datatime
                Date and time of the beginning of the model run for forecast runs, and end of the model run for nowcast runs.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            gridName: string 
                Name of grid being used in model run (e.g., ed95d, hsofs NCSC_SAB_v1.23...)
            csvURL:
                URL to SQL function that queries db and returns CSV file of data
        Returns
            CSV file
    '''

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
    timeMark = args.timeMark
    modelRunID = args.modelRunID
    inputDataSource = args.inputDataSource
    inputSourceArchive = args.inputSourceArchive
    gridName = args.gridName
    csvURL = args.csvURL
        
    logger.info('Start processing data from '+harvestDir+inputFilename+', with output directory '+ingestDir+', model run ID '+
                modelRunID+', timemark '+timeMark+', and csvURL '+csvURL+'.')
    addApsVizStationFileMeta(harvestDir, ingestDir, inputFilename, timeMark, modelRunID, inputDataSource, inputSourceArchive, gridName, csvURL)
    logger.info('Finished processing data from '+harvestDir+inputFilename+', with output directory '+ingestDir+', model run ID '+
                modelRunID+', timemark '+timeMark+', and csvURL '+csvURL+'.')
 
# Run main function takes harvestDir, ingestDir, inputFilename, and timeMark as input.
if __name__ == "__main__": 
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputFilename: string
                The name of the input file
            timeMark: datatime
                Date and time of the beginning of the model run for forecast runs, and end of the model run for nowcast runs.
            modelRunID: string
                Unique identifier of a model run. It combines the instance_id, and uid from asgs_dashboard db
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer,
                NAMFORECAST_NCSC_SAB_V1.23...). Used by ingestSourceMeta, and ingestData.
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            gridName: string 
                Name of grid being used in model run (e.g., ed95d, hsofs NCSC_SAB_v1.23...) 
            csvURL:
                URL to SQL function that queries db and returns CSV file of data
        Returns
            None
    '''

    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--harvestDIR", "--harvestDir", help="Input directory path", action="store", dest="harvestDir", required=True)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--inputFilename", help="Input file name containing meta data on apsViz stations", action="store", dest="inputFilename", required=True)
    parser.add_argument("--timeMark", help="Time model run started", action="store", dest="timeMark", required=True)
    parser.add_argument("--modelRunID", help="Model run ID for model run", action="store", dest="modelRunID", required=True)
    parser.add_argument("--inputDataSource", help="Input data source to be processed", action="store", dest="inputDataSource", required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", choices=['noaa','ndbc','contrails','renci'], required=True) 
    parser.add_argument("--gridName", help="Name of grid being used in model run", action="store", dest="gridName", required=True)
    parser.add_argument("--csvURL", help="URL to SQL function that will retrieve a csv file", action="store", dest="csvURL", required=True)

    args = parser.parse_args() 
    main(args)

