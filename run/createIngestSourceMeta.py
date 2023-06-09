#!/usr/bin/env python
# coding: utf-8

# Import python modules
import argparse
import psycopg
import sys
import os
import pandas as pd
from loguru import logger

def getStationID(locationType):
    ''' Returns a DataFrame containing a list of station ids and station names, based on the location type (COASTAL, TIDAL or RIVERS), 
        from table drf_gauge_station.
        Parameters
            locationType: string
                gauge location type (COASTAL, TIDAL, or RIVERS) 
        Returns
            DataFrame
    '''

    try:
        # Create connection to database and get cursor
        conn = psycopg.connect(dbname=os.environ['ASGS_GAUGES_DATABASE'], user=os.environ['ASGS_GAUGES_USERNAME'], host=os.environ['ASGS_GAUGES_HOST'], port=os.environ['ASGS_GAUGES_PORT'], password=os.environ['ASGS_GAUGES_PASSWORD'])
        cur = conn.cursor()

        # Run query 
        cur.execute("""SELECT station_id, station_name FROM drf_gauge_station
                       WHERE location_type = %(location_type)s
                       ORDER BY station_name""", 
                    {'location_type': locationType})
       
        # convert query output to Pandas dataframe 
        df = pd.DataFrame(cur.fetchall(), columns=['station_id', 'station_name'])

        # Close cursor and database connection
        cur.close()
        conn.close()

        # Return Pandas dataframe
        return(df)

    # If exception log error
    except (Exception, psycopg.DatabaseError) as error:
        logger.info(error)

def addMeta(ingestDir, inputDataSource, inputSourceName, inputSourceArchive, inputUnits, inputLocationType):
    ''' Returns a CSV file that containes source information specific to station IDs that have been extracted from the drf_gauge_station table.
        The function adds additional source information (data source, source name, source archive, data units) to the station IDs. This 
        information is latter ingested into table drf_gauge_source by running the ingestSourceData() function in ingetTask.py
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars)
            inputLocationType: string
                gauge location type (COASTAL, TIDAL, or RIVERS)
        Returns
            CSV file
    '''

    df = getStationID(inputLocationType)

    df['data_source'] = inputDataSource
    df['source_name'] = inputSourceName
    df['source_archive'] = inputSourceArchive
    df['units'] = inputUnits

    # Drop station_name from DataFrame 
    df.drop(columns=['station_name'], inplace=True)

    # Reorder column name and update indeces 
    newColsOrder = ['station_id','data_source','source_name','source_archive','units']
    df=df.reindex(columns=newColsOrder)

    # Write dataframe to csv file 
    outputFile = 'source_'+inputSourceName+'_stationdata_'+inputSourceArchive+'_'+inputLocationType+'_'+inputDataSource+'_meta.csv'
    df.to_csv(ingestDir+outputFile, index=False, header=False)

# Main program function takes args as input, which contains the ingestDir, and outputFile values.
@logger.catch
def main(args):
    ''' Main program function takes args as input, starts logger, runs addMeta(), which writes output to CSV file.
        The CSV file will be ingest into table drf_gauge_source when ingestSourceData() function is run in ingetTask.py
        Parameters
            args: dictionary
                contains the parameters listed below
            harvestDir: string
                Directory path to harvest data files
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars)
            inputLocationType: string
                gauge location type (COASTAL, TIDAL, or RIVERS)
        Returns
            CSV file
    '''

    # Add logger
    logger.remove()
    log_path = os.path.join(os.getenv('LOG_PATH', os.path.join(os.path.dirname(__file__), 'logs')), '')
    logger.add(log_path+'createIngestSourceMeta.log', level='DEBUG')
    logger.add(sys.stdout, level="DEBUG")
    logger.add(sys.stderr, level="ERROR")

    # Extract args variables
    ingestDir = os.path.join(args.ingestDir, '')
    inputDataSource = args.inputDataSource
    inputSourceName = args.inputSourceName
    inputSourceArchive = args.inputSourceArchive
    inputUnits = args.inputUnits
    inputLocationType = args.inputLocationType

    logger.info('Start processing source data for data source '+inputDataSource+', with source name '+inputSourceName+', source archive '+inputSourceArchive+', and location type '+inputLocationType+'.')

    # Run addMeta function
    addMeta(ingestDir, inputDataSource, inputSourceName, inputSourceArchive, inputUnits, inputLocationType)
    logger.info('Finished processing source data for file data source '+inputDataSource+', with source name '+inputSourceName+', source archive '+inputSourceArchive+', and location type '+inputLocationType+'.')

# Run main function takes ingestDir, and outputFile as input.
if __name__ == "__main__":
    ''' Takes argparse inputs and passes theme to the main function
        Parameters
            ingestDir: string
                Directory path to ingest data files, created from the harvest files
            inputDataSource: string
                Unique identifier of data source (e.g., river_gauge, tidal_predictions, air_barameter, wind_anemometer, NAMFORECAST_NCSC_SAB_V1.23...)
            inputSourceName: string
                Organization that owns original source data (e.g., ncem, ndbc, noaa, adcirc...)
            inputSourceArchive: string
                Where the original data source is archived (e.g., contrails, ndbc, noaa, renci...)
            inputUnits: string
                Units of data (e.g., m (meters), m^3ps (meter cubed per second), mps (meters per second), and mb (millibars)
            inputLocationType: string
                Gauge location type (COASTAL, TIDAL, or RIVERS)
        Returns
            None
    '''         

    parser = argparse.ArgumentParser()

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--ingestDIR", "--ingestDir", help="Output directory path", action="store", dest="ingestDir", required=True)
    parser.add_argument("--inputDataSource", help="Input data source name", action="store", dest="inputDataSource", required=True)
    parser.add_argument("--inputSourceName", help="Input source name", action="store", dest="inputSourceName", choices=['adcirc','noaa','ndbc','ncem'], required=True)
    parser.add_argument("--inputSourceArchive", help="Input source archive name", action="store", dest="inputSourceArchive", choices=['noaa','ndbc','contrails','renci'], required=True) 
    parser.add_argument("--inputUnits", help="Input units", action="store", dest="inputUnits", required=True)
    parser.add_argument("--inputLocationType", help="Input location type", action="store", dest="inputLocationType", required=True)

    # Parse input arguments
    args = parser.parse_args()

    # Run main
    main(args)

