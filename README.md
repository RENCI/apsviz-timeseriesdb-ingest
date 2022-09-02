# apsviz-timeseriesdb-ingest 
The software, in this repo, is used to ingest gauge data from NOAA, NCEM, NDBC, and ADICIRC. To begin ingesting data, you first need to install the apsviz-timeseriesdb repo which can bet downloaded from:  

https://github.com/RENCI/apsviz-timeseriesdb  

Flow the installation instractions for that repo. It creates and postgesql database, that serves data using a Django Rest Framework (DRF) app. 

The gauge data that is being ingested cans currently be accessed from the apsviz-timeseriesdb.edc.renci.org VM in the following directory:   

/projects/ees/TDS/DataHarvesting/DAILY_HARVESTING/ 

It was generated using the software in the ADCIRC Support Tools (AST) repo, which can be downloaded from:  

https://github.com/RENCI/AST

## Install apsviz-timeseriesdb-ingest

To install apsviz-timeseriesdb-ingest you first need to clone it:

git clone https://github.com/RENCI/apsviz-timeseriesdb-ingest.git

Then change your directory to apsviz-timeseriesdb-ingest/build:

cd apsviz-timeseriesdb-ingest/build

From this directory you can run the build.sh file as follows:

./build.sh latest

After the build has finished edit the createcontainer.sh file, adding the directory path you want to add as a volume:

\#!/bin/bash
\# setup specific to apsviz_timeseriesdb_ingest 
version=$1;

docker run -ti --name apsviz_timeseriesdb_ingest_$version \
  --volume /xxxx/xxxx/xxxx:/data \
  -d apsviz_timeseriesdb_ingest:$version /bin/bash 

The xxxx represent directories in your directory path, so replace them with the actual directory names. The directory you add as a volume should have enough storage space to do the job. After editing createcontainer.sh run it:

createcontainer.sh latest

The value 'latest' in the above commands is the version of the docker image and container you are creating. You can use values other than 'latest', but make sure you use the same values in all the commands.

The next step is to make a network link to apsviz-timeseriesdb_default. This step will enable you to access the apsviz-timeseriesdb database. Before taking this step you have to install apsviz-timeseriesdb. If you have not install apsviz-timeseriesdb go to the following URL for instructions:

https://github.com/RENCI/apsviz-timeseriesdb

To create the network link run the createnetwork.sh file as follows:

./createnetwork.sh latest

At this point things are installed, and you can access the apsviz_timeseriesdb_ingest container shell using the root_shell.sh file as follows:

./root_shell.sh latest
 

## Create list of data files, and ingest them into the database

The first step is to make sure you have created a directory to store the files that are going to be creating. From within the apsviz_timeseriesdb_ingest shell make the following directories:

mkdir -p /data/DataIngesting/DAILY_INGEST

Next, create the files that contain the list of files that are to be ingested into the database. This can be done using the createHarvestFileMeta.py program by running the following commands:

conda run -n apsvizTimeseriesdbIngest python createHarvestFileMeta.py --inputDir /projects/ees/TDS/DataHarvesting/DAILY_HARVESTING/ --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset adcirc_stationdata  
conda run -n apsvizTimeseriesdbIngest python createHarvestFileMeta.py --inputDir /projects/ees/TDS/DataHarvesting/DAILY_HARVESTING/ --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset contrails_stationdata  
conda run -n apsvizTimeseriesdbIngest python createHarvestFileMeta.py --inputDir /projects/ees/TDS/DataHarvesting/DAILY_HARVESTING/ --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset noaa_stationdata  

The information in these files can be ingested into the database using the ingestData.py program, by running the follwing command:  
 
python ingestData.py --inputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask File --inputDataset None

## Create files containing list of stations, and ingest them into the database

The next step is the create files, that contain the guage stations. This can be down using the createIngestStationMeta.py program by running the following commands:

conda run -n apsvizTimeseriesdbIngest python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset adcirc  
conda run -n apsvizTimeseriesdbIngest python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset contrails  
conda run -n apsvizTimeseriesdbIngest python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset noaa  

Before running these commands make sure you have ingested the original NOAA and NCEM gauge data by following the instalation instructions for the apsviz-timeseriesdb repo.

The information in these files can be ingested into the database using the ingestData.py program, by running the follwing command: 

conda run -n apsvizTimeseriesdbIngest python ingestData.py --inputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask Station --inputDataset None

## Create files containing list of sources, and ingest them into the database

Now that the gauge stations have been ingested into the database, the source files can be created using the createIngestSourceMeta.py program. This program uses station data that was ingested in the previous section. It generates a list of source, using the station_id from the station table, and adds information about data sources. To create the source files run the following commands:  

conda run -n apsvizTimeseriesdbIngest python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_TIDAL_namforecast_HSOFS_meta.csv  
conda run -n apsvizTimeseriesdbIngest python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_TIDAL_nowcast_HSOFS_meta.csv  
conda run -n apsvizTimeseriesdbIngest python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_COASTAL_namforecast_HSOFS_meta.csv  
conda run -n apsvizTimeseriesdbIngest python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_COASTAL_nowcast_HSOFS_meta.csv  
conda run -n apsvizTimeseriesdbIngest python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_RIVERS_namforecast_HSOFS_meta.csv  
conda run -n apsvizTimeseriesdbIngest python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile adcirc_stationdata_RIVERS_nowcast_HSOFS_meta.csv  
conda run -n apsvizTimeseriesdbIngest python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile noaa_stationdata_TIDAL_meta.csv  
conda run -n apsvizTimeseriesdbIngest python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile contrails_stationdata_COASTAL_meta.csv  
conda run -n apsvizTimeseriesdbIngest python createIngestSourceMeta.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --outputFile contrails_stationdata_RIVERS_meta.csv  

The information in these files can be ingested into the database using the ingestData.py program, by running the follwing command: 

conda run -n apsvizTimeseriesdbIngest python ingestData.py --inputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask Source --inputDataset None  

## Create files containing gauge data, and ingest them into the database

Now that both the both the station and source data has been ingested into the database, the files containing the gauge data cand be created by using the createIngestData.py program. This program reads in the data files in the DataHarvesting directory, and adds the source_id, from the source table, along the a timemark value along with the source_id, and timestep uniquely identifies each record in the data table. To create the data files run the following commands:  

conda run -n apsvizTimeseriesdbIngest python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset adcirc  
conda run -n apsvizTimeseriesdbIngest python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset contrails  
conda run -n apsvizTimeseriesdbIngest python createIngestData.py --outputDir /projects/ees/TDS/DataIngesting/DAILY_INGEST/ --inputDataset noaa 

The information in these files can be ingested into the database using the ingestData.py program, by running the follwing commands:

conda run -n apsvizTimeseriesdbIngest python ingestData.py --inputDir None --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask Data --inputDataset adcirc  
conda run -n apsvizTimeseriesdbIngest python ingestData.py --inputDir None --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask Data --inputDataset contrails  
conda run -n apsvizTimeseriesdbIngest python ingestData.py --inputDir None --ingestDir /home/DataIngesting/DAILY_INGEST/ --inputTask Data --inputDataset noaa  

## Create view of station, source and data tables 

The final step is the create a view combining the station, source and data tables. This can be down by running the following command:  

conda run -n apsvizTimeseriesdbIngest python ingestData.py --inputDir None --ingestDir None --inputTask View --inputDataset None  

 
