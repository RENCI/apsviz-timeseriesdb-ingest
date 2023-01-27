# apsviz-timeseriesdb-ingest 
The software, in this repo, is used to ingest gauge data from NOAA, NCEM, NDBC, and ADICIRC. To begin ingesting data, you first need to install the apsviz-timeseriesdb repo which can bet downloaded from:  

https://github.com/RENCI/apsviz-timeseriesdb  

Follow the installation instructions for that repo. It creates and postgesql database, that serves data using a Django Rest Framework (DRF) app. 

The gauge data that is being ingested cans currently be accessed from the apsviz-timeseriesdb.edc.renci.org VM in the following directory:   

/projects/ees/TDS/DataHarvesting/DAILY_HARVESTING/ 

On Sterling using k8s the directory path is:

/data/ast-run-harvester/

It was generated using the software in the ADCIRC Support Tools (AST) repo, which can be downloaded from:  

https://github.com/RENCI/AST

## Install apsviz-timeseriesdb-ingest

To install apsviz-timeseriesdb-ingest you first need to clone it:

git clone https://github.com/RENCI/apsviz-timeseriesdb-ingest.git

Next edit the run/createEnvVars.sh file adding a password to the line:

SQL_PASSWORD=xxxxxx

where you change xxxxxx to the password that is used to access the database in apsviz-timeseriesdb. 

Then change your directory to apsviz-timeseriesdb-ingest/build:

cd apsviz-timeseriesdb-ingest/build

From this directory you can run the build.sh file as follows:

./build.sh latest

After the build has finished edit the createcontainer.sh file, adding the directory path you want to add as a volume:

\#!/bin/bash  
\# setup specific to apsviz_timeseriesdb_ingest  
version=$1; 

docker run -ti --name apsviz_timeseriesdb_ingest_$version \\  
  --volume /xxxx/xxxx/xxxx:/data \\  
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

From within the apsviz_timeseriesdb_ingest shell make the following directories:

mkdir -p /data/DataIngesting/DAILY_INGEST

This does not have to be down when running on k8s.

If you are running on you local machine also make sure you have downloaded the harvest files from the directory:

/projects/ees/TDS/DataHarvesting/DAILY_HARVESTING/

on the apsviz-timeseriesdb.edc.renci.org VM, to the same location that you added as a volume when create the container, so that when you are in the container the following directory exits:

/data/DataIngesting/DAILY_HARVESTING/

On Sterling, using k8s, check if this directory exits:

/data/ast-run-harvester/
 
## Ingest Data into Database

### Ingest The Short Way

#### Prepare Ingest

To ingest the station data and source data run the command below in the /home/nru directory:

python prepare4Ingest.py --ingestDir /data/DataIngesting/DAILY_INGEST/ --inputTask SequenceIngest 

On Sterling, using k8s, the command would be:

python prepare4Ingest.py --ingestDir /data/ast-run-ingester/ --inputTask SequenceIngest

#### Ingest data

To ingest the gauge data run the command below in the /home/nru directory:

python runIngest.py --harvestDir /data/DataHarvesting/DAILY_HARVESTING/ --ingestDir /data/DataIngesting/DAILY_INGEST/ --inputTask SequenceIngest

On Sterling, using k8s, the command would be:

python runIngest.py --harvestDir /data/ast-run-harvester/ --ingestDir /data/ast-run-ingester/ --inputTask SequenceIngest

### Ingest The Long Way

#### Create View

To create a view combining the drf_gauge_station, drf_gauge_source, and drf_gauge_data tables run the following command:

python prepare4Ingest.py --inputTask View

This will create a view (drf_gauge_station_source_data) that is accessible through the Django REST Framework API:

http://xxxx.xxxx.xxx/api/gauge_station_source_data/

where xxxx.xxxx.xxx is your server.

#### Ingest Station Data 

If running the commands below on Sterling, using k8s, replace the directory paths with the /data/ast-run-harvester/, and /data/ast-run-ingester/ directory paths, where appropriate.

To ingest the station meta data run the command below in the /home/nru directory:

python prepare4Ingest.py --ingestDir /data/DataIngesting/DAILY_INGEST/ --inputTask IngestStations

This will ingest the station data in the stations directory into the drf_gauge_station table in the database.

#### Create and Ingest Source Data

To ingest the source data, first run the source_meta.bin file:

python prepare4Ingest.py --inputTask IngestSourceMeta

This will ingest meta data on the data sources that will be used as argparse input when running runIngest.py. The next step is to ingest the source data into the drf_gauge_source table. To do this run the following command:

python prepare4Ingest.py --ingestDir /data/DataIngesting/DAILY_INGEST/ --inputTask IngestSource

This will create Source data files in /data/DataIngesting/DAILY_INGEST and then ingest them into the drf_gauge_source table in the database.

#### Create and Ingest Harvest File Meta Data

To create and ingest the harvest file meta data run the following command:

python runIngest.py --harvestDir /data/DataHarvesting/DAILY_HARVESTING/ --ingestDir /data/DataIngesting/DAILY_INGEST/ --inputTask File 

This will create Harvest meta data files in /data/DataIngesting/DAILY_INGEST and then ingest them into the drf_harvest_data_file_meta  table in the database.

#### Create and Ingest Data Files

To create and ingest the data files first run the command:

python runIngest.py --ingestDir /data/DataIngesting/DAILY_INGEST/ --inputTask DataCreate

This will create data files in /data/DataIngesting/DAILY_INGEST/.

The next step is the ingest them by running the following command:

python runIngest.py --ingestDir /data/DataIngesting/DAILY_INGEST/ --inputTask DataIngest

This will ingest the data files, created in the above command, into the drf_gauge_data table in the database. 

#### Add New Source

To add a new source, first create the source meta, and ingest it into the drf_source_meta table by running the following command:

python ingestTasks.py --inputDataSource xxxxx_xxx --inputSourceName xxxxxx --inputSourceArchive xxxxxx --inputLocationType xxxxxx --inputTask Source_meta

where the --inputDataSource xxxxx_xxx is the data source such as namforecast_ec95d, the --inputSourceName xxxxxx is the source name such as adcirc, the --inputSourceArchive xxxxxx is the source archive such as renci, and the --inputLocationType xxxxxx is the location type such as tidal.

In the next step create the source data files that will be ingested into the drf_gauge_source table by running the following command:

python createIngestSourceMeta.py --ingestDir /data/DataIngesting/DAILY_INGEST/ --outputFile nnnn_stationdata_aaaaaa_llllllll_dddddddddd_meta.csv

where nnnn is the source name (e.g. adcirc), aaaaaa is the source archive (e.g. renci), llllllll is the location type (e.g. tidal), and dddddddddd is the data source (e.g. namforecast_ec95d).

Finally run the following command to ingest that data into the drf_gauge_source table in the database:

python ingestTasks.py --inputTask Data --inputDataSource ddddddddddd--inputSourceName nnnn --inputSourceArchive aaaaaa is the source archive i(e.g. renci).

where dddddddddd is the data source (e.g. namforecast_ec95d), nnnn is the source name (e.g. adcirc), and aaaaaa is the source archive (e.g. renci).

The source is now ready to be used in ingesting data.
