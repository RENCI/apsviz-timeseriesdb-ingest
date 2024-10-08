# apsviz-timeseriesdb-ingest 
The software, in this repo, is used to ingest observation station data (tidal and river gauges, buoy) from NOAA, NCEM, and NDBC, as well as ADCIRC model data georeferenced to those observation stations. 

The Python scripts used to process the data are:

* prepare4Ingest.py - This script manages the ingestion of station (drf_gauge_station) and source data (drf_gauge_source).
* Scripts to Ingest Observation Data
  * createIngestObsSourceMeta.py - This script creates the source meta data, for observation sources.
  * runObsIngest.py - This script manages the ingestion of station observation data.
  * ingestObsTasks.py - This script has functions that interact with the DB to ingest observation data.
  * createHarvestObsFileMeta.py - This script creates meta-data about the observation harvest data files to be ingested. This meta-data is used to manage the ingest process.
  * createIngestObsData.py - This script creates the data files, from the original observation harvest data files, that are ingested into the drf_gauge_data table. It adds a source ID, and timemark to the original data.
  * createRetainObsStationFileMeta.py - This script creates meta-data files, that are ingested into the drf_retain_obs_station_file_meta table. The meta-data in that table are used to track the ingestion of the obs stations meta-files, which is used in to display station location in the ApsViZ front end. 
* Scripts to Ingest Model Data
  * createIngestModelSourceMeta.py - This script creates the source meta data, for model sources.
  _runModelIngest.py - This script manages the ingestion of station model data.
  * ingestModelTasks.py - This script has functions that interact with the DB to ingest model data.
  * createHarvestModelFileMeta.py - This script creates meta-data about the model harvest data files to be ingested. This meta-data is used to manage the ingest process.
  * createIngestModelData.py - This script creates the data files, from the original model harvest data files, that are ingested into the drf_model_data table. It adds a source ID, and timemark to the original data.
  * createApsVizStationFileMeta.py - This script creates meta-data files, that are ingested into the drf_apsviz_station_file_meta table. The meta-data in that table are used to track the ingestion of the model stations meta-files, which are used in to display station location in the ApsViZ front end. 
  * createIngestApsVizStationData.py - This script creates the ApsViz Station data, from the harvest model station meta files. It also extracts observation stations available for that time period, form the drf_retain_obs_station_file_meta table, and includes them.
  * getDashboardMeta.py - This script interacts with variables from the ASGS_Mon_config_item table, in the asgs_dashboard DB, that are used to add ADCIRC model source automatically.

# Install apsviz-timeseriesdb-ingest

To begin ingesting data, you first need to install the apsviz-timeseriesdb repo which can bet downloaded from:

https://github.com/RENCI/apsviz-timeseriesdb

Follow the installation instructions for that repo. It creates and PostgreSQL database, that serves data using a Django Rest Framework (DRF) app.

The gauge data that is being ingested can be accessed on Sterling using k8s the directory path is:

/data/ast-run-harvester/

It was generated using the software in the ADCIRC Support Tools (AST) repo, which can be downloaded from:

https://github.com/RENCI/AST

## Deploying To The Sterling Kubernetes Cluster

Deploying on the Sterling k8s  cluster is done using GitHub Actions which pushes the docker image to the RENCI Harbor docker repository. A YAML file, with Kubernetes configurations, is then used to deploy as a pod on Sterling.

## Installing on your local machine

To install apsviz-timeseriesdb-ingest you first need to clone it:

git clone https://github.com/RENCI/apsviz-timeseriesdb-ingest.git

Next edit the run/createEnvVars.sh file adding a password to the line:

APSVIZ_GAUGES_PASSWORD=xxxxxx

where you change xxxxxx to the password that is used to access the database in apsviz-timeseriesdb. 

This file contains the env variables to be able to access the PostgreSQL database.

At this point change your directory to apsviz-timeseriesdb-ingest/build:

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

he value 'latest' in the above commands is the version of the docker image and container you are creating. You can use values other than 'latest', but make sure you use the same values in all the commands.

The next step is to make a network link to apsviz-timeseriesdb_default. This step will enable you to access the apsviz-timeseriesdb database. Before taking this step you have to install apsviz-timeseriesdb. If you have not install apsviz-timeseriesdb go to the following URL for instructions:

https://github.com/RENCI/apsviz-timeseriesdb

To create the network link run the createnetwork.sh file as follows:

./createnetwork.sh latest

At this point things are installed, and you can access the apsviz_timeseriesdb_ingest container shell using the root_shell.sh file as follows:

./root_shell.sh latest

However, before doing that copy the createEnvVars.sh, you edited, to the container using the following command:

docker cp ../run/createEnvVars.sh apsviz_timeseriesdb_ingest_latest:/home/nru/

Then when you access the container run the following command to create the env variables to access the PostgreSQL database:

source ./createEnvVars.sh

The env variables are already set on the Sterling Kubernetes cluster, so the above commands are not required.

Next from within the apsviz_timeseriesdb_ingest shell make the following directories:

mkdir -p /data/ast-run-harvester
mkdir -l /data/ast-run-ingester

This does not have to be down when running on the Sterling Kubernetes cluster.

If you are running on your local machine make sure you have downloaded the harvest files from the directory:

/data/ast-run-harvester/

on Sterling

On Sterling, using k8s, check if this directory exits:

/data/ast-run-harvester/
 
# Ingest Data into Database

## Ingest The Short Way

### Prepare Ingest

To ingest the station data and source data run the command below in the /home/nru directory:

python prepare4Ingest.py --ingestDir /data/ast-run-ingester/ --inputTask SequenceIngest

### Ingest data

To ingest the gauge data run the command below in the /home/nru directory:

python runObsIngest.py --harvestDir /data/ast-run-harvester/ --ingestDir /data/ast-run-ingester/ --inputTask SequenceIngest

To ingest ADCIRC model data, for a specific modelRunID, run the command below:

python runModelIngest.py --harvestDir /data/ast-run-harvester/ --ingestDir /data/ast-run-ingester/ --inputTask SequenceIngest --modelRunID xxxx-dddddddddd-mmmmmmmmmmm

where xxxx is the instance ID, dddddddddd is the start time of the model run, and mmmmmmmmmmm is the model run type, such as gfsforecast. Combined they form the modelRunID, xxxx-dddddddddd-mmmmmmmmmmm, as this example 4358-2023042312-gfsforecast shows. 

## Ingest The Long Way

### Create Obs View

To create a view combining the drf_gauge_station, drf_gauge_source, and drf_gauge_data tables run the following command:

python prepare4Ingest.py --inputTask createObsView

This will create a view (drf_gauge_station_source_data) that is accessible through the Django REST Framework API:

http://xxxx.xxxx.xxx/api/gauge_station_source_data/

where xxxx.xxxx.xxx is your server.

### Create Model View

To create a view combining the drf_gauge_station, drf_model_source, drf_model_instance, and drf_model_data tables run the following command:

python prepare4Ingest.py --inputTask createModelView

This will create a view (drf_model_station_source_data) that is accessible through the Django REST Framework API:

http://xxxx.xxxx.xxx/api/model_station_source_data/

where xxxx.xxxx.xxx is your server.

### Ingest Station Data 

If running the commands below on Sterling, using k8s, replace the directory paths with the /data/ast-run-harvester/, and /data/ast-run-ingester/ directory paths, where appropriate.

To ingest the station meta data run the command below in the /home/nru directory:

python prepare4Ingest.py --ingestDir /data/ast-run-ingester/ --inputTask IngestStations

This will ingest the station data in the stations directory into the drf_gauge_station table in the database.

### Create and Ingest Source Data

To ingest the source data, first ingest the source meta using the following command:

python prepare4Ingest.py --inputTask ingestSourceMeta

This will ingest meta data, containing information on the data sources, into the drf_source_meta table. This meta data will be used as argparse input when running prepare4Ingest.py, runObsIngest.py and runModelIngest.py. 

The next step is to ingest the source data into the drf_gauge_source table. To do this run the following command:

python prepare4Ingest.py --ingestDir /data/ast-run-ingester/ --inputTask ingestSourceData

This will create Source data files in /data/ast-run-ingester/ and then ingest them into the drf_gauge_source table in the database.

### Create and Ingest Harvest File Meta Data

To create and ingest the harvest file meta data, for observation data, run the following command:

python runObsIngest.py --harvestDir /data/ast-run-harvester/ --ingestDir /data/ast-run-ingester/ --inputTask ingestHarvestDataFileMeta

This will create Harvest meta data files in /data/ast-run-ingester and then ingest them into the drf_harvest_data_file_meta  table in the database.

To create and ingest the harvest file meta data, for ADCIRC model data, run the following command:

python runModelIngest.py --harvestDir /data/ast-run-harvester/ --ingestDir /data/ast-run-ingester/ --inputTask ingestHarvestDataFileMeta --modelRunID xxxx-dddddddddd-mmmmmmmmmmm 

where xxxx is the instance ID, dddddddddd is the start time of the model run, and mmmmmmmmmmm is the model run type, such as gfsforecast. Combined they form the modelRunID, xxxx-dddddddddd-mmmmmmmmmmm, as this example 4358-2023042312-gfsforecast shows.

### Create and Ingest ApsViz Station Data:

The ApsViz station data is used by the ApsViz interface to display station that have data for a specific model run, so this station data is not the same station data in the drf_gauge_station table. The ApsViz station data is ingested into the drf_apsviz_station table, which contains additional columns, including the modelRunID and timemark, which the ApsViz interface uses. To create this data from the original harvest meta files, and ingest it into the drf_apsviz_station table run the following command: 

python runModelIngest.py --ingestDir /data/ast-run-ingester/ --modelRunID xxxx-dddddddddd-mmmmmmmmmmm --inputTask runApsVizStationCreateIngest

Where xxxx-dddddddddd-mmmmmmmmmmm is the same as described above.

### Create and Ingest Data Files

#### To create and ingest the observation data files first run the command:

python runObsIngest.py --ingestDir /data/ast-run-ingester/ --inputTask DataCreate

This will create data files in /data/ast-run-ingester/.

The next step is to ingest the files by running the following command:

python runObsIngest.py --ingestDir /data/ast-run-ingester/ --inputTask DataIngest

python runObsIngest.py --ingestDir /data/ast-run-ingester/ --inputTask runRetainObsStationCreateIngest

This will ingest the data files, created in the above command, into the drf_gauge_data table in the database. 

#### To create and ingest the ADCIRC model data files first run the command:

python runModelIngest.py --ingestDir /data/ast-run-ingester/  --modelRunID xxxx-dddddddddd-mmmmmmmmmmm --inputTask DataCreate

This will create data files in /data/ast-run-ingester/.

The next step is to ingest the files by running the following command:

python runModelIngest.py --ingestDir /data/ast-run-ingester/  --modelRunID xxxx-dddddddddd-mmmmmmmmmmm --inputTask DataIngest

This will ingest the data files, created in the above command, into the drf_gauge_obs_data table in the database.

### Add New Observation Source

To add a new source, first create the source meta, and ingest it into the drf_source_meta table by running the following command:

python ingestObsTasks.py --inputDataSource xxxxx_xxx --inputSourceName xxxxxx --inputSourceArchive xxxxxx --inputSourceVariable xxxxx_xxxxx --inputFilenamePrefix xxxxx_stationdata_xxxx_xxxxxx --inputLocationType xxxxxx --inputUnits x --inputTask ingestSourceMeta

where: 
  * --inputDataSource xxxxx_xxx is the data source such as gfsforecast_ec95d, 
  * --inputSourceName xxxxxx is the source name such as adcirc, 
  * --inputSourceArchive xxxxxx is the source archive such as renci, 
  * --inputSourceVariable xxxxx_xxxxx is the source variable name such as water_level, 
  * --inputFilenamePrefix xxxxx_stationdata_xxxx_xxxxxx is the input file name prefix such as adcirc_stationdata_RENCI_NAMFORECAST_EC95D_FORECAST, 
  * --inputLocationType xxxxxx is the location type such as tidal, and 
  * --inputUnits x is the variables units such as m for meters.

In the next step create the source data files that will be ingested into the drf_gauge_source table by running the following command:

python createIngestObsSourceMeta.py --ingestDir /xxx/xxx-xxxxx-xxxx/ --inputDataSource xxxxx_xxx --inputSourceName xxxxx --inputSourceArchive xxxxx --inputUnits x --inputLocationType xxxxx 

where:
  * --ingestDir /xxx/xxx-xxxxx-xxxx/ is the ingest directory such as /data/ast-run-ingester/, 
  * --inputDataSource xxxxx_xxx is the data source such as gfsforecast_ec95d, 
  * --inputSourceName xxxxx is the source name such as adcirc, 
  * --inputSourceArchive xxxxx is the source archive such as renci, 
  * --inputLocationType xxxxx is the location type such as tidal, and 
  * --inputUnits x is the variables units such a m for meters.

Finally run the following command to ingest that data into the drf_gauge_source table in the database:

python ingestObsTasks.py --ingestDir /xxx/xxx-xxxxx-xxxx/ --inputTask ingestSourceData

where the --ingestDir /xxx/xxxxxxx/xxxxxxxx/ is the ingest directory such as /data/ast-run-ingester/

The source is now ready to be used in ingesting data.
