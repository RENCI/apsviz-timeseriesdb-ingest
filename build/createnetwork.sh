#!/bin/bash
# setup specific to apsviz_timeseriesdb_ingest 
version=$1;

docker network connect apsviz-timeseriesdb_default apsviz_timeseriesdb_ingest_$version
