#!/bin/bash
version=$1;

docker exec -it apsviz_timeseriesdb_ingest_$version bash
