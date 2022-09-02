#!/bin/bash
version=$1;

docker build -t apsviz_timeseriesdb_ingest:$version .
