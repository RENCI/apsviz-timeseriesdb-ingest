#!/bin/bash
# setup specific to apsviz_timeseriesdb_ingest 
version=$1;

docker run -ti --name apsviz_timeseriesdb_ingest_$version \
  --volume /xxxx/xxxx/xxxx:/data \
  -d apsviz_timeseriesdb_ingest:$version /bin/bash 
