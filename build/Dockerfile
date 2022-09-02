##############
# Docker file for the creation of the apsviz-timeseriesdb-ingest docker image.
#
# to create image: docker build -t apsviz_timeseriesdb_ingest:latest .
# to push image:
#       docker tag apsviz_timeseriesdb_ingest:latest containers.renci.org/apsviz_timeseriesdb_ingest:latest
#       docker push containers.renci.org/apsviz_timeseriesdb_ingest:latest
##############
FROM continuumio/miniconda3

# author
MAINTAINER Jim McManus

# extra metadata
LABEL version="1.0"
LABEL description="apsviz_timeseriesdb_ingest image with Dockerfile."

# update sources list, and install basic apps, one per line for better caching
RUN apt-get clean && apt-get --allow-releaseinfo-change update  &&\
    apt-get install -qy vim cron

# update conda and set the download channel
RUN conda update conda && \
    conda config --add channels conda-forge

# add user nru 
RUN useradd -m -d /home/nru -u 1000 nru
RUN chmod 777 /home/nru
USER nru 

# make app directory in nru
RUN mkdir /home/nru/app
WORKDIR /home/nru/app
RUN chmod 777 -R .

# Create the environment, initialize conda in bash config files, and install repos:
COPY environment.yml .
RUN conda env create -f environment.yml

# make d directory for the repos and go there
RUN mkdir /home/nru/repos
WORKDIR /home/nru/repos
RUN chmod 777 -R .

# get the repo
RUN git clone -b container https://github.com/RENCI/apsviz-timeseriesdb-ingest.git

# change to the run directory                        
WORKDIR /home/nru/repos/apsviz-timeseriesdb-ingest/run

# set the python path
ENV PYTHONPATH=/home/nur/repos/apsviz-timeseriesdb-ingest

# tell conda what the shell is
RUN conda init bash
