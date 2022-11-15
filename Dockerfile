# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

##############
# Docker file for the creation of the apsviz-timeseriesdb-ingest docker image.
#
# to create image: docker build -t apsviz_timeseriesdb_ingest:latest .
# to push image:
#       docker tag apsviz_timeseriesdb_ingest:latest containers.renci.org/apsviz_timeseriesdb_ingest:latest
#       docker push containers.renci.org/apsviz_timeseriesdb_ingest:latest
##############
FROM continuumio/miniconda3 as build

# author
MAINTAINER Jim McManus

# extra metadata
LABEL version="v0.0.1"
LABEL description="apsviz_timeseriesdb_ingest image with Dockerfile."

# update conda
RUN conda update conda

# Create the virtual environment
COPY build/environment.yml .
RUN conda env create -f environment.yml

# install conda pack to compress this stage
RUN conda install -c conda-forge conda-pack

# conpress the virtual environment
RUN conda-pack -n apsvizTimeseriesdbIngest -o /tmp/env.tar && \
  mkdir /venv && cd /venv && tar xf /tmp/env.tar && \
  rm /tmp/env.tar

# fix up the paths
RUN /venv/bin/conda-unpack

##############
# stage 2: create a python implementation using the stage 1 virtual environment
##############
FROM python:3.9-slim

# update apt-get packages
RUN apt-get update

# install basic apps
RUN apt-get install -qy vim cron

# clear out the apt cache
RUN apt-get clean

# add user nru and switch to it
RUN useradd --create-home -u 1000 nru
USER nru

# make app directory in nru
WORKDIR /home/nru

# Copy /venv from the previous stage:
COPY --from=build /venv /venv

# make the virtual environment active
ENV VIRTUAL_ENV /venv
ENV PATH /venv/bin:$PATH

# copy python and bin files to container
COPY run/*.py ./
COPY run/*.csv ./
COPY run/env .env
COPY stations/*.csv ./stations/

# set the python path
ENV PYTHONPATH=/home/nru

