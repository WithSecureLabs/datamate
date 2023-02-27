# Python base image
FROM python:3.10-slim

# Install Python dependencies
RUN pip install pandas
RUN pip install dask[complete]
RUN pip install pyarrow

# Create the folder for the script
RUN mkdir -p /converter
RUN apt-get update && apt-get -y install nano

# Copy script and config file into the image
COPY dask_json_to_parquet.py /converter/
COPY processing_config.json /converter

#Set working directory to the directory of the script
WORKDIR /converter/