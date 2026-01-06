# Base image with Python, R, and Julia pre-installed (Data Science Stack)
FROM jupyter/pyspark-notebook:latest

USER root

# Install system dependencies for GIS and Health data (PySUS needs these)
RUN apt-get update && apt-get install -y \
    libgdal-dev \
    g++ \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

# Install specialized libraries
# PySUS: DATASUS data extraction
# PySAL: Spatial Analysis
# MLflow: MLOps tracking
# Pandera: Data Validation
# DuckDB: Fast data processing
RUN pip install --no-cache-dir \
    pysus \
    geopandas \
    pysal \
    mlflow \
    pandera \
    duckdb \
    xgboost \
    h3 \
    python-dotenv \
    'pyarrow>=22.0.*' \
    geobr

# Set the working directory
WORKDIR /home/jovyan/work