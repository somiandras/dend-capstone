# Udacity Data Engineering Nanodegree - Capstone Project

## Introduction

In the final project of the course my goal is to stich together most of the technologies I learned about in the different modules of the DEND into a coherent data pipeline.

For the basis of the project I chose the famous __NYC taxi trips__ dataset. This is a well known and widely used data source in data analysis and data science projects. Even if it's not a very novel dataset, from a data engineering perspective it is a great candidate for this project for several reasons:

- It is an extremely large dataset, using big data tools is essential
- Updated regularily, great for scheduled pipelines
- Can be merged with several other datasets, eg. geography, weather, etc.

## Goal of the Project

For the project I assumed that all trips included in the source data belong to a single taxi company. The goal of the project is to setup a data warehouse on AWS that can provide the management of this imaginative taxi company with high level view and the possibility to drill down and gain deeper insights about the performance of the company.

## Datasets

### 1. NYC Taxi & Limousine Commission (TLC) Trip Record Data

Source: [https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

This is the base "transactional" dataset, containing millions of individual taxi trips. The individual files are availabile in a public S3 bucket.

### 2. NYC TLC Taxi Zone Lookup Table

Source: [https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv](https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv)

This is supplementary data for identifying the zones where the individual trips started or ended. Ideal for adding a geographical dimension to the analysis.

### 3. NOAA Daily Weather Data

Source: [https://www.ncdc.noaa.gov/cdo-web/datasets#GHCND](https://www.ncdc.noaa.gov/cdo-web/datasets#GHCND)

This is a separate data source which adds weather information to the trip data. I requested the file for this project via website of NOAA.

## Technologies/Services Used

- Airflow
- S3
- Amazon Redshift

## Processing Steps

### 1. Preprocessing

The original csv file from NOAA contained daily observations from 134 weather stations in New York, New Jersey and Connecticut, from 2017-01-01 to 2020-07-31.

In order to simulate loading from different formats and to preprocess the datafile, I filtered the columns, split up the csv file into daily JSON files, and uploaded them to the S3 bucket in the following format: `s3://dend-projects-somi/weather/<YEAR-MONTH>/weather-<DATE>.json`.

### 2. Setting up Redshift

### 3. ETL

#### 3.1 Staging

#### 3.2 Final Load

#### 3.3 Daily Runs

## How to Run the Project

## Files in the Repository

```bash
.
├── README.md
└── data
```

## Potential Improvements

### Security

## What If's

### Whaat if the data was increased by 100x

### Whaat if the pipelines would be run on a daily basis by 7 am every day

### Whaat if the database needed to be accessed by 100+ people
