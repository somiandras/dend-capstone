# Udacity Data Engineering Nanodegree - Capstone Project

## Introduction

In the final project of the course my goal is to stich together most of the technologies I learned about in the different modules of the DEND into a coherent data pipeline.

For the basis of the project I chose the famous __NYC taxi trips__ dataset. This is a well known and widely used data source in data analysis and data science projects. Even if it's not a very novel dataset, from a data engineering perspective it is a great candidate for this project for several reasons:

- It is an extremely large dataset, using big data tools is essential
- Updated regularily, great for scheduled pipelines
- Can be merged with several other datasets, eg. geography, weather, etc.

## Goal of the Project

For this project I will assume that all trips included in the source data belong to a single taxi company. The goal of the project is to setup a relatively simple data warehouse on AWS that can provide the management of this imaginative taxi company with timely, high level view and the possibility to drill down and gain deeper insights about the performance of the company.

## Datasets

### 1. NYC Taxi & Limousine Commission (TLC) Trip Record Data

This is the base "transactional" dataset, containing millions of individual taxi trips. The individual files are availabile in a public S3 bucket.

Source: [https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

### 2. NYC TLC Taxi Zone Lookup Table

This is supplementary data for identifying the zones where the individual trips started or ended. Ideal for adding a geographical dimension to the analysis.

Source: [https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv](https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv)

### 3. NOAA Daily Weather Data

This is a separate data source adding weather information to the trip data. I requested the file for this project via website of NOAA. The data seems incomplete in some places, therefore ideal for practicing how to handle missing datapoints in a pipeline.

Source: [https://www.ncdc.noaa.gov/cdo-web/datasets#GHCND](https://www.ncdc.noaa.gov/cdo-web/datasets#GHCND)

## Technologies Used

[WIP]

## Processing Steps

### 1. Gathering Data

### 2. ETL

## How to Run the Project

## Files in the Repository

```bash
.
├── README.md
└── data
```

## Potential Improvements

### Security

## What If...

### ...the data was increased by 100x?

### ...the pipelines would be run on a daily basis by 7 am every day?

### ...the database needed to be accessed by 100+ people?
