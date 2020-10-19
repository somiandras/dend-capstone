#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from io import StringIO

import pandas as pd
import boto3

logging.basicConfig(level=logging.INFO)


def split_and_upload():
    """
    Load weather data, filter columns and upload in daily JSON files to S3.
    """
    df = pd.read_csv(
        "data/2319102.csv",
        parse_dates=["DATE"],
        quoting=1,
        usecols=[
            "STATION",
            "NAME",
            "LATITUDE",
            "LONGITUDE",
            "ELEVATION",
            "DATE",
            "AWND",
            "PRCP",
            "SNOW",
            "TMAX",
            "TMIN",
        ],
    )

    s3 = boto3.resource("s3")

    for date, group in df.groupby("DATE"):
        buffer = StringIO()
        group.to_json(buffer, orient="records", lines=True)

        date_str = date.date().isoformat()
        month_str = date_str[:7]
        path = f"weather/{month_str}/weather-{date_str}.json"

        try:
            s3.Object("dend-projects-somi", path).put(Body=buffer.getvalue())
        except Exception as e:
            logging.error(e)
        else:
            logging.info(f"Saved {date_str}")


if __name__ == "__main__":
    split_and_upload()
