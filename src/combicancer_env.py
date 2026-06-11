#!/usr/bin/env python3
"""Spark session factory for the combicancer OMOP-isation pipeline."""
import os

HERE = os.path.dirname(os.path.abspath(__file__))


def get_spark(app_name="combicancer"):
    """Local Spark session for OMOP building."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
