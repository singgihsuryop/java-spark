package com.singgihsuryop.spark.library.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class SparkWriter {
    public abstract void write(Dataset dataset, String outputPath);
}
