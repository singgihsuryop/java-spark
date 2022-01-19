package com.singgihsuryop.spark.library.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class CSVWriter extends SparkWriter
{

    @Override
    public void write(Dataset dataset, String outputPath) {
        dataset
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .option("nullValue", null)
                .csv(outputPath);;
    }
}
