package com.singgihsuryop.spark.library.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

public class JSONWriter extends SparkWriter
{

    public void write(Dataset dataset, String outputPath) {

        dataset.write()
            .mode(SaveMode.Overwrite)
            .parquet(outputPath);

    }

}
