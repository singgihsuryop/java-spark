package com.singgihsuryop.spark.library.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class ORCWriter extends SparkWriter
{

    public void write(Dataset dataset, String outputPath) {
        dataset.write()
                .mode(SaveMode.Overwrite)
                .option("parquet.bloom.filter.enabled#favorite_color", "true")
                .option("parquet.bloom.filter.expected.ndv#favorite_color", "1000000")
                .option("parquet.enable.dictionary", "true")
                .option("parquet.page.write-checksum.enabled", "false")
                .orc(outputPath);
    }

}
