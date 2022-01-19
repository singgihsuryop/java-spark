package com.singgihsuryop.spark.library.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class SparkReader {
    protected SparkSession spark;
    public SparkReader(){
        this.spark=SparkSession.builder().master("local[*]").getOrCreate();
    }
    public abstract Dataset<Row> read(String inputPath);
}
