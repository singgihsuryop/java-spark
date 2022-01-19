package com.singgihsuryop.spark.library.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class JSONReader extends SparkReader
{
    public JSONReader(){
        super();
    }
    public JSONReader(SparkSession spark){
        this.spark = spark;
    }

    public Dataset<Row> read(String inputPath)
    {
        Dataset<Row> dataSet = spark.read().json(inputPath);
        return dataSet;
    }

}
