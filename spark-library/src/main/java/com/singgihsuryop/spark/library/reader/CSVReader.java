package com.singgihsuryop.spark.library.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class CSVReader extends SparkReader
{

    public CSVReader(){
        super();
    }

    public CSVReader(SparkSession spark){
        this.spark = spark;
    }

    public Dataset<Row> read(String inputPath)
    {
        Dataset<Row> csv = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(inputPath);

        return csv;
    }

}
