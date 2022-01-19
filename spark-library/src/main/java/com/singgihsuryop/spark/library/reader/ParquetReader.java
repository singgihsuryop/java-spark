package com.singgihsuryop.spark.library.reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetReader extends SparkReader
{
    public ParquetReader(){
        super();
    }
    public ParquetReader(SparkSession spark){
        this.spark = spark;
    }

    public Dataset<Row> read(String inputPath)
    {
        Dataset<Row> dataSet = spark.read()
            .option("mergeSchema", "true")
            .parquet(inputPath);

        return dataSet;
    }

}
