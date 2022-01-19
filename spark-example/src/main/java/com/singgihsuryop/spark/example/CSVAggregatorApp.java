package com.singgihsuryop.spark.example;


import com.singgihsuryop.spark.library.reader.CSVReader;
import com.singgihsuryop.spark.library.reader.ParquetReader;
import com.singgihsuryop.spark.library.writer.CSVWriter;
import com.singgihsuryop.spark.library.writer.ParquetWriter;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.spark.sql.Dataset;

import java.util.Date;

/**
 * Hello world!
 *
 */
public class CSVAggregatorApp
{

    public static void main( String[] args )
    {

        CSVReader csvReader = new CSVReader();
        CSVWriter csvWriter = new CSVWriter();


        String inputDir = "./spark-example/src/main/resources/input/csv/";
        String outputDir = "./spark-example/src/main/resources/output/"
                .concat(DateFormatUtils.format(new Date(), "yyyyMMdd_HH-mm-ss"));

        Dataset csvDataset = csvReader.read(inputDir);

        csvDataset =  csvDataset.filter(csvDataset.col("name").equalTo("singgih"));

        csvWriter.write(csvDataset, outputDir);

    }

/*    public Dataset<?> createDataset()
    {
        Seq s = Seq(1639037433, 1639048233, 1639098633, 1639177833);
spark.sparkContext().parallelize(s, 10);
        return dataSet;
    } */



}
