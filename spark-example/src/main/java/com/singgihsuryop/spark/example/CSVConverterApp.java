package com.singgihsuryop.spark.example;


import com.singgihsuryop.spark.library.reader.CSVReader;
import com.singgihsuryop.spark.library.reader.ParquetReader;
import com.singgihsuryop.spark.library.writer.JSONWriter;
import com.singgihsuryop.spark.library.writer.ParquetWriter;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.spark.sql.Dataset;

import java.util.Date;

/**
 * Hello world!
 *
 */
public class CSVConverterApp
{

    public static void main( String[] args ) {
        toJSON();
    }

    public static void toParquet(){

        CSVReader csvReader = new CSVReader();
        ParquetWriter parquetWriter = new ParquetWriter();


        String inputDir = "./spark-example/src/main/resources/input/csv/";
        String outputDir = "./spark-example/src/main/resources/output/"
                .concat(DateFormatUtils.format(new Date(), "yyyyMMdd_HH-mm-ss"));
        Dataset csvDataset = csvReader.read(inputDir);

        parquetWriter.write(csvDataset, outputDir);

        ParquetReader parquetReader = new ParquetReader();
        Dataset parquetDataset = parquetReader.read(outputDir);
        parquetDataset.show();
        parquetDataset.printSchema();
    }

    public static void toJSON(){

        CSVReader csvReader = new CSVReader();
        JSONWriter jsonWriter = new JSONWriter();


        String inputDir = "./spark-example/src/main/resources/input/csv/";
        String outputDir = "./spark-example/src/main/resources/output/"
                .concat(DateFormatUtils.format(new Date(), "yyyyMMdd_HH-mm-ss"));
        Dataset csvDataset = csvReader.read(inputDir);

        jsonWriter.write(csvDataset, outputDir);

    }

/*    public Dataset<?> createDataset()
    {
        Seq s = Seq(1639037433, 1639048233, 1639098633, 1639177833);
spark.sparkContext().parallelize(s, 10);
        return dataSet;
    } */



}
