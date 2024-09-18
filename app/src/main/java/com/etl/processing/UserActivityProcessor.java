package com.etl.processing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class UserActivityProcessor {

    /**
     * Reads user activity csv data & processing column (UTC -> KST)
     * KST 기준 daily partition 처리
     * Format : Parquet with Snappy compression 처리
     * 
     * @param spark       SparkSession
     * @param filePath    csv file(kaggle raw data) path
     * @param outputPath  테이블 데이터가 저장되는 HDFS 경로
     * @param tableName   태이블명(user_activity)
     */ 
    
    public static void processFile(SparkSession spark, String filePath, String outputPath, String tableName) {

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filePath);

        // Convert event_time from UTC to KST(event_time_kst)
        // Extract year, month, day based on KST
        df = df.withColumn("event_time_kst", functions.from_utc_timestamp(functions.col("event_time"), "Asia/Seoul"))
                .withColumn("year", functions.year(functions.col("event_time_kst")))
                .withColumn("month", functions.month(functions.col("event_time_kst")))
                .withColumn("day", functions.dayofmonth(functions.col("event_time_kst")));

        // Dataset<Row> partitionedDf = df.coalesce(chunkSize);

        df.write()
                .mode(SaveMode.Append)
                .partitionBy("year", "month", "day")
                .format("parquet")
                .option("compression", "snappy")
                .save(outputPath);
    }
}