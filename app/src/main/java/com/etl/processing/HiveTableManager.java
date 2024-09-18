package com.etl.processing;

import org.apache.spark.sql.SparkSession;

public class HiveTableManager {

    /**
     * Hive External table(user_activity) 없는 경우 생성
     * The table schema includes columns for event details and is partitioned by year, month, and day.
     * Format : Parquet with Snappy compression.
     * 
     * @param spark       SparkSession
     * @param outputPath  테이블 데이터가 저장되는 HDFS 경로
     * @param tableName   태이블명(user_activity)
     */
    
     public static void createExternalTable(SparkSession spark, String outputPath, String tableName) {
        String createTableQuery = "CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + " (\n" +
                "  event_time_kst TIMESTAMP,\n" +
                "  event_type STRING,\n" +
                "  product_id INT,\n" +
                "  category_id BIGINT,\n" +
                "  category_code STRING,\n" +
                "  brand STRING,\n" +
                "  price DOUBLE,\n" +
                "  user_id INT,\n" +
                "  user_session STRING\n" +
                ") PARTITIONED BY (year INT, month INT, day INT)\n" +
                "STORED AS PARQUET\n" +
                "LOCATION '" + outputPath + "'";

        // Execute the SQL query
        spark.sql(createTableQuery);
        // Hive의 metadata가 실제 파티션 구조와 일치되도록 업데이트
        spark.sql("MSCK REPAIR TABLE " + tableName);
    }
}