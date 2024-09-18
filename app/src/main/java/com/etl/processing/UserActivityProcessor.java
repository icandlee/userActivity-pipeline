package com.etl.processing;
//spark
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
//package
import com.etl.utils.CheckpointManager; 

public class UserActivityProcessor {

    /**
     * Reads user activity csv data & processing column (UTC -> KST)
     * KST 기준 daily partition 처리
     * Format : Parquet with Snappy compression 처리
     * 데이터를 chunkSize 기준으로 나누어 적재 진행하여 대용량 데이터 처리 대응
     * chunk단위 적재시마다 checkpoint 기록하여 장애시 복구 장치 구현
     * 
     * @param spark       SparkSession
     * @param filePath    csv file(kaggle raw data) path
     * @param outputPath  테이블 데이터가 저장되는 HDFS 경로(ex.hdfs://nn:9000/user/hive/external/useractivity)
     * @param tableName   태이블명(user_activity)
     */ 

    public static void processData(SparkSession spark, String filePath, String outputPath, String checkPointPath, String tableName) {

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(filePath);

        int chunkSize = 100000; // ex.임의로 지정
        long totalRows = df.count();
        long chunks = (totalRows + chunkSize - 1) / chunkSize; // 총 청크 수 계산

        //마지막 처리된 chunk index load & 해당 chun idx 부터 실행
        int lastProcessedChunk = CheckpointManager.loadCheckpoint(spark, checkPointPath);
        for (int i = lastProcessedChunk; i < chunks; i++) { 
            long start = i * chunkSize;
            long end = Math.min(start + chunkSize, totalRows);

            // get chunk dataframe
            Dataset<Row> chunkDF = df.limit((int) end).except(df.limit((int) start));

            // Convert event_time from UTC to KST(event_time_kst)
            // Extract year, month, day based on KST
            chunkDF = chunkDF.withColumn("event_time_kst", functions.from_utc_timestamp(functions.col("event_time"), "Asia/Seoul"))
                    .withColumn("year", functions.year(functions.col("event_time_kst")))
                    .withColumn("month", functions.month(functions.col("event_time_kst")))
                    .withColumn("day", functions.dayofmonth(functions.col("event_time_kst")));

            chunkDF.write()
                    .mode(SaveMode.Append)
                    .partitionBy("year", "month", "day")
                    .format("parquet")
                    .option("compression", "snappy")
                    .save(outputPath);
            
            // writed chunk check point
            String checkMsg = CheckpointManager.formatCheckpointMessage(i, start, end);
            CheckpointManager.saveCheckpoint(spark, checkPointPath, checkMsg);
        }

        //reset checkpoint - 전체 데이터 적재 완료
        String checkMsg = CheckpointManager.formatCheckpointMessage(0, 0, 0);
        CheckpointManager.saveCheckpoint(spark, checkPointPath, checkMsg);
    }

}
