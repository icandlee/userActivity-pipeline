package com.etl.utils;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

public class CheckpointManager {

    // checkpoint file name
    private static final String CHECKPOINT_FILE_NAME = "checkpoint.txt";

    /**
     * write chunk checkpoint in HDSF file
     * ex) chunkIdx:1 | startRow:100000 | endRow:200000
     * 
     * @param spark          SparkSession 
     * @param chunkIndex     datafrmae chunk index
     * @param checkPointPath checkpoint file path (ex.hdfs://nn:9000/user/hive/checkpoint)
     * @param checkMsg       checkpoint message
     */
    public static void saveCheckpoint(SparkSession spark, String checkPointPath, String checkMsg) {

        try {
            // Get Hadoop FileSystem for file operations
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());   
            // HDFS 경로에 checkpoint txt 파일 작성
            Path path = new Path(checkPointPath + "/" + CHECKPOINT_FILE_NAME); 
            try (OutputStream os = fs.create(path, true); // true = overwrite existing file
                PrintWriter out = new PrintWriter(os)) {
                out.println(checkMsg);
        }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * create formatting check point message
     * -> {chunkIdx}|{startRow}|{endRow}
     * 
     * @param chunkIndex chunk index
     * @param startRow   시작행 idx
     * @param endRow     마지막행 idx
     * @return           check point message
     */
    public static String formatCheckpointMessage(int chunkIndex, long startRow, long endRow) {
        return String.format("%d|%d|%d", chunkIndex, startRow, endRow);
    }

    /**
     * Load check point - 장애 발생으로 인한 복구 지점 확인
     * 
     * @param checkPointPath checkpoint file path (ex.hdfs://nn:9000/user/hive/checkpoint)
     * @return               chunk Index
     */
    public static int loadCheckpoint(SparkSession spark, String checkPointPath) {

        try {
            // Get Hadoop FileSystem for file operations
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());   
            Path path = new Path(checkPointPath + "/" + CHECKPOINT_FILE_NAME); 
        
            // 체크포인트 파일에서 청크 인덱스를 읽는 로직
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                if ((line = br.readLine()) != null) {
                    System.out.println(line);
                    // split with "|"
                    String[] parts = line.split("|");   
                    // get chunkIdx
                    String chunkIdxStr = parts[0];
                    return Integer.parseInt(chunkIdxStr);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // checkpoint 파일 없을 경우 0부터 시작
        return 0; 
    }
    

}