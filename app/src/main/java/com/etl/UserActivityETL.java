package com.etl;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
//spark
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
//logger
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import java.io.File; 
//class 
import com.etl.config.ConfigLoader;
import com.etl.processing.UserActivityProcessor; 

/**
 * UserActivityETL 클래스 : user activity 데이터를 처리하고 HIVE 테이블에 적재하는 ETL 작업
 */
public class UserActivityETL{

    private static final Logger logger =Logger.getLogger(UserActivityETL.class); 

    /**
     * ETL 작업을 수행
     * @param args 첫번째 인수 : 처리할 데이터의 년-월 (예: "2019-10")
     */ 

    public static void main(String[] args) {
        logger.setLevel(Level.INFO); // log level

        //저장할 데이터의 년-월 (ex.2019-10)
        String targetDate = args[0];
        //load config
        Properties properties = ConfigLoader.loadProperties("config.properties"); 
        
        //get file path
        String filePath = "file://" + properties.getProperty("userActivity.file.path") + "/" + targetDate + ".csv";
        //String iputFilePath = "file://" + filePath;
        String outputPath =  properties.getProperty("hive.external.url") + "/useractivity";
        String tableName = "user_activity";

        //SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Hive")
                .master("local[*]") 
                .config("spark.hadoop.fs.defaultFS", properties.getProperty("hdfs.url"))
                .config("hive.metastore.uris", properties.getProperty("hive.metastore.url"))
                .config("spark.sql.warehouse.dir", properties.getProperty("hive.warehouse.url"))
                .config("spark.task.maxFailures", "4") // 작업 실패 시 재시도
                .enableHiveSupport()
                .getOrCreate();

        try {
            UserActivityProcessor.processFile(spark, filePath, outputPath, tableName);
        } catch (Exception e) {
            //log
            logger.error("Error occurred during batch job execution: ",e);
            System.exit(1);
        } finally {
            // close Spark session
            spark.stop();
            logger.info("spark job finished.");
        }
    }

}
