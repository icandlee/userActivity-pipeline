package user;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
//spark
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
//hadoop
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.fs.FileUtil;
//logger
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class UserActivityETL{

    private static final Logger logger =Logger.getLogger(UserActivityETL.class); 

    public static void main(String[] args) {
        logger.setLevel(Level.INFO); // 로그 레벨 설정

        //Check input parameters
        // if (args.length < 2) {
        //     System.err.println("args: UserActivityETL <start_date> <end_date>");
        //     System.exit(1);
        // }
        String startDate ="2019-10-01";
        //String endDate = args[1];      // e.g., "2019-10-31" 

        //load config.properties
        Properties properties = new Properties();
        try(InputStream input = UserActivityETL.class.getClassLoader().getResourceAsStream("config.properties")) {
            properties.load(input);
            if (input == null) {
                System.out.println("unable to find config.properties");
                return;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        //get properties
        String hdfsUrl = properties.getProperty("hdfs.url");
        String hiveMetastoreUrl = properties.getProperty("hive.metastore.url");
        String hiveWarehouseUrl = properties.getProperty("hive.warehouse.url");
        String hiveExternalUrl = properties.getProperty("hive.external.url");
        String hiveTempUrl= properties.getProperty("hive.temp.url");

        //SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Hive")
                .master("local[*]") 
                .config("spark.hadoop.fs.defaultFS", hdfsUrl)  
                .config("hive.metastore.uris", hiveMetastoreUrl)
                .config("spark.sql.warehouse.dir", hiveWarehouseUrl)
                .config("spark.task.maxFailures", "4") // 작업 실패 시 재시도
                .enableHiveSupport()
                .getOrCreate();
        try {
            // Get Hadoop FileSystem for file operations
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

            //check hive connection
            Dataset<Row> tables = spark.sql("SELECT * FROM user_activity LIMIT 3");
            tables.show();

            // read raw file
            String filePath = "file:///home/project/data/2019-Oct.csv";
            Dataset<Row> df = spark.read()
                    .format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")  
                    .load(filePath);

            // Convert event_time from UTC to KST(event_time_kst)
            df = df.withColumn("event_time_kst", from_utc_timestamp(col("event_time"), "Asia/Seoul"));

            // Extract year, month, day based on KST
            df = df.withColumn("year", year(col("event_time_kst")))
            .withColumn("month", month(col("event_time_kst")))
            .withColumn("day", dayofmonth(col("event_time_kst")));

            df.show(1); 

            String tableName = "user_activity";
            String outputPath = hiveExternalUrl + "/useractivity";
            String tempOutputPath = hiveTempUrl + "/useractivity/" + startDate;

            //Write data partitioned by year/month/day
            df.write()
            .mode(SaveMode.Overwrite)
            .partitionBy("year", "month", "day")
            .format("parquet")
            .option("compression", "snappy")
            .save(tempOutputPath); 

            // 최종 테이블로 데이터 이동
            Path srcPath = new Path(tempOutputPath);
            Path dstPath = new Path(outputPath); 

            // if (!fs.exists(dstPath)) {
            //     fs.mkdirs(dstPath);
            // }
            
            boolean success = FileUtil.copy(fs, srcPath, fs, dstPath, true, spark.sparkContext().hadoopConfiguration());
            // check file copy
            if (success) {
                // 임시 경로 삭제
                fs.delete(srcPath, true);
            } else {
                //log
                logger.error("Failed to move files from " + tempOutputPath + " to " + outputPath);
            }

            //Create External table If not exists
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
    
            spark.sql(createTableQuery);
            spark.sql("MSCK REPAIR TABLE " + tableName);

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
