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

public class UserActivityETL{

    public static void main(String[] args) {
        
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
    
        //SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("CSV to Hive")
                .master("local[*]") 
                .config("spark.hadoop.fs.defaultFS", hdfsUrl)  
                .config("hive.metastore.uris", hiveMetastoreUrl)
                .config("spark.sql.warehouse.dir", hiveWarehouseUrl)
                .enableHiveSupport()
                .getOrCreate();

        //check hive connection
        Dataset<Row> tables = spark.sql("SHOW TABLES");
        tables.show(); 

        // read raw file
        String filePath = "file:///home/project/data/2019-Oct.csv";
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")  // 스키마 자동 유추
                .load(filePath);

        // Convert event_time from UTC to KST(event_time_kst)
        df = df.withColumn("event_time_kst", from_utc_timestamp(col("event_time"), "Asia/Seoul"));

        // Extract year, month, day based on KST
        df = df.withColumn("year", year(col("event_time_kst")))
        .withColumn("month", month(col("event_time_kst")))
        .withColumn("day", dayofmonth(col("event_time_kst")));

        df.show(1); 

        // close Spark session
        spark.stop();
    } 
    
}
