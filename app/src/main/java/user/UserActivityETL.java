package user;
import static org.apache.spark.sql.functions.e;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;
//spark
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

        // close Spark session
        spark.stop();
    } 
    
}
