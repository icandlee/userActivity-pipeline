package com.etl.config;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;

public class ConfigLoader {
    private static final Logger logger = Logger.getLogger(ConfigLoader.class);

    /**
     * config.properties 파일 (/main/resources)
     * 
     * @param fileName config 파일명(ex.config.properties)
     * @return Properties - properties 객체; 파일을 로드할 수 없는 경우 null을 반환
     */
    
    public static Properties loadProperties(String fileName) {
        Properties properties = new Properties();
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
            properties.load(input);
        } catch (IOException e) {
            logger.error("Error loading properties file: " + fileName, e);
            return null;
        }
        return properties;
    }
}