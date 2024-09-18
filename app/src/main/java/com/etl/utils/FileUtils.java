package com.etl.utils;

import java.io.File;

public class FileUtils {

    public static int getChunkSize(String filePath) {
        File file = new File(filePath);

        if (!file.exists() || !file.isFile()) {
            throw new IllegalArgumentException("File does not exist or is not a valid file: " + filePath);
        }

        long fileSize = file.length();
        final long threshold = 500 * 1024 * 1024;

        return fileSize < threshold ? 1 : (int) Math.ceil(fileSize / (double) threshold);
    }
}