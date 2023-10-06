package it.unipi.dii.aide.mircv.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import static it.unipi.dii.aide.mircv.utils.Constants.LOG_PATH;

public final class Logger {

    private static String logFileName = LOG_PATH;

    public Logger(String logFileName) {
        Logger.logFileName = logFileName;
    }

    public static void logInfo(String message) {
        log("INFO", message);
    }

    public static void logWarning(String message) {
        log("WARNING", message);
    }

    public static void logError(String message) {
        log("ERROR", message);
    }

    private static void log(String logLevel, String message) {
        String formattedLog = getFormattedLog(logLevel, message);

        try (PrintWriter writer = new PrintWriter(new FileWriter(logFileName, true))) {
            writer.println(formattedLog);
        } catch (IOException e) {
            System.err.println("Failed to write to log file: " + e.getMessage());
        }
    }

    private static String getFormattedLog(String logLevel, String message) {
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formattedDate = dateFormat.format(now);
        return "[" + formattedDate + "] [" + logLevel + "] " + message;
    }
}
