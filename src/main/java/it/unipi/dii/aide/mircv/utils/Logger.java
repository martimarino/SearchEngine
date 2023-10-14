package it.unipi.dii.aide.mircv.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import static it.unipi.dii.aide.mircv.utils.Constants.DEBUG_FOLDER;

public final class Logger {

    private final String logFileName;

    public static Logger spimi_logger = new Logger("spimi.log");
    public static Logger merge_logger = new Logger("merge.log");
    public static Logger query_logger = new Logger("query.log");
    public static Logger term_logger = new Logger("0000.log");

    public Logger(String logFileName) {
        this.logFileName = DEBUG_FOLDER + logFileName;
    }

    public void logInfo(String message) {
        log("INFO", message);
    }

    public void logWarning(String message) {
        log("WARNING", message);
    }

    public void logError(String message) {
        log("ERROR", message);
    }

    private void log(String logLevel, String message) {
        String formattedLog = getFormattedLog(logLevel, message);

        try (PrintWriter writer = new PrintWriter(new FileWriter(logFileName, true))) {
            writer.println(formattedLog);
        } catch (IOException e) {
            System.err.println("Failed to write to log file: " + e.getMessage());
        }
    }

    private String getFormattedLog(String logLevel, String message) {
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String formattedDate = dateFormat.format(now);
        return "[" + formattedDate + "] [" + logLevel + "] " + message;
    }
}
