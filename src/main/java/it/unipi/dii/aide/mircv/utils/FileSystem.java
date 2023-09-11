package it.unipi.dii.aide.mircv.utils;

import java.io.File;
import java.io.RandomAccessFile;

import static it.unipi.dii.aide.mircv.utils.Constants.*;

public final class FileSystem {

    private FileSystem() {
        throw new UnsupportedOperationException();
    }

    /**
     * function to delete all the file except "stopwords.txt", "collection.tsv", and "msmarco-test2020-queries.tsv"
     * that are in resources.
     */
    public static void file_cleaner() {

        File folder = new File(RES);
        File[] files = folder.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isFile()
                        && !file.getName().equals("stopwords.txt")
                        && !file.getName().equals("collection.tsv")
                        && !file.getName().equals("msmarco-test2020-queries.tsv")) {
                    try {
                        if (file.delete()) {
                            System.out.println("Deleted: " + file.getName());
                        } else {
                            System.err.println("Failed to delete: " + file.getName());
                        }
                    } catch (SecurityException e) {
                        System.err.println("SecurityException: " + e.getMessage());
                    }
                }
            }
        }
    }

    public static void delete_tempFiles() {

        File folder = new File(RES);
        File[] files = folder.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isFile() && file.getName().startsWith("partial_")) {
                    try {
                        if (file.delete()) {
                            System.out.println("Deleted: " + file.getName());
                        } else {
                            System.err.println("Failed to delete: " + file.getName());
                        }
                    } catch (SecurityException e) {
                        System.err.println("SecurityException: " + e.getMessage());
                    }
                }
            }
        }
    }

}
