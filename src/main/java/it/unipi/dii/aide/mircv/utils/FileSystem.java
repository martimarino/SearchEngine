package it.unipi.dii.aide.mircv.utils;

import java.io.File;

import static it.unipi.dii.aide.mircv.utils.Constants.*;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public final class FileSystem {

    private FileSystem() {
        throw new UnsupportedOperationException();
    }

    /**
     * function to delete all the file except "stopwords.txt", "collection.tsv", and "msmarco-test2020-queries.tsv"
     * that are in resources.
     */
    public static void file_cleaner() {

        try {
            File partial_folder = new File(PARTIAL_FOLDER);
            FileUtils.cleanDirectory(partial_folder);
            File merged_folder = new File(MERGED_FOLDER);
            FileUtils.cleanDirectory(merged_folder);

            File flags = new File(FLAGS_FILE);
            if(flags.exists())
                FileUtils.delete(flags);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void delete_tempFiles() {

        File partial_directory = new File(PARTIAL_FOLDER);
        try {
            FileUtils.cleanDirectory(partial_directory);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void delete_mergedFiles() {

        File dict = new File(DICTIONARY_FILE);
        File docid = new File(DOCID_FILE);
        File termfreq = new File(TERMFREQ_FILE);
        if(dict.exists() && docid.exists() && termfreq.exists()) {
            try {
                FileUtils.delete(dict);
                FileUtils.delete(docid);
                FileUtils.delete(termfreq);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
