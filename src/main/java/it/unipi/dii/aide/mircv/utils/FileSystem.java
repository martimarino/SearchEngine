package it.unipi.dii.aide.mircv.utils;

import java.io.*;

import it.unipi.dii.aide.mircv.data_structures.Flags;
import org.apache.commons.io.FileUtils;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import static it.unipi.dii.aide.mircv.data_structures.DocumentElem.DOCELEM_SIZE;
import static it.unipi.dii.aide.mircv.utils.Constants.*;


public final class FileSystem {

    public static FileChannel dict_channel, docTable_channel, flags_channel, docId_channel, termFreq_channel;
    public static FileChannel partialDict_channel, partialDocId_channel, partialTermFreq_channel;
    public static FileChannel blocks_channel, skip_channel;

    private FileSystem() {
        throw new UnsupportedOperationException();
    }

    /**
     * function to delete all the file except "stopwords.txt", "collection.tsv", and "msmarco-test2020-queries.tsv"
     * that are in resources.
     */
    public static void file_cleaner() {

        try {
            // Clean or create the partial folder
            File partial_folder = new File(PARTIAL_FOLDER);
            if (partial_folder.exists() && partial_folder.isDirectory()) {
                File[] partialFiles = partial_folder.listFiles();
                if (partialFiles != null && partialFiles.length > 0) {
                    FileUtils.cleanDirectory(partial_folder);
                    System.out.println("Partial folder cleaned.");
                } else {
                    System.out.println("Partial folder is already empty.");
                }
            } else {
                if (partial_folder.mkdirs()) {
                    System.out.println("Partial folder created.");
                } else {
                    System.out.println("Failed to create partial folder.");
                    return;
                }
            }

            // Clean or create the merged folder
            File merged_folder = new File(MERGED_FOLDER);
            if (merged_folder.exists() && merged_folder.isDirectory()) {
                File[] mergedFiles = merged_folder.listFiles();
                if (mergedFiles != null && mergedFiles.length > 0) {
                    FileUtils.cleanDirectory(merged_folder);
                    System.out.println("Merged folder cleaned.");
                } else {
                    System.out.println("Merged folder is already empty.");
                }
            } else {
                if (merged_folder.mkdirs()) {
                    System.out.println("Merged folder created.");
                } else {
                    System.out.println("Failed to create merged folder.");
                    return;
                }
            }

            // Clean or create the debug folder
            if(Flags.isDebug_flag()) {
                File debug_folder = new File(DEBUG_FOLDER);
                if (debug_folder.exists() && debug_folder.isDirectory()) {
                    File[] mergedFiles = debug_folder.listFiles();
                    if (mergedFiles != null && mergedFiles.length > 0) {
                        FileUtils.cleanDirectory(debug_folder);
                        System.out.println("Debug folder cleaned.");
                    } else {
                        System.out.println("Debug folder is already empty.");
                    }
                } else {
                    if (debug_folder.mkdirs()) {
                        System.out.println("Debug folder created.");
                    } else {
                        System.out.println("Failed to create debug folder.");
                        return;
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void delete_tempFiles() throws IOException {

        closeChannels();

        File partial_directory = new File(PARTIAL_FOLDER);
        try {
            FileUtils.cleanDirectory(partial_directory);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Function that check if there are all .txt files in "/resources/merged" folder
     * The file that controls are: dictionary.txt, docId.txt, documentTable.txt, termFreq.txt
     *
     * @return  true -> there are all merged files into disk
     *          false -> there aren't all merged files into disk
     */
    public static boolean areThereAllMergedFiles()
    {
        // define all file
        File docTable = new File(DOCTABLE_FILE);        // documentTable.txt
        File dict = new File(DICTIONARY_FILE);          // dictionary.txt"
        File docDID = new File(DOCID_FILE);             // docId.txt
        File docTF = new File(TERMFREQ_FILE);           // termFreq.txt

        return docTable.exists() && dict.exists() && docDID.exists() && docTF.exists();
    }

    public static void saveIntoFile(String data, String fileName) {
        try (FileWriter writer = new FileWriter(DEBUG_FOLDER + fileName, true)) {
            writer.write(data);
            writer.write(System.lineSeparator());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void closeChannels() throws IOException {

        FileChannel[] arr_channels = {dict_channel, docTable_channel, flags_channel, docId_channel, termFreq_channel,
                partialDict_channel, partialDocId_channel, partialTermFreq_channel,
                blocks_channel, skip_channel};

        for(FileChannel fl : arr_channels)
            if(fl != null && fl.isOpen())
                fl.close();

    }

}
