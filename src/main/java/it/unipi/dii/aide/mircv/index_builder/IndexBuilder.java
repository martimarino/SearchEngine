package it.unipi.dii.aide.mircv.index_builder;

import it.unipi.dii.aide.mircv.data_structures.CollectionStatistics;
import it.unipi.dii.aide.mircv.data_structures.Flags;
import it.unipi.dii.aide.mircv.utils.FileSystem;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Scanner;

import static it.unipi.dii.aide.mircv.data_structures.Flags.*;
import static it.unipi.dii.aide.mircv.data_structures.Flags.storeFlagsIntoDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;

public class IndexBuilder {

    public static void main(String[] args) throws IOException {

        Scanner sc = new Scanner(System.in);
        long startTime, endTime;                // variables to calculate the execution time
        // while constituting the user interface
        //String mode = sc.nextLine();        // take user's choice
        file_cleaner(); // delete all created index files

        setSws(getUserChoice(sc, "stopwords removal"));    // take user preferences on the removal of stopwords
        setCompression(getUserChoice(sc, "compression"));  // take user preferences on the compression
        setDebug_flag(getUserChoice(sc, "debug"));

        printUI("Index building " + (Flags.isCompressionEnabled()? "with " : "without ") +
                "compression " + (Flags.isSwsEnabled()? "with " : "without ") + "stopwords and stemming removal " +
                (Flags.isDebug_flag()? "with " : "without ") + "debug mode ");

        storeFlagsIntoDisk();      // store Flags

        try (
                // open partial files to read the partial dictionary and index
                RandomAccessFile partialDocidFile = new RandomAccessFile(PARTIAL_DOCID_FILE, "rw");
                RandomAccessFile partialTermfreqFile = new RandomAccessFile(PARTIAL_TERMFREQ_FILE, "rw");
                RandomAccessFile partialDictFile = new RandomAccessFile(PARTIAL_DICTIONARY_FILE, "rw");
                RandomAccessFile documentTableFile = new RandomAccessFile(DOCTABLE_FILE, "rw");

                RandomAccessFile blocksFile = new RandomAccessFile(BLOCKOFFSETS_FILE, "rw");

        ) {
            // FileChannel in input (partial file)
            partialDict_channel = partialDictFile.getChannel();
            partialDocId_channel = partialDocidFile.getChannel();
            partialTermFreq_channel = partialTermfreqFile.getChannel();
            docTable_channel = documentTableFile.getChannel();
            blocks_channel = blocksFile.getChannel();

            // Do SPIMI Algorithm
            System.out.println("\nIndexing...");
            startTime = System.currentTimeMillis();         // start time to SPIMI Algorithm
            PartialIndexBuilder.SPIMI();          // partial index building through SPIMI algorithm
            // merge blocks into disk
            IndexMerger.mergeBlocks();                      // merge blocks
            endTime = System.currentTimeMillis();           // end time of merge blocks
            printTime("\nTime index building " + (endTime - startTime) + "ms (" + formatTime(startTime, endTime) + ")");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeChannels();
        }
    }

    /**
     * fucntion to get the choise of the user for options, the options are pass
     *
     * @param sc     scanner to get the choice of the user inserted via keyboard
     * @param option options passed by parameter
     * @return true if the user chooses yes (enter Y), false if the user chooses no (enter N)
     */


    public static boolean getUserChoice(Scanner sc, String option) {
        while (true) {
            printUI("Type Y or N for " + option + " option");   // print of the option
            String choice = sc.nextLine().toUpperCase();                    // take the user's choice
            // check the user's input
            if (choice.equals("Y")) {
                return true;
            } else if (choice.equals("N")) {
                return false;
            }
        }
    }
}
