package it.unipi.dii.aide.mircv.index_builder;

import java.io.IOException;

import static it.unipi.dii.aide.mircv.Main.getUserChoice;
import static it.unipi.dii.aide.mircv.Main.sc;
import static it.unipi.dii.aide.mircv.Main.startTime;
import static it.unipi.dii.aide.mircv.Main.endTime;
import static it.unipi.dii.aide.mircv.data_structures.Flags.*;
import static it.unipi.dii.aide.mircv.data_structures.Flags.storeFlagsIntoDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;

public class IndexBuilder {
    public static void main(String[] args) throws IOException {

        printUI("\n++++++++++++  INDEX BUILDER  ++++++++++++\n");

        buildInvertedIndex();

    }

    public static void buildInvertedIndex() throws IOException {
        file_cleaner();                             // delete all created files
        setSws(getUserChoice(sc, "stopwords removal"));    // take user preferences on the removal of stopwords
        setCompression(getUserChoice(sc, "compression"));  // take user preferences on the compression
        setDebug_flag(getUserChoice(sc, "debug"));
        storeFlagsIntoDisk();      // store Flags
        // Do SPIMI Algorithm
        System.out.println("\nIndexing...");
        startTime = System.currentTimeMillis();         // start time to SPIMI Algorithm
        PartialIndexBuilder.SPIMI();          // do SPIMI
        endTime = System.currentTimeMillis();           // end time of SPIMI algorithm
        printTime("\nSPIMI Algorithm done in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");

        // merge blocks into disk
        startTime = System.currentTimeMillis();         // start time to merge blocks
        IndexMerger.mergeBlocks();                      // merge blocks
        endTime = System.currentTimeMillis();           // end time of merge blocks
        printTime("\nBlocks merged in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
        delete_tempFiles();
        closeChannels();
    }
}
