package it.unipi.dii.aide.mircv.utils;

public final class Constants {

    private Constants() {
        throw new UnsupportedOperationException();
    }

    public static final String RES_FOLDER = "src/main/resources/";
    public static final String PARTIAL_FOLDER = RES_FOLDER + "partial/";
    public static final String MERGED_FOLDER = RES_FOLDER + "merged/";

    // -------------------------------- Constants for file paths -------------------------------------------
    public final static String COLLECTION_PATH = RES_FOLDER + "collection.tsv";

    public static final String PARTIAL_DICTIONARY_FILE = PARTIAL_FOLDER + "partial_dictionary.txt"; // file in which is stored the vocabulary in blocks
    public static final String PARTIAL_DOCID_FILE = PARTIAL_FOLDER + "partial_docId.txt";  // file containing the docId (element of posting list) for each block
    public static final String PARTIAL_TERMFREQ_FILE = PARTIAL_FOLDER + "partial_termFreq.txt";   // file containing the TermFrequency (element of posting list) for each block

    public static final String DOCTABLE_FILE = MERGED_FOLDER + "documentTable.txt"; // file in which is stored the document table
    public static final String DICTIONARY_FILE = MERGED_FOLDER + "dictionary.txt"; // file in which is stored the dictionary
    public static final String DOCID_FILE = MERGED_FOLDER + "docId.txt";   // file containing the docId of the InvertedIndex merged
    public static final String TERMFREQ_FILE = MERGED_FOLDER + "termFreq.txt";   // file containing the termFreq of the InvertedIndex merged

    public static final String BLOCKOFFSETS_FILE = PARTIAL_FOLDER + "blocks.txt"; // file containing the offset of each vocabulary block
    public static final String FLAGS_FILE = RES_FOLDER + "flags.txt"; // file in which flags are stored

    // -------------------------------- Constants for variable bytes -------------------------------------------

    public static final int INT_BYTES = Integer.BYTES;
    public static final int LONG_BYTES = Long.BYTES;

    public static final int TERM_DIM = 20;                      // Length of a term (in bytes)

    public static int N_POSTINGS = 0;                  // Number of partial postings to save in the file

    // -------------------------------------- Constants for file offsets ----------------------------------------------

    public static long PARTIAL_DICTIONARY_OFFSET = 0;          // Offset of the terms in the dictionary
    public static long INDEX_OFFSET = 0;               // Offset of the termfreq and docid in index

    public static double MEMORY_THRESHOLD = 0.8;

    // ---------------------------------------- Utilities for debugging -----------------------------------------------

    // variable that indicates after how many iterations to make a control printout (used in various methods)
    public static int printInterval = 1000000;
    // variable that stipulates the behaviour for control printouts. If false there will be no printouts, if true there will be all printouts.
    public static boolean verbose = false;

    public static String formatTime(long start, long end) {

        long elapsedTime = end - start;
        long seconds = (elapsedTime / 1000) % 60;
        long minutes = (elapsedTime / 1000 / 60) % 60;
        long hours = (elapsedTime / 1000 / 3600);

        return String.format("%02d:%02d:%02d", hours, minutes, seconds);
    }

    // terminal colors
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_CYAN = "\u001B[96m";
    public static final String ANSI_YELLOW = "\u001B[93m";

}
