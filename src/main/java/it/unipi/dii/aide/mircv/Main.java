package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.*;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.Scanner;


public class Main {

    public final static String COLLECTION_PATH = "src/main/resources/collection.tsv";

    public final static int DOCELEM_SIZE = 18;

    public static void main(String[] args) throws IOException {

        long startTime, endTime;    // variables to show execution time
        Scanner sc = new Scanner(System.in);
        /*
        DataAnalysis da = new DataAnalysis();
        da.runAnalysis();
        */

        file_cleaner();                             // delete all created files

        DataStructureHandler.SPIMIalgorithm();      // Do SPIMI Algorithm

        //DataStructureHandler.getBlocksFromDisk();
        //IndexMerger.mergeBlocks();

        // Read Collection info from disk
        startTime = System.currentTimeMillis();         // start time to get collection from disk
        DataStructureHandler.getCollectionFromDisk();
        endTime = System.currentTimeMillis();           // end time to get collection from disk
        System.out.println("\nCollection loaded");
        System.out.println("\nLoaded in(s)" + (endTime-startTime)/1000 + " in (m): " + (endTime-startTime)/60000);

        // Read Flags from disk
        startTime = System.currentTimeMillis();         // start time to read Flags from disk
        DataStructureHandler.getFlagsFromDisk();
        endTime = System.currentTimeMillis();           // end time to get collection from disk
        System.out.println("\nFlags loaded");
        System.out.println("\nLoaded in(s)" + (endTime-startTime)/1000 + " in (m): " + (endTime-startTime)/60000);

        //Read Document Index from disk and put into memory
        DocumentTable dt = new DocumentTable();         // create document table in memory
        startTime = System.currentTimeMillis();         // start time to read Document Index from disk
        // for to read all DocumentElement stored into disk
        for(int i = 0; i < 8841823; i++) { //need to put nr of documents into collection class
            DocumentElement de = DataStructureHandler.getDocumentIndexFromDisk(i*DOCELEM_SIZE); // get the ith DocElem

            if(de != null)
                dt.setDocIdToDocElem(de.getDocno(), de.getDocid(), de.getDoclength());
        }
        endTime = System.currentTimeMillis();           // end time to read Document Index from disk
        System.out.println("\nDocumentTable loaded");
        System.out.println("\nLoaded in(s)" + (endTime-startTime)/1000 + " in (m): " + (endTime-startTime)/60000);

        //Read Dictionary from disk
        startTime = System.currentTimeMillis();         // start time to read Dictionary from disk
        Dictionary d = DataStructureHandler.getDictionaryFromDisk();
        endTime = System.currentTimeMillis();           // end time to read Dictionary from disk
        System.out.println("\nV dictionary loaded");
        System.out.println("\nLoaded in(s)" + (endTime-startTime)/1000 + " in (m): " + (endTime-startTime)/60000);
/*
        DataStructureHandler.getIndexFromDisk();*/

        // while for the user interface
        while(true) {

            System.out.println("\n*** SEARCH ENGINE ***\n");
            System.out.println("Insert query or type 'q' to quit\n");
            String query = sc.nextLine();

            if (query == null || query.isEmpty()){
                System.out.println("Error: the query is empty. Retry");
                continue;
            }

            if (query.equals("q"))  // quit command
                break;

            int validN;     // 1 = positive number - 0 = negative number or not a number
            do {
                validN = 0;
                System.out.println("Type the number of results to retrieve (10 or 20)");

                try {
                    int n = Integer.parseInt(sc.nextLine());
                    if(n > 0)
                        validN = 1;
                } catch (NumberFormatException nfe) {
                    System.out.println("Insert a valid positive number");
                    validN = 0;
                }
            } while (validN == 0);

            Flag.enableSws(getUserChoice(sc, "stopwords removal"));
            Flag.enableCompression(getUserChoice(sc, "compression"));
            Flag.enableScoring(getUserChoice(sc, "scoring"));

        }

    }

    /**
     * function to delete all the file except "stopwords.txt", "collection.tsv", and "msmarco-test2020-queries.tsv"
     * that are in resources.
      */
    private static void file_cleaner() {

        File folder = new File("./src/main/resources/");
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

    /**
     * fucntion to get the choise of the user for options, the options are pass
     *
     * @param sc scanner to get the choice of the user inserted via keyboard
     * @param option  options passed by parameter
     * @return true if the user chooses yes (enter Y), false if the user chooses no (enter N)
     */
    private static boolean getUserChoice(Scanner sc, String option) {
        while (true) {
            System.out.println("Type Y or N for " + option + " options");   // print of the option
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


