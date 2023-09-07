package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.*;
import java.util.Scanner;

import static it.unipi.dii.aide.mircv.data_structures.DocumentElement.DOCELEM_SIZE;
import static it.unipi.dii.aide.mircv.utils.Constants.*;


public class Main {

    public static void main(String[] args) {

        Scanner sc = new Scanner(System.in);

        while (true) {

            System.out.println(ANSI_CYAN + "\n********** SEARCH ENGINE **********" + ANSI_RESET);
            System.out.println(ANSI_CYAN + "\n\tSelect an option:\n\t  i -> build the index\n\t  q -> query mode\n\t  x -> exit" + ANSI_RESET);
            System.out.println(ANSI_CYAN + "\n***********************************\n" + ANSI_RESET);
            String mode = sc.nextLine();

            switch (mode) {

                case "i":
                    file_cleaner();                             // delete all created files

                    Flag.enableSws(getUserChoice(sc, "stopwords removal"));
                    Flag.enableCompression(getUserChoice(sc, "compression"));
                    Flag.enableScoring(getUserChoice(sc, "scoring"));

                    long startTime, endTime;

                    // Do SPIMI Algorithm
                    System.out.println("\nIndexing...");
                    DataStructureHandler.SPIMIalgorithm();

                    // Read Flags from disk
                    startTime = System.currentTimeMillis();
                    DataStructureHandler.getFlagsFromDisk();
                    endTime = System.currentTimeMillis();
                    System.out.println("\nFlags loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");


                    //Read Document Index from disk and put into memory
                    DocumentTable dt = new DocumentTable();         // create document table in memory
                    startTime = System.currentTimeMillis();

                    // for to read all DocumentElement stored into disk
                    for(int i = 0; i < 8841823; i++) { //need to put nr of documents into collection class
                        DocumentElement de = DataStructureHandler.getDocumentIndexFromDisk(i*DOCELEM_SIZE); // get the ith DocElem

                        if(de != null)
                            dt.setDocIdToDocElem(de.getDocno(), de.getDocid(), de.getDoclength());
                    }
                    endTime = System.currentTimeMillis();           // end time to read Document Index from disk
                    System.out.println("Document Table loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");

                    //Read Dictionary from disk
                    startTime = System.currentTimeMillis();         // start time to read Dictionary from disk
                    Dictionary d = DataStructureHandler.getDictionaryFromDisk();
                    endTime = System.currentTimeMillis();           // end time to read Dictionary from disk
                    System.out.println("Dictionary loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
/*
        DataStructureHandler.getIndexFromDisk();*/

                    continue;

                case "q":

                    System.out.print(ANSI_CYAN + "Insert query: \n" + ANSI_RESET);
                    String query = sc.nextLine();

                    if (query == null || query.isEmpty()) {
                        System.out.println(ANSI_CYAN + "Error: the query is empty. Retry" + ANSI_RESET);
                        continue;
                    }

                    int validN = 0;     // 1 = positive number - 0 = negative number or not a number
                    do {
                        System.out.println(ANSI_CYAN + "Type the number of results to retrieve (10 or 20)" + ANSI_RESET);
                        try {
                            int n = Integer.parseInt(sc.nextLine());
                            validN = (n > 0) ? 1 : 0;
                        } catch (NumberFormatException nfe) {
                            System.out.println(ANSI_CYAN + "Insert a valid positive number" + ANSI_RESET);
                        }
                    } while (validN == 0);
                    continue;

                default:
                    return;
            }
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
     * @param sc     scanner to get the choice of the user inserted via keyboard
     * @param option options passed by parameter
     * @return true if the user chooses yes (enter Y), false if the user chooses no (enter N)
     */
    private static boolean getUserChoice(Scanner sc, String option) {
        while (true) {
            System.out.println(ANSI_CYAN + "\nType Y or N for " + option + " options" + ANSI_RESET);   // print of the option
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


