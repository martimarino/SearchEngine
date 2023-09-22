package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import static it.unipi.dii.aide.mircv.utils.FileSystem.*;
import static it.unipi.dii.aide.mircv.utils.Constants.*;


public class Main {

    public static void main(String[] args) throws IOException {

        Scanner sc = new Scanner(System.in);
        long startTime, endTime;                // variables to calculate the execution time

        // while constituting the user interface
        while (true) {
            // print of the user interface
            System.out.println(ANSI_CYAN + "\n********** SEARCH ENGINE **********" + ANSI_RESET);
            System.out.println(ANSI_CYAN +
                    "\n\tSelect an option:" +
                    "\n\t  m -> try merge only" +
                    "\n\t  i -> build the index" +
                    "\n\t  l -> load from disk" +
                    "\n\t  q -> query mode" +
                    "\n\t  x -> exit"
                    + ANSI_RESET);
            System.out.println(ANSI_CYAN + "\n***********************************\n" + ANSI_RESET);
            String mode = sc.nextLine();        // take user's choice

            // switch to run user's choice
            switch (mode) {

                case "m":       // per debugging, prova solo il merge
                    delete_mergedFiles();

                    DataStructureHandler.readBlockOffsetsFromDisk();

                    startTime = System.currentTimeMillis();         // start time to merge blocks from disk
                    IndexMerger.mergeBlocks();                      // merge
                    endTime = System.currentTimeMillis();           // end time to merge blocks from disk
                    System.out.println(ANSI_YELLOW + "Merged in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
                    continue;                                   // go next while cycle

                case "i":
                    file_cleaner();                             // delete all created files

                    Flag.setSws(getUserChoice(sc, "stopwords removal"));    // take user preferences on the removal of stopwords
                    Flag.setCompression(getUserChoice(sc, "compression"));  // take user preferences on the compression
                    Flag.setScoring(getUserChoice(sc, "scoring"));          // take user preferences on the scoring

                    DataStructureHandler.storeFlagsIntoDisk();      // store Flags
                    // Do SPIMI Algorithm
                    System.out.println("\nIndexing...");
                    startTime = System.currentTimeMillis();         // start time to SPIMI Algorithm
                    DataStructureHandler.SPIMIalgorithm();          // do SPIMI
                    endTime = System.currentTimeMillis();           // end time of SPIMI algorithm
                    System.out.println(ANSI_YELLOW + "\nSPIMI Algorithm done in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

                    // merge blocks into disk
                    startTime = System.currentTimeMillis();         // start time to merge blocks
                    IndexMerger.mergeBlocks();                      // merge blocks
                    endTime = System.currentTimeMillis();           // end time of merge blocks
                    System.out.println(ANSI_YELLOW + "\nBlocks merged in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
                    continue;                           // go next while iteration

/*
                case "l":

                    // Read Flags from disk
                    DataStructureHandler.readFlagsFromDisk();

                    // Read collection statistics from disk
                    DataStructureHandler.readCollectionStatsFromDisk();

                    // Read Document Table from disk and put into memory
                    startTime = System.currentTimeMillis();             // start time to read Document Table from disk
                    DataStructureHandler.readDocumentTableFromDisk();   // read Document Table
                    endTime = System.currentTimeMillis();               // end time to read Document Table from disk
                    System.out.println(ANSI_YELLOW + "Document Table loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

                    // Read Dictionary from disk
                    startTime = System.currentTimeMillis();         // start time to read Dictionary from disk
                    DataStructureHandler.readDictionaryFromDisk();  // read dictionary
                    endTime = System.currentTimeMillis();           // end time to read Dictionary from disk
                    System.out.println(ANSI_YELLOW + "Dictionary loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
                    continue;                           // go next while iteration
*/

                case "q":       // query
                    ArrayList<Integer> rankedResults;       // ArrayList that contain the ranked results of query
                    int numberOfResults = 0;    // take the integer entered by users that indicate the number of results wanted for query

                    // control check that all the files and resources required to execute a query are present
                    if (!QueryProcessor.queryStartControl()) {
                        System.out.println(ANSI_RED + "Error: there aren't all files and resources . Please set all and than retry." + ANSI_RESET);
                        continue;                           // go next while iteration
                    }

                    System.out.print(ANSI_CYAN + "Insert query: \n" + ANSI_RESET);
                    String query = sc.nextLine();           // take user's query
                    // control check of the query
                    if (query == null || query.isEmpty()) {
                        System.out.println(ANSI_RED + "Error: the query is empty. Please, retry." + ANSI_RESET);
                        continue;                           // go next while iteration
                    }

                    boolean isConjunctive = false;          // true = Conjunctive query
                    boolean isDisjunctive = false;          // true = Disjunctive query
                    // do while for choosing Conjunctive(AND) or Disjunctive(OR) query
                    do {
                        System.out.println(ANSI_CYAN + "Type C for choosing Conjunctive query or D for choosing Disjunctive queries." + ANSI_RESET);
                        try {
                            String choice = sc.nextLine().toUpperCase();        // take the user's choice
                            // check the user's input
                            if (choice.equals("C")) {
                                isConjunctive = true;           // set isConjunctive
                            } else if (choice.equals("D")) {
                                isDisjunctive = true;           // set isDisjunctive
                            }
                        } catch (NumberFormatException nfe) {
                            System.out.println(ANSI_RED + "Insert a valid character." + ANSI_RESET);
                        }
                    } while (!(isConjunctive || isDisjunctive));  // continues until isConjunctive or isDisjunctive is set

                    int validN = 0;     // 1 = positive number - 0 = negative number or not a number
                    // do while for choosing the number of results to return
                    do {
                        System.out.println(ANSI_CYAN + "Type the number of results to retrieve (10 or 20)" + ANSI_RESET);
                        try {
                            numberOfResults = Integer.parseInt(sc.nextLine());    // take the int inserted by user
                            validN = (numberOfResults > 0) ? 1 : 0;               // validity check of the int
                        } catch (NumberFormatException nfe) {
                            System.out.println(ANSI_RED + "Insert a valid positive number" + ANSI_RESET);
                        }
                    } while (validN == 0);  // continues until a valid number is entered

                    startTime = System.currentTimeMillis();         // start time of execute query

                    // do query and retry the results
                    rankedResults = QueryProcessor.queryManager(query,isConjunctive,isDisjunctive,numberOfResults);
                    printQueryResults(rankedResults);               // print the results of the query

                    endTime = System.currentTimeMillis();           // end time of execute query

                    // shows query execution time
                    System.out.println(ANSI_YELLOW + "\nQuery executes in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
                    continue;                       // go next while iteration

                default:
                    return;     // exit to switch, case not valid
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

    /**
     * function to shows the user the ranked results (DocID) of the query executed
     *
     * @param rankedResults the results returned by the query
     */
    private static void printQueryResults(ArrayList<Integer> rankedResults)
    {
        if (rankedResults.size() != 0)      // there are results
        {
            System.out.println(ANSI_CYAN + "Query results:" + ANSI_RESET);
            for (int i = 0; i < rankedResults.size(); i++)
            {
                System.out.println(ANSI_CYAN + (i + 1) + " - " + rankedResults.get(i) + ANSI_RESET);
            }
        }
        else                                // there aren't results
        {
            System.out.println(ANSI_CYAN + "No results found for this query." + ANSI_RESET);
        }
    }

}


