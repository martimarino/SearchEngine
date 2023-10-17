package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.DataStructureHandler;
import it.unipi.dii.aide.mircv.data_structures.Flags;
import it.unipi.dii.aide.mircv.data_structures.IndexMerger;
import it.unipi.dii.aide.mircv.data_structures.PartialIndexBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Scanner;

import static it.unipi.dii.aide.mircv.data_structures.Flags.*;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.Query.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;

public class Main {

    public static void main(String[] args) throws IOException {

        Scanner sc = new Scanner(System.in);
        long startTime, endTime;                // variables to calculate the execution time

        // while constituting the user interface
        while (true) {
            // print of the user interface
            printUI(
                    "\n********** SEARCH ENGINE **********" +
                            "\n\tSelect an option:" +
                            "\n\t  m -> try merge only" +
                            "\n\t  i -> build the index" +
                            "\n\t  q -> query mode" +
                            "\n\t  x -> exit" +
                            "\n***********************************\n");
            String mode = sc.nextLine();        // take user's choice

            // switch to run user's choice
            switch (mode) {

                case "m":       // per debugging, prova solo il merge

                    delete_mergedFiles();
                    //setCompression(true);  // take user preferences on the compression

                    DataStructureHandler.readBlockOffsetsFromDisk();

                    setSws(getUserChoice(sc, "stopwords removal"));    // take user preferences on the removal of stopwords
                    setCompression(getUserChoice(sc, "compression"));  // take user preferences on the compression
                    storeFlagsIntoDisk();      // store Flags

                    startTime = System.currentTimeMillis();         // start time to merge blocks from disk
                    IndexMerger.mergeBlocks();                      // merge
                    endTime = System.currentTimeMillis();           // end time to merge blocks from disk
                    printTime( "Merged in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
                    closeChannels();
                    continue;                                   // go next while cycle

                case "i":
                    file_cleaner();                             // delete all created files

                    setSws(getUserChoice(sc, "stopwords removal"));    // take user preferences on the removal of stopwords
                    setCompression(getUserChoice(sc, "compression"));  // take user preferences on the compression

                    storeFlagsIntoDisk();      // store Flags
                    // Do SPIMI Algorithm
                    System.out.println("\nIndexing...");
                    startTime = System.currentTimeMillis();         // start time to SPIMI Algorithm
                    PartialIndexBuilder.SPIMIalgorithm();          // do SPIMI
                    endTime = System.currentTimeMillis();           // end time of SPIMI algorithm
                    printTime("\nSPIMI Algorithm done in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");

                    // merge blocks into disk
                    startTime = System.currentTimeMillis();         // start time to merge blocks
                    IndexMerger.mergeBlocks();                      // merge blocks
                    endTime = System.currentTimeMillis();           // end time of merge blocks
                    printTime("\nBlocks merged in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
                    closeChannels();
                    continue;                           // go next while iteration

                case "d", "q":
                    while (true) {
                        Flags.setConsiderSkippingBytes(true);

                        if (!Query.queryStartControl())
                            continue;

                        printUI("Insert query (or press x to exit):");
                        String q = sc.nextLine();           // take user's query

                        if (q == null || q.isEmpty()) {
                            printError("Error: the query is empty. Please, retry.");
                            continue;                           // go next while iteration
                        }

                        if (q.equals("x"))
                            return;

                        printUI("Select scoring type (t for TFIDF, b for BM25): \n");
                        String scoringType = sc.nextLine().toLowerCase().trim();
                        if(scoringType != "t" || scoringType != "b")
                            scoringType = "t";
                        // take user preferences on the scoring
                        getNumberOfResults(q, sc, scoringType);

                        closeChannels();
                    }

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
    public static boolean getUserChoice(Scanner sc, String option) {
        while (true) {
            printUI("\nType Y or N for " + option + " options");   // print of the option
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
    public static void printQueryResults(ArrayList<Integer> rankedResults)
    {
        if (rankedResults.size() != 0)      // there are results
        {
            System.out.println(ANSI_CYAN + "Query results:" + ANSI_RESET);
            for (int i = 0; i < rankedResults.size(); i++)
                printUI((i + 1) + " - " + rankedResults.get(i));
        }
        else                                // there aren't results
            printUI("No results found for this query.");
    }

    private static void getNumberOfResults(String query, Scanner sc, String score){
        while(true) {
            printUI("Insert number of results (10 or 20):");
            int k = Integer.parseInt(sc.nextLine().trim());
            if(k == 10 || k == 20) {
                while(true) {
                    printUI("Choice conjunctive or disjunctive (press C or D)");
                    String queryType = sc.nextLine().toLowerCase();
                    if (queryType.equals("c") || queryType.equals("d")){
                        try {
                            executeQuery(query, k, queryType, score.equals("t")? false : true); //score true if BM25, false if TFIDF
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        return;
                    }
                }
            }

        }
    }

}