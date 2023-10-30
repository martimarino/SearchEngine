package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.Flags;
import it.unipi.dii.aide.mircv.query.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;

public class Main {

    public static Scanner sc = new Scanner(System.in);
    public static long startTime, endTime;                // variables to calculate the execution time

    public static void main(String[] args) throws IOException {

        printUI("\n++++++++++++  SEARCH ENGINE  ++++++++++++\n");

        Flags.setConsiderSkipElem(true);

        if (!Query.queryStartControl())
            return;

        while (true) {

            printUI("\nInsert query (or press x to exit):");
            String q = sc.nextLine();           // take user's query

            if (q == null || q.isEmpty()) {
                printError("Error: the query is empty. Please, retry.");
                continue;                           // go next while iteration
            }

            if (q.equals("x"))
                return;

            String message = "Select Conjunctive (c) or Disjunctive (d)";
            String type = getUserInput(sc, message, "c", "d");
            message = "Select scoring type between bm25 (b) and tfidf (t):";
            String score = getUserInput(sc, message, "b", "t");
            String algorithm = " ";
            if (type.equals("d")) {
                message = "Select algorithm type between Max score (m) or DAAT (d) :";
                algorithm = getUserInput(sc, message, "m", "d");
            }
            int nResults = getNumberOfResults(sc);
            Query.executeQuery(q, nResults, type, score, algorithm);
            closeChannels();
        }

    }

    /**
     * function to shows the user the ranked results (DocID) of the query executed
     *
     * @param rankedResults the results returned by the query
     */
    public static void printQueryResults(ArrayList<Integer> rankedResults)
    {
        if (!rankedResults.isEmpty())      // there are results
        {
            System.out.println(ANSI_CYAN + "Query results:" + ANSI_RESET);
            for (int i = 0; i < rankedResults.size(); i++)
                printUI((i + 1) + " - " + rankedResults.get(i));
        }
        else                                // there aren't results
            printUI("No results found for this query.");
    }

    public static int getNumberOfResults(Scanner sc){
        while(true) {
            printUI("Insert number of results (10 or 20):");
            int k = Integer.parseInt(sc.nextLine().trim());
            if(k == 10 || k == 20) {
                    return k;
            }
        }
    }

    public static String getUserInput(Scanner sc, String message, String option1, String option2){
        while(true){
            printUI(message);
            String text = sc.nextLine().toLowerCase().trim();
            if(text.equals(option1))
                return option1;
            if(text.equals(option2))
                return option2;
        }
    }

}