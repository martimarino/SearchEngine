package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.*;
import java.util.Scanner;


public class Main {

    public final static String collection_path = "src/main/resources/collection.tsv";

    public static void main(String[] args) throws IOException {

        Scanner sc = new Scanner(System.in);

//        DataAnalysis da = new DataAnalysis();
//        da.runAnalysis();

        DataStructureHandler.initializeDataStructures();

        /* Read Collection info from disk */
        DataStructureHandler.getCollectionFromDisk();
        System.out.println("\nCollection loaded");

        /* Read Flags from disk */
        DataStructureHandler.getFlagsFromDisk();
        System.out.println("\nFlags loaded");

        /* Read Document Index from disk */
        DataStructureHandler.getDocumentIndexFromDisk();
        System.out.println("\nDocument loaded");

        /* Read Dictionary from disk */
        DataStructureHandler.getDictionaryFromDisk();
        System.out.println("\nDictionary loaded");

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

    private static boolean getUserChoice(Scanner sc, String option) {
        while (true) {
            System.out.println("Type Y or N for " + option + " options");
            String choice = sc.nextLine().toUpperCase();
            if (choice.equals("Y")) {
                return true;
            } else if (choice.equals("N")) {
                return false;
            }
        }
    }

}


