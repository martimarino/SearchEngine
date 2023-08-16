package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.utils.DataAnalysis;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;


public class Main {
    static String collection = "src/main/resources/collection.tsv";


    public static void main(String[] args) throws IOException {

        Scanner sc = new Scanner(System.in);
        String choice;

        DataAnalysis da = new DataAnalysis(collection);
        da.runAnalysis();

        DataStructureHandler dsh = new DataStructureHandler();
        dsh.createStructures(collection);

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

        String query;

        while(true) {

            System.out.println("\n*** SEARCH ENGINE ***\n");

            System.out.println("Insert query or type 'q' to quit\n");
            query = sc.nextLine();

            if (query == null || query.isEmpty()){
                System.out.println("Error: the query is empty. Retry");
                continue;
            }

            if (query.equals("q"))
                break;

            int validN;
            do {
                validN = 0;
                System.out.println("Type the number of results to retrieve (10 or 20)");
                choice = sc.nextLine();
                try {
                    int n = Integer.parseInt(choice);
                    if(n > 0)
                        validN = 1;
                } catch (NumberFormatException nfe) {
                    System.out.println("Insert a valid positive number");
                    validN = 0;
                }
            } while (validN == 0);


            do {
                System.out.println("Type Y or N for stopwords removal and stemming options");
                choice = sc.nextLine();
            } while (!choice.equals("Y") && !choice.equals("N"));
            if (choice.equals("Y")) {
                Flag.enableSws(true);
            }

            do {
                System.out.println("Type Y or N for compression");
                choice = sc.nextLine();
            } while (!choice.equals("Y") && !choice.equals("N"));
            if (choice.equals("Y")) {
                Flag.enableCompression(true);
            }

            do {
                System.out.println("Type Y or N for scoring");
                choice = sc.nextLine();
            } while (!choice.equals("Y") && !choice.equals("N"));
            if (choice.equals("Y")) {
                Flag.enableScoring(true);
            }

        }

    }

}


