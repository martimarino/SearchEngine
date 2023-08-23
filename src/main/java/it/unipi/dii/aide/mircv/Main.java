package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.*;
import java.util.Scanner;


public class Main {

    public final static String COLLECTION_PATH = "src/main/resources/collection.tsv";

    public final static int DOCELEM_SIZE = 28;

    public static void main(String[] args) throws IOException {

        Scanner sc = new Scanner(System.in);

/*       DataAnalysis da = new DataAnalysis();
       da.runAnalysis();*/

        file_cleaner();

        DataStructureHandler.initializeDataStructures();
        DataStructureHandler.SPIMIalgorithm();

        /* Read Collection info from disk */
        DataStructureHandler.getCollectionFromDisk();
        System.out.println("\nCollection loaded");

        /* Read Flags from disk */
        DataStructureHandler.getFlagsFromDisk();
        System.out.println("\nFlags loaded");

        /* Read Document Index from disk */
        for(int i = 0; i < 10000; i++) { //need to put nr of documents into collection class
            DocumentElement de = DataStructureHandler.getDocumentIndexFromDisk(i* DOCELEM_SIZE);

            if(de != null)
                DataStructureHandler.getDt().setDocIdToDocElem(de.getDocno(), de.getDocid(), de.getDoclength());
        }
        System.out.println("\nDocument loaded");

        /* Read Dictionary from disk */

        DataStructureHandler.getDictionaryFromDisk();
        System.out.println("\nV dictionary loaded");

        DataStructureHandler.getIndexFromDisk();

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


