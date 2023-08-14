package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.DataStructureHandler;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {

        /* Read Collection info from disk */
        DataStructureHandler.getCollectionFromDisk();
        System.out.println("\nCollection loaded");

        /* Read Options from disk */
        DataStructureHandler.getOptionsFromDisk();
        System.out.println("\nOptions loaded");

        /* Read Document Index from disk */
        DataStructureHandler.getDocumentIndexFromDisk();
        System.out.println("\nDocument loaded");

        /* Read Dictionary from disk */
        DataStructureHandler.getDictionaryFromDisk();
        System.out.println("\nDictionary loaded");

        /* Load Stopwords */
//        TextProcessor.stopwords= ;

        String query;

        while(true) {

            System.out.println("--------------------------------------------");
            System.out.println("Search Engine");
            System.out.println("--------------------------------------------\n");

            System.out.println("Insert query or type 'q' to quit\n");
            Scanner sc = new Scanner(System.in);
            query = sc.nextLine();

            if (query == null || query.isEmpty()){
                System.out.println("Error: the query is empty. Retry");
                continue;
            }

            if (query.equals("q"))
                break;




        }

    }

}