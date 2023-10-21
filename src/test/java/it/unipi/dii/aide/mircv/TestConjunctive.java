package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.Flags;
import it.unipi.dii.aide.mircv.data_structures.IndexMerger;
import it.unipi.dii.aide.mircv.data_structures.PartialIndexBuilder;
import it.unipi.dii.aide.mircv.query.Query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import static it.unipi.dii.aide.mircv.Main.getUserChoice;
import static it.unipi.dii.aide.mircv.data_structures.Flags.*;
import static it.unipi.dii.aide.mircv.utils.Constants.formatTime;
import static it.unipi.dii.aide.mircv.utils.Constants.printTime;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;

public class TestConjunctive {

    static Scanner sc = new Scanner(System.in);
    static long startTime, endTime;
    public static void main(String[] args) throws IOException {

//        file_cleaner();                             // delete all created files
//
//        setSws(getUserChoice(sc, "stopwords removal"));    // take user preferences on the removal of stopwords
//        setCompression(getUserChoice(sc, "compression"));  // take user preferences on the compression
//
//        storeFlagsIntoDisk();      // store Flags
//        // Do SPIMI Algorithm
//        System.out.println("\nIndexing...");
//        startTime = System.currentTimeMillis();         // start time to SPIMI Algorithm
//        PartialIndexBuilder.SPIMIalgorithm();          // do SPIMI
//        endTime = System.currentTimeMillis();           // end time of SPIMI algorithm
//        printTime("\nSPIMI Algorithm done in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
//
//        // merge blocks into disk
//        startTime = System.currentTimeMillis();         // start time to merge blocks
//        IndexMerger.mergeBlocks();                      // merge blocks
//        endTime = System.currentTimeMillis();           // end time of merge blocks
//        printTime("\nBlocks merged in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
//        closeChannels();
//        delete_tempFiles();


        ArrayList<Long> timeQueries = new ArrayList<>();
        long avgTime = 0;
        int nQuery = 0;

        try (BufferedReader TSVReader = new BufferedReader(new FileReader("src/main/resources/msmarco-test2020-queries.tsv"))) {
            String line;

            Flags.setConsiderSkippingBytes(true);

            if (!Query.queryStartControl())
                return;

            while ((line = TSVReader.readLine()) != null) {
                String query = line.split("\t")[1]; //splitting the line and adding its items in String[]
                long startTime = System.currentTimeMillis();
                Query.executeQueryPQ(query, 10, "c", false);
                long endTime = System.currentTimeMillis();
                String time = "query = \"" + query + " \"";
                System.out.println(time);
                avgTime += (int) (endTime - startTime);
                timeQueries.add(avgTime);
                nQuery++;
            }
            closeChannels();

            long end = System.currentTimeMillis() - startTime;
            System.out.println("Query executed in: " + end + " ms");

            /* Add time to array */
            timeQueries.add(end);

            avgTime /= nQuery;

            System.out.println("The average time to process test queries is: -> " + avgTime + " ms");
        }
    }
}