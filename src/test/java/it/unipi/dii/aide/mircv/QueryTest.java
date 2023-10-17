package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.Flags;
import it.unipi.dii.aide.mircv.data_structures.IndexMerger;
import it.unipi.dii.aide.mircv.data_structures.PartialIndexBuilder;
import it.unipi.dii.aide.mircv.utils.Constants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

import static it.unipi.dii.aide.mircv.Main.getUserChoice;
import static it.unipi.dii.aide.mircv.Query.queryStartControl;
import static it.unipi.dii.aide.mircv.data_structures.Flags.*;
import static it.unipi.dii.aide.mircv.data_structures.Flags.storeFlagsIntoDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.closeChannels;
import static it.unipi.dii.aide.mircv.utils.FileSystem.file_cleaner;

class QueryTest {

    String filename = "performanceOutputAll.txt";

//    @BeforeAll
//    static void getFromFile(){
//        Flags.setConsiderSkippingBytes(true);
//        try {
//            queryStartControl();
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


/*
    @Test
     void CheckQueryTimeConjunctive() {
        ArrayList<Integer> result = new ArrayList<>();
        int numberOfResults = 10;
        try (BufferedReader TSVReader = new BufferedReader(new FileReader("src/main/resources/msmarco-test2020-queries.tsv"))) {
            String line = null;
            while ((line = TSVReader.readLine()) != null) {
                String query = line.split("\t")[1]; //splitting the line and adding its items in String[]
                long startTime = System.currentTimeMillis();
                executeQuery("of the", 10);
                long endTime = System.currentTimeMillis();
                System.out.println("query '" + query + "' in time : " + (endTime - startTime)/1000 + "s");
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
*/

//    @Test
    void name() {
        ArrayList<Integer> result = new ArrayList<>();
        int numberOfResults = 10;
        int avgTimePQ = 0;
        int avgTime = 0;

        int nQuery = 0;
        try (BufferedReader TSVReader = new BufferedReader(new FileReader("src/main/resources/msmarco-test2020-queries.tsv"))) {
            String line = null;
            while ((line = TSVReader.readLine()) != null) {
                String query = line.split("\t")[1]; //splitting the line and adding its items in String[]
                long startTimePQ = System.currentTimeMillis();
                Query.executeQueryPQ(query, numberOfResults, "c");
                long endTimePQ = System.currentTimeMillis();
                String PQtime = "query \"" + query + " \" time PQ : " + (endTimePQ - startTimePQ) + "ms";
                System.out.println(PQtime);
                avgTimePQ += (endTimePQ - startTimePQ);
                long startTime = System.currentTimeMillis();
                Query.executeQuery(query, numberOfResults, "d");
                long endTime = System.currentTimeMillis();
                String time = "query \"" + query + " \" time : " + (endTime - startTime) + "ms";
                System.out.println(time);
                avgTime += (endTime - startTime);
                nQuery++;

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {

                    // Scrivi il testo con informazioni sulla formattazione nel file
                    writer.write(PQtime);
                    writer.newLine();
                    writer.write("______________________________________________________________________\n");
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(nQuery > 0) {
                String AvgPQ = "Average time PQ: " + (avgTimePQ / (nQuery)) + "ms";
                String Avg = "Average time: " + (avgTime / (nQuery)) + "ms";

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {

                    // Scrivi il testo con informazioni sulla formattazione nel file
                    writer.write("*********************** Avg times ************************");
                    writer.newLine();
                    writer.write(AvgPQ);
                    writer.newLine();
                    writer.write(Avg);
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                printTime(AvgPQ);
                printTime(Avg);
            }
        } catch (Exception e) {
            System.out.println(e);
        }

    }

//    @Test
//    void testSmallWithSkip() throws IOException {
//
//        COLLECTION_PATH = "src/main/resources/small_collection.tar.gz";
//        MEMORY_THRESHOLD = 0.008;
//        SKIP_POINTERS_THRESHOLD = 128;
//
//        setConsiderSkippingBytes(false);
//        testBuildIndex();
//
//        setConsiderSkippingBytes(true);
//        queryStartControl();
//        name();
//
//    }
//
//    @Test
//    void testSmallWithoutSkip() throws IOException {
//
//        COLLECTION_PATH = "src/main/resources/small_collection.tar.gz";
//        MEMORY_THRESHOLD = 0.008;
//        SKIP_POINTERS_THRESHOLD = Integer.MAX_VALUE;
//
//        setConsiderSkippingBytes(false);
//        testBuildIndex();
//
//        setConsiderSkippingBytes(true);
//        queryStartControl();
//        name();
//
//    }

//    @Test
//    void testFullWithSkip() {
//
//        COLLECTION_PATH = "src/main/resources/collection.tar.gz";
//        MEMORY_THRESHOLD = 0.8;
//        SKIP_POINTERS_THRESHOLD = 1024;
//
//        filename = "testFullWithSkip.txt";
//        setConsiderSkippingBytes(true);
//        queryStartControl();
//        name();
//
//    }
//
//    @Test
//    void testFullWithoutSkip() {
//
//        COLLECTION_PATH = "src/main/resources/collection.tar.gz";
//        MEMORY_THRESHOLD = 0.8;
//        SKIP_POINTERS_THRESHOLD = Integer.MAX_VALUE;
//
//        filename = "testFullWithoutSkip.txt";
//        setConsiderSkippingBytes(true);
//        queryStartControl();
//        name();
//
//    }


    void testBuildIndex() throws IOException {
        file_cleaner();                             // delete all created files

        setSws(false);    // take user preferences on the removal of stopwords
        setCompression(false);  // take user preferences on the compression
        setScoring(false);          // take user preferences on the scoring

        storeFlagsIntoDisk();      // store Flags

        Flags.setConsiderSkippingBytes(false);

        PartialIndexBuilder.SPIMIalgorithm();

        IndexMerger.mergeBlocks();

        closeChannels();
    }


}