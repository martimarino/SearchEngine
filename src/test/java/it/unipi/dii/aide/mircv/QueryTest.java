package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.Flags;
import it.unipi.dii.aide.mircv.query.Query;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;

import static it.unipi.dii.aide.mircv.query.Query.queryStartControl;
import static it.unipi.dii.aide.mircv.utils.Constants.printTime;

class QueryTest {

    String filename = "performanceOutputAll.txt";

    @BeforeAll
    static void getFromFile(){
        Flags.setConsiderSkipInfo(true);
        queryStartControl();

    }


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

    @Test
    void name() {
        int numberOfResults = 10;
        int avgTimePQ = 0;
        int avgTime = 0;
        int nQuery = 0;
        try (BufferedReader TSVReader = new BufferedReader(new FileReader("src/main/resources/msmarco-test2020-queries.tsv"))) {
            String line;
            while ((line = TSVReader.readLine()) != null) {
                String query = line.split("\t")[1]; //splitting the line and adding its items in String[]
                long startTimePQ = System.currentTimeMillis();
//                Query.executeQueryPQ(query, numberOfResults, false, true, true);
                long endTimePQ = System.currentTimeMillis();
                String PQtime = "query \"" + query + " \" time PQ : " + (endTimePQ - startTimePQ) + "ms";
                System.out.println(PQtime);
                avgTimePQ += (int) (endTimePQ - startTimePQ);
                long startTime = System.currentTimeMillis();
                Query.executeQuery(query, numberOfResults, false, false, false);
                long endTime = System.currentTimeMillis();
                String time = "query \"" + query + " \" time : " + (endTime - startTime) + "ms";
                //System.out.println(time);
                avgTime += (endTime - startTime);
                avgTimePQ += (endTimePQ - startTimePQ);
                nQuery++;

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {

                    // Scrivi il testo con informazioni sulla formattazione nel file
                    writer.write(PQtime);
                    writer.newLine();
                    writer.write(time);
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
                //assertTrue((avgTimePQ / (nQuery)) < 1000);
                //assertTrue( (avgTime / (nQuery))  < 1000);

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
            e.printStackTrace();
        }

    }

    @Test
    void testMaxScore() {

        int numberOfResults = 10;
        int avgTime = 0;
        String filename = "outputMaxScore.txt";
        int nQuery = 0;
        try (BufferedReader TSVReader = new BufferedReader(new FileReader("src/main/resources/msmarco-test2020-queries.tsv"))) {
            String line;
            while ((line = TSVReader.readLine()) != null) {
                String query = line.split("\t")[1];
                long startTime = System.currentTimeMillis();
                Query.executeQuery(query, numberOfResults, false, false, true);
                long endTime = System.currentTimeMillis();
                String time = "query \"" + query + " \" time : " + (endTime - startTime) + "ms";
                System.out.println(time);
                avgTime += (endTime - startTime);
                nQuery++;

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {

                    // Scrivi il testo con informazioni sulla formattazione nel file
                    writer.write(time);
                    writer.newLine();
                    writer.write("______________________________________________________________________\n");
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(nQuery > 0) {
                String Avg = "Average time: " + (avgTime / (nQuery)) + "ms";

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {

                    // Scrivi il testo con informazioni sulla formattazione nel file
                    writer.write("*********************** Avg times ************************");
                    writer.newLine();
                    writer.write(Avg);
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                printTime(Avg);
            }
        } catch (Exception e) {
            e.printStackTrace();
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

/*    @Test
    void testFullWithSkip() throws IOException {

        COLLECTION_PATH = "src/main/resources/collection.tar.gz";
        MEMORY_THRESHOLD = 0.8;
        SKIP_POINTERS_THRESHOLD = 1024;

        filename = "testFullWithSkip.txt";
        setConsiderSkippingBytes(true);
        queryStartControl();
        name();

    }

    @Test
    void testFullWithoutSkip() throws IOException {

        COLLECTION_PATH = "src/main/resources/collection.tar.gz";
        MEMORY_THRESHOLD = 0.8;
        SKIP_POINTERS_THRESHOLD = Integer.MAX_VALUE;

        filename = "testFullWithoutSkip.txt";
        setConsiderSkippingBytes(true);
        queryStartControl();
        name();

    }


    void testBuildIndex () throws IOException {
        file_cleaner();                             // delete all created files

        setSws(false);    // take user preferences on the removal of stopwords
        setCompression(false);  // take user preferences on the compression

        storeFlagsIntoDisk();      // store Flags

        Flags.setConsiderSkippingBytes(false);

        PartialIndexBuilder.SPIMIalgorithm();

        IndexMerger.mergeBlocks();

        closeChannels();
    }


    @Test
    void testResults() {

        try {
            String query = "home site";
            getFromFile();
            executeQuery(query, 10, "d", false);
            executeQueryPQ(query, 10, "d", false);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }*/


}