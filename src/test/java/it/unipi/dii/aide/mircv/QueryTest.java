package it.unipi.dii.aide.mircv;

import it.unipi.dii.aide.mircv.data_structures.Flags;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import static it.unipi.dii.aide.mircv.Query.queryStartControl;
import static it.unipi.dii.aide.mircv.utils.Constants.*;

class QueryTest {

    @BeforeAll
    static void getFromFile(){
        Flags.setConsiderSkippingBytes(true);
        try {
            queryStartControl();

        } catch (IOException e) {
            e.printStackTrace();
        }
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
        ArrayList<Integer> result = new ArrayList<>();
        boolean isConjunctive = true;
        boolean isDisjunctive = false;
        int numberOfResults = 10;
        int avgTimePQ = 0;
        int avgTime = 0;
        int avgTimeAle = 0;
        int nQuery = 0;
        String filename = "performanceOutputAll.txt";
        try (BufferedReader TSVReader = new BufferedReader(new FileReader("src/main/resources/msmarco-test2020-queries.tsv"))) {
            String line = null;
            while ((line = TSVReader.readLine()) != null) {
                String query = line.split("\t")[1]; //splitting the line and adding its items in String[]
                long startTimePQ = System.currentTimeMillis();
                Query.executeQueryPQ(query, numberOfResults, "d");
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
                long startTimeAle = System.currentTimeMillis();
                QueryProcessor.queryManager(query, false, true, numberOfResults);
                long endTimeAle = System.currentTimeMillis();
                String timeAle = "query \"" + query + " \" time : " + (endTimeAle - startTimeAle) + "ms";
                System.out.println(time);
                avgTimeAle += (endTimeAle - startTimeAle);
                nQuery++;

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {

                    // Scrivi il testo con informazioni sulla formattazione nel file
                    writer.write(PQtime);
                    writer.newLine();
                    writer.write(time);
                    writer.newLine();
                    writer.write("____________________________________________");
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(nQuery > 0) {
                String AvgPQ = "Average time PQ: " + (avgTimePQ / (nQuery)) + "ms";
                String Avg = "Average time: " + (avgTime / (nQuery)) + "ms";
                String AleAvg =  "Average time Ale: " + (avgTimeAle / (nQuery)) + "ms";

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {

                    // Scrivi il testo con informazioni sulla formattazione nel file
                    writer.write("*********************** Avg times ************************");
                    writer.newLine();
                    writer.write(AvgPQ);
                    writer.newLine();
                    writer.write(Avg);
                    writer.newLine();
                    writer.write(AleAvg);
                    writer.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                printTime(AvgPQ);
                printTime(Avg);
                printTime(AleAvg);
            }
        } catch (Exception e) {
            System.out.println(e);
        }


    }

}