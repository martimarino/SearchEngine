package it.unipi.dii.aide.mircv.test;

import it.unipi.dii.aide.mircv.Main;
import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.query.Query;
import it.unipi.dii.aide.mircv.utils.FileSystem;

import java.io.*;
import java.util.Scanner;

import static it.unipi.dii.aide.mircv.Main.getNumberOfResults;
import static it.unipi.dii.aide.mircv.Main.getUserInput;
import static it.unipi.dii.aide.mircv.query.Query.queryStartControl;
import static it.unipi.dii.aide.mircv.utils.Constants.printTime;

public class Test {
    public static void main(String[] args){

        Flags.setConsiderSkippingBytes(true);
        queryStartControl();
        Scanner sc = new Scanner(System.in);

        while(true) {
            int avgTime = 0;
            int nQuery = 0;
            String filename = "src/main/resources/performance/";
            String message = "Select Conjunctive (c) or Disjunctive (d)";
            boolean type = getUserInput(sc, message, "c", "d");
            message = "Select scoring type between bm25 and tfidf:";
            boolean score = getUserInput(sc, message, "bm25", "tfidf");
            boolean algorithm = false;
            if(!type) {
                message = "Select algorithm type, please write maxscore for Max score or daat for DAAT) :";
                algorithm = getUserInput(sc, message, "maxscore", "daat");
            }
            int nResults = getNumberOfResults(sc);
            filename += (type ? "conj" : "disj") + "_" + (score ? "bm25" : "tfidf") + "_" + (!type ? (algorithm ? "maxscore" : "daat") : "") + ".txt";
            try (BufferedReader TSVReader = new BufferedReader(new FileReader("src/main/resources/msmarco-test2020-queries.tsv"))) {
                String line = null;
                while ((line = TSVReader.readLine()) != null) {
                    String query = line.split("\t")[1]; //splitting the line and adding its items in String[]
                    long startTime = System.currentTimeMillis();
                    Query.executeQuery(query, nResults, type, score, algorithm);
                    long endTime = System.currentTimeMillis();
                    String time = "query \"" + query + " \" time : " + (endTime - startTime) + "ms";
                    avgTime += (endTime - startTime);
                    nQuery++;

                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {

                        // Scrivi il testo con informazioni sulla formattazione nel file
                        writer.newLine();
                        writer.write(time);
                        writer.newLine();
                        writer.write("______________________________________________________________________\n");
                        writer.newLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (nQuery > 0) {
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

    }
}