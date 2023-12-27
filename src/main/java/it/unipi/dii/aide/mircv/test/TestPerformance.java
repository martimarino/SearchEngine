package it.unipi.dii.aide.mircv.test;

import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.query.Query;
import it.unipi.dii.aide.mircv.query.ResultBlock;
import it.unipi.dii.aide.mircv.utils.TextProcessor;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.stream.Collectors;

import static it.unipi.dii.aide.mircv.Main.getNumberOfResults;
import static it.unipi.dii.aide.mircv.Main.getUserInput;
import static it.unipi.dii.aide.mircv.query.Query.*;
import static it.unipi.dii.aide.mircv.utils.Constants.printTime;

public class TestPerformance {

    public static PriorityQueue<ResultBlock> pq_res;   // contains results (increasing)

    public static void main(String[] args) {

        Flags.setConsiderSkipElem(true);
        queryStartControl();
        Scanner sc = new Scanner(System.in);
        String filename = "src/main/resources/performance/";

        while(true) {
            String message = "Select efficiency (e) or effectiveness (ef) test ";
            String efficiencyOrEffectiveness = getUserInput(sc, message, "e", "ef");
            int nResults = getNumberOfResults(sc);
            message = "Select Conjunctive (c) or Disjunctive (d)";
            String type = getUserInput(sc, message, "c", "d");
            String algorithm = " ";
            if (type.equals("d")) {
                message = "Select algorithm type between maxscore (m) or daat (d) :";
                algorithm = getUserInput(sc, message, "m", "d");
            }
            if (efficiencyOrEffectiveness.equals("e")) {
                message = "Select scoring type between bm25 (b) and tfidf (t):";
                String score = getUserInput(sc, message, "b", "t");
                filename += (type.equals("c") ? "conj" : "disj") + "_" + (score.equals("b") ? "bm25" : "tfidf") + "_" + (type.equals("d") ? (algorithm.equals("m") ? "maxscore" : "daat") : "") + "_" + (Flags.isSwsEnabled() ? "nostopwords" : "") + ".txt";
                evaluateEfficiency(filename, nResults, type, score, algorithm);

            } else {
                Query.k = nResults;
                Query.disj_conj = type;
                Query.tfidf_bm25 = "b";
                Query.daat_maxscore = algorithm;
                trec_eval(nResults, filename + "test_" + type + "_b" + "_" + algorithm);
            }
        }
    }

    public static void evaluateEfficiency(String filename, int nResults, String type, String score, String algorithm) {
        int avgTime = 0;
        int nQuery = 0;

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

    public static void trec_eval(int k, String filename) {

        String fix = "Q0";
        String runid = "RUN-01";

        try (BufferedReader TSVReader = new BufferedReader(new FileReader("src/main/resources/msmarco-test2020-queries.tsv"));
             BufferedWriter resultsBuffer = new BufferedWriter(new FileWriter(filename + "_results.txt", true))
        ) {
            String line = null;
            while ((line = TSVReader.readLine()) != null) {
                PriorityQueue<ResultBlock> resultQueueInverse = new PriorityQueue<>(k, new ResultBlock.CompareResDec());

                String query = line.split("\t")[1]; //splitting the line and adding its items in String[]
                String topicId = line.split("\t")[0]; // get the id of the query
                System.out.println("Query " + topicId);
                ArrayList<String> q = TextProcessor.preprocessText(query);
                List<String> query_terms = q.stream().distinct().collect(Collectors.toList());
                prepareStructures(query_terms);
                //results from increasing order priority queue to decreasing order one
                while (!Query.pq_res.isEmpty()) {
                    ResultBlock r = Query.pq_res.poll();
                    resultQueueInverse.add(r);
                }

                int i = 1;

                System.out.println("size: " + resultQueueInverse.size());

                while (!resultQueueInverse.isEmpty()) {
                    ResultBlock polled = resultQueueInverse.poll();
                    String resultsLine = topicId + "\t" + fix + "\t" + DataStructureHandler.documentTable.get(polled.getDocId()).getDocno() + "\t" + i + "\t" + polled.getScore() + "\t" + runid + "\n";
                    resultsBuffer.write(resultsLine);
                    i++;
                }
                clearStructures();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
