//package it.unipi.dii.aide.mircv.utils;
//
//import it.unipi.dii.aide.mircv.TextProcessor;
//import it.unipi.dii.aide.mircv.data_structures.PostingList;
//
//import java.io.BufferedReader;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.nio.charset.StandardCharsets;
//import java.util.ArrayList;
//import java.util.HashMap;
//
//import static it.unipi.dii.aide.mircv.utils.Constants.COLLECTION_PATH;
//
//
//public class DataAnalysis {
//
//    HashMap<String, PostingList> dict = new HashMap<>();
//
//    static BufferedReader br;
//    static int maxTermSize = 0;
//
//    public DataAnalysis() throws IOException {
//
//        /* Read collection */
//        br = new BufferedReader(new InputStreamReader(new FileInputStream(COLLECTION_PATH), StandardCharsets.UTF_8));
//
//    }
//
//    public void runAnalysis() throws IOException {
//        String record = br.readLine();
//
//        System.out.println("\n*** DATA ANALYSIS ***\n");
//
//        int i = 0;
//        int empty = 0;
//        int maxTerms = 0;
//
//
//        while(record != null) {
//            ArrayList<String> preprocessed = TextProcessor.preprocessText(record);
//
//            // if is empty
//            if(preprocessed.isEmpty()) {
//                empty++;
//            }
//            else if(i < 10) //if isn't empty
//            {
//                System.out.println("Original record: " + record);
//                System.out.println("Preprocessed record:" + preprocessed);
//            }
//            maxTerms = Math.max(preprocessed.size(), maxTerms);
//            maxTermSize = findMaxWordLength(preprocessed);
//            i++;
//            record = br.readLine();
//        }
//        System.out.println("\n--------------------------------------------");
//        System.out.println("Total number of records: " + i);
//        System.out.println("Max terms per record: " + maxTerms);
//        System.out.println("Longest term size: " + maxTermSize);
//        System.out.println("Total empty records: " + empty);
//        System.out.println("--------------------------------------------");
//
//    }
//
//    public static int findMaxWordLength(ArrayList<String> strings) {
//
//        for (String str : strings) {
//            if (str.length() > maxTermSize)
//                System.out.println(str);
//            maxTermSize = Math.max(str.length(), maxTermSize);
//        }
//
//        return maxTermSize;
//    }
//}
