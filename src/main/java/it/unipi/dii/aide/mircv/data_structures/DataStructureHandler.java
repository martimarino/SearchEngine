package it.unipi.dii.aide.mircv.data_structures;

import it.unipi.dii.aide.mircv.TextProcessor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

public class DataStructureHandler {

    public static void getCollectionFromDisk() {

    }

    public static void getFlagsFromDisk() {

    }

    public static void getDocumentIndexFromDisk() {

    }

    public static void getDictionaryFromDisk() {

    }

    /**
     * function to create and fill the lexicon, document table and inverted index
     * @throws IOException
     */
    public void createStructures(String collection) throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(collection), StandardCharsets.UTF_8))) {
            // data structures init
            DocumentTable dt = new DocumentTable();
            Lexicon lexicon = new Lexicon();
            InvertedIndex invertedIndex = new InvertedIndex();

            System.out.println("\n*** Data structure build ***\n");

            int docCounter = 1;         // DocID of the current document
            int termCounter = 0;        // TermID of the current term

            String record;

            // scroll through the dataset documents
            while ((record = br.readLine()) != null) {
                ArrayList<String> preprocessed = TextProcessor.preprocessText(record);

                if (preprocessed.isEmpty()) {
                    continue; // Skip empty documents
                }

                String docno = preprocessed.remove(0);
                dt.setDocIdToDocElem(docno, docCounter, preprocessed.size());
                docCounter++;

                dt.setDocIdToDocElem(docno, docCounter, preprocessed.size()); // add element to document table
                docCounter++;              // update counter

                for (String term : preprocessed) {
                    // Lexicon build
                    LexiconElem lexElem = lexicon.getOrCreateTerm(term, termCounter);
                    termCounter++;

                    // Build inverted index
                    if (invertedIndex.addTerm(term, docCounter)) {
                        lexElem.incDf();
                    }
                    // test print for lexicon
                    if (termCounter < 10) {
                        HashMap<String, LexiconElem> lex = lexicon.getTermToTermStat();
                        System.out.println("********** Lexicon ********");
                        System.out.println("Term: " + term);
                        System.out.println("TermId: " + termCounter + " - TermId elem:" + lex.get(term).getTermId());
                        System.out.println("Df: " + lex.get(term).getDf());
                        System.out.println("Cf: " + lex.get(term).getCf());
                        System.out.println("Lexicon size: " + lex.size());
                    }
                }
                // test print for documentElement
                if (docCounter == 5354) {
                    HashMap<Integer, DocumentElement> doctable = dt.getDocIdToDocElem();
                    System.out.println("********** Document Table ********");
                    System.out.println("Docid: " + docCounter);
                    System.out.println("DocTable size: " + doctable.size());
                    System.out.println("Docno: " + doctable.get(docCounter - 1).getDocno());
                    System.out.println("Length: " + doctable.get(docCounter - 1).getDoclength());
                }
            }
        }
    }
}
