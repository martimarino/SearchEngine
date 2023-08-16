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
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(collection), StandardCharsets.UTF_8));
        String record = br.readLine();

        DocumentTable dt = new DocumentTable();
        Lexicon lexicon = new Lexicon();
        InvertedIndex invertedIndex = new InvertedIndex();

        System.out.println("\n*** Data structure build ***\n");
        int docCounter = 1;         // counter whose value will represent the DocID of the current document
        int termCounter = 0;        // counter whose value will represent the TermID of the current term
        // while to scroll through the dataset documents
        while(record != null) {
            ArrayList<String> preprocessed = TextProcessor.preprocessText(record);
            String docno = preprocessed.get(0);         // get the docno
            preprocessed.remove(0);
            if(preprocessed.isEmpty()) // if document is empty skip it
                continue;

            dt.setDocIdToDocElem(docno, docCounter, preprocessed.size()); // add element to document table
            docCounter++;              // update counter
            // check if the document is empty
            if(!preprocessed.isEmpty()){
                for(String term: preprocessed){
                    // Lexicon build
                    // control check
                    if(!lexicon.getTermToTermStat().containsKey(term)) {
                        System.out.println("CHECK: " + termCounter + " TERM: " + term);
                        termCounter++;
                    }
                    lexicon.addTerm(term, termCounter);
                    // Build inverted index
                    if(invertedIndex.addTerm(term, docCounter))
                        lexicon.incDf(term);         // Update df if first time term in this document

                    // test print for lexicon
                    if(termCounter < 10)
                    {
                        HashMap<String, LexiconElem> lex = lexicon.getTermToTermStat();
                        System.out.println("********** Lexicon ********");
                        System.out.println("Term: " + term);
                        System.out.println("TermId: " + termCounter + "TermId elem:" + lex.get(term).getTermId());
                        System.out.println("Df: " + lex.get(term).getDf());
                        System.out.println("Cf: " + lex.get(term).getCf());
                        System.out.println("Lexicon size: " + lex.size());
                    }

                }
            }
            // test print for documentElement
            if(docCounter == 5354)
            {
                HashMap<Integer, DocumentElement> doctable = dt.getDocIdToDocElem();
                System.out.println("********** Document Table ********");
                System.out.println("Docid: " + docCounter);

                System.out.println("DocTable size: " + doctable.size());
                System.out.println("Docno: " + doctable.get(docCounter-1).getDocno());
                System.out.println("Length: " + doctable.get(docCounter-1).getDoclength());
            }
            record = br.readLine();
        }
    }

}
