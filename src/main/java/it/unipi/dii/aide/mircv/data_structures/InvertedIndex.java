package it.unipi.dii.aide.mircv.data_structures;

import java.util.*;

/**
 * Stores term-document statistics (in posting lists)
 */

public class InvertedIndex {
    private HashMap<String, ArrayList<Posting>> invertedIndex;

    public InvertedIndex() {
        invertedIndex = new HashMap<>();
    }

    /**
     * Add a term and its associated posting to the inverted index.
     *
     * @param term  The term to be added.
     * @param docId The document ID associated with the term.
     */
    public void addTerm(String term, int docId) {
        int termFreq = 0;
        // add or update posting list of the term
        if (!invertedIndex.containsKey(term)) {
            invertedIndex.put(term, new ArrayList<>());
            termFreq = 1;
            //add new term in lexicon

        }
        else{
            termFreq = invertedIndex.get(term).get(docId).getTermFreq() + 1;
            //update df and cf in lexicon
        }
        // update inverted index
        ArrayList<Posting> postings = invertedIndex.get(term);
        postings.add(new Posting(docId,termFreq));
        // update lexicon elem

    }

    /**
     * Get the postings associated with a given term.
     *
     * @param term The term to retrieve postings for.
     * @return The list of postings associated with the term.
     */
    public List<Posting> getPostings(String term) {
        return invertedIndex.getOrDefault(term, new ArrayList<>());
    }
}