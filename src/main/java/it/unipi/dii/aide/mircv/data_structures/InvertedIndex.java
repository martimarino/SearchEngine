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
    public boolean addTerm(String term, int docId) {
        int termFreq = 1;
        boolean incDf = false;
        // add or update posting list of the term
        if (!invertedIndex.containsKey(term))   // there isn't the term in hash table
        {
            invertedIndex.put(term, new ArrayList<>());
            invertedIndex.get(term).add(new Posting(docId, termFreq));
        }
        else                                    // there is the term in hash table
        {
            if(!invertedIndex.get(term).contains(docId))      //there isn't a posting element with this docID
            {
                invertedIndex.get(term).add(new Posting(docId, termFreq));
                incDf = true; // term in a new doc -> update df
            }
            else            // there is a posting element with this DocID
            {
                termFreq = invertedIndex.get(term).get(docId).getTermFreq() + 1;
                invertedIndex.get(term).get(docId).setTermFreq(termFreq);
            }
        }
        return incDf;
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