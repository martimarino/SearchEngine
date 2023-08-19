package it.unipi.dii.aide.mircv.data_structures;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Stores term-document statistics (in posting lists)
 */

public class InvertedIndex {
    private HashMap<String, PostingList> invertedIndex;

    public InvertedIndex() {
        this.invertedIndex = new HashMap<String, PostingList>();
    }

    /**
     * Add a term and its associated posting to the inverted index.
     *
     * @param term  The term to be added.
     * @param docId The document ID associated with the term.
     * @return incDf If true the term is not in the document so need to increment df in lexicon, otherwise no need to increment it
     */
    public boolean addTerm(String term, int docId, int tf) {
         //arraylist<Posting> version
        /*int termFreq = 1;
        boolean incDf = false;                  // default value is false
        // add or update posting list of the term
        if (!invertedIndex.containsKey(term))   // there isn't the term in hash table
        {

            invertedIndex.put(term, new ArrayList<>());
            invertedIndex.get(term).put(term, new Posting(docId, termFreq));
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
        return incDf;*/

        // instead of ArrayList<Posting> we can use directly PostingList
        int termFreq = 1;
        boolean incDf = false;                  // default value is false
        //tf is 0 when building index, otherwise in case of get index
        if(tf != 0)
            termFreq = tf;
        // add or update posting list of the term
        if (!invertedIndex.containsKey(term))   // there isn't the term in hash table
        {
            invertedIndex.put(term, new PostingList(term, new ArrayList<Posting>()));
            invertedIndex.get(term).getPostings().add(new Posting(docId, termFreq));
        }
        else                                    // there is the term in hash table
        {
            ArrayList<Posting> posting = invertedIndex.get(term).getPostings();
            if(posting.get(posting.size()-1).getDocId() != docId)      //there isn't a posting element with this docID
            {
                invertedIndex.get(term).getPostings().add(new Posting(docId, termFreq));
                incDf = true; // term in a new doc -> update df
                ArrayList<Posting> pos = invertedIndex.get(term).getPostings();
            }
            else            // there is a posting element with this DocID
            {
                termFreq = invertedIndex.get(term).getPostings().get(posting.size()-1).getTermFreq() + 1;
                invertedIndex.get(term).getPostings().get(posting.size()-1).setTermFreq(termFreq);
            }
            if(tf != 0)
                System.out.println("TF: " + tf + "TERMFREQ: " + invertedIndex.get(term).getPostings().get(posting.size()-1).getTermFreq());
        }

        return incDf;
    }

    /**
     * Get the postings associated with a given term.
     *
     * @param term The term to retrieve postings for.
     * @return The list of postings associated with the term.
     */
/*    public List<Posting> getPostings(String term) {
        return invertedIndex.getOrDefault(term, new ArrayList<>());
    }*/

    public PostingList getPostings(String term) {
        return invertedIndex.getOrDefault(term, new PostingList());
    }

    public void sort(){
        invertedIndex = getInvertedIndex().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> { throw new AssertionError(); },
                        LinkedHashMap::new
                ));
    }

    public HashMap<String, PostingList> getInvertedIndex() {
        return invertedIndex;
    }

    public void setInvertedIndex(HashMap<String, PostingList> invertedIndex) {
        this.invertedIndex = invertedIndex;
    }
}