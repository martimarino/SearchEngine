package it.unipi.dii.aide.mircv.data_structures;

import java.util.ArrayList;

/**
 * Stores term-document statistics (in posting lists)
 */
public class PostingList {
    // term associated with the posting ArrayList
    private String term;
    // ArrayList of postings with DocID and TermFrequency for each document in which term is present
    private ArrayList<Posting> postings;
}
