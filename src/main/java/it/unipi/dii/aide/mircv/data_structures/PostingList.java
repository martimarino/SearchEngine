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


    public PostingList(){
        this.term = "";
        this.postings = new ArrayList<>();
    }
    public PostingList(String term, ArrayList<Posting> postings) {
        this.term = term;
        this.postings = postings;
    }


    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public ArrayList<Posting> getPostings() {
        return postings;
    }

    public void setPostings(ArrayList<Posting> postings) {
        this.postings = postings;
    }
}
