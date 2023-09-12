//package it.unipi.dii.aide.mircv.data_structures;
//
//import java.util.ArrayList;
//
///**
// * Stores {term - document statistics} in posting lists
// */
//public class PostingList {
//
//    private String term;        // term associated with the posting ArrayList
//    private ArrayList<Posting> postings;        // ArrayList of postings with DocID and TermFreq for each doc in which term is present
//
//    public PostingList(){
//        this.term = "";
//        this.postings = new ArrayList<>();
//    }
//
//    public PostingList(String term) {
//        this.term = term;
//        this.postings = new ArrayList<>();
//    }
//
//    public void addPosting(Posting posting) {
//        postings.add(posting);
//    }
//
//    public void extend (PostingList pl) {
//        for (Posting p : pl.getPostings()) {
//            addPosting(p);
//        }
//    }
//
//    // ---- start method get and set ----
//
//    public String getTerm() {
//        return term;
//    }
//
//    public void setTerm(String term) {
//        this.term = term;
//    }
//
//    public ArrayList<Posting> getPostings() {
//        return postings;
//    }
//
//    public void setPostings(ArrayList<Posting> postings) {
//        this.postings = postings;
//    }
//
//    @Override
//    public String toString() {
//        StringBuilder list = new StringBuilder();
//        for (Posting p : postings) {
//            list.append(p).append(", ");
//        }
//        return "PostingList{" +
//                "term='" + term + '\'' +
//                ", postings=[" + list + "]" +
//                '}';
//    }
//
//}
