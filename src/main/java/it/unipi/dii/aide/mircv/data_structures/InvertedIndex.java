//package it.unipi.dii.aide.mircv.data_structures;
//
//import java.util.*;
//import java.util.stream.Collectors;
//
//import static it.unipi.dii.aide.mircv.utils.Constants.verbose;
//
///**
// * Stores term - document statistics (in posting lists)
// */
//public class InvertedIndex {
//
//    private HashMap<String, DictionaryElemPostings> invertedIndex;
//    // HashMap<String, ArrayList<Posting>> invertedIndex;
//
//    public InvertedIndex() {
//        this.invertedIndex = new HashMap<>();
//    }
//
//    /**
//     * Add a term and its associated posting to the inverted index.
//     *
//     * @param term  The term to be added.
//     * @param docId The document ID associated with the term.
//     * @param tf    The term frequency (tf) in the document.
//     * @return incDf If true, the term is not in the document, so df needs to be incremented in the dictionary;
//     * otherwise, no need to increment it.
//     */
//    public boolean addTerm(String term, int docId, int tf) {
//        // Initialize term frequency to 1 if tf is not provided (tf = 0 during index construction)
//        int termFreq = (tf != 0) ? tf : 1;
//
//        // Get or create the DictionaryElemPostings associated with the term
//        DictionaryElemPostings postingList = invertedIndex.computeIfAbsent(term, key -> new DictionaryElemPostings(key, new ArrayList<>()));
//        List<Posting> postings = postingList.getPostings();
//
//        // Check if the posting list is empty or if the last posting is for a different document
//        if (postings.isEmpty() || postings.get(postings.size() - 1).getDocId() != docId) {
//            // Add a new posting for the current document
//            postings.add(new Posting(docId, termFreq));
//
//            // Print term frequency and term frequency in the current posting (only during index construction)
//            if (tf != 0 && verbose) {
//                System.out.println("TF: " + tf + " TERMFREQ: " + termFreq);
//            }
//
//            return true; // Increment df only if it's a new document
//        } else {
//            // Increment the term frequency for the current document
//            int lastTermFreq = postings.get(postings.size() - 1).getTermFreq();
//            postings.get(postings.size() - 1).setTermFreq(lastTermFreq + 1);
//            return false; // No need to increment df
//        }
//    }
//
//
//    /**
//     * Get the postings associated with a given term.
//     *
//     * @param term The term to retrieve postings for.
//     * @return The list of postings associated with the term.
//     */
//
//    public void addPosting(String term, Posting posting) {
//        // Get or create the DictionaryElemPostings associated with the term
//        DictionaryElemPostings postingList = invertedIndex.computeIfAbsent(term, key -> new DictionaryElemPostings(key, new ArrayList<>()));
//        postingList.addPosting(posting);
//    }
//
//    public DictionaryElemPostings getPostings(String term) {
//        return invertedIndex.getOrDefault(term, new DictionaryElemPostings());
//    }
//
//    public void sort(){
//        invertedIndex = getInvertedIndex().entrySet().stream()
//                .sorted(Map.Entry.comparingByKey())
//                .collect(Collectors.toMap(
//                        Map.Entry::getKey,
//                        Map.Entry::getValue,
//                        (a, b) -> { throw new AssertionError(); },
//                        LinkedHashMap::new
//                ));
//    }
//
//    public HashMap<String, DictionaryElemPostings> getInvertedIndex() {
//        return invertedIndex;
//    }
//
//    public void setInvertedIndex(HashMap<String, DictionaryElemPostings> invertedIndex) {
//        this.invertedIndex = invertedIndex;
//    }
//
//}