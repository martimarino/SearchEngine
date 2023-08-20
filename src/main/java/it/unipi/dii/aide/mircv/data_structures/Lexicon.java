
package it.unipi.dii.aide.mircv.data_structures;

import java.util.HashMap;

public class Lexicon {
    // HashMap between a term and its statistics, contained in a LexiconElem.
    private HashMap<String, LexiconElem> termToTermStat;

    // constructor without parameters
    public Lexicon() {
        this.termToTermStat = new HashMap<>();
    }

//    public void addTerm(String term, int termId) {
//        if(termToTermStat.containsKey(term))
//            incCf(term);
//        else {
//            System.out.println("ELSE");
//            LexiconElem le = new LexiconElem(1, 1, termId);
//            termToTermStat.put(term, le);
//        }
//    }
    /**
     * Function which returns, if present, the LexiconElem associated with the term passed as a parameter.
     * Otherwise, it creates a new LexiconElem associated with the term, inserts it in the HashMap and returns it.
     */
    public LexiconElem getOrCreateTerm(String term, int termCounter) {
        return termToTermStat.computeIfAbsent(term, t -> new LexiconElem(termCounter, term));
    }

    /**
     * function which returns, if present, the LexiconElem associated with the term passed as a parameter.
     * Otherwise, returns null.
     */
    public LexiconElem getTermStat(String term) {
        return termToTermStat.getOrDefault(term, null);
    }

//    public void incDf(String term) {
//        termToTermStat.replace(term, termToTermStat.get(term).incDf());
//    }
//
//    public void incCf(String term){
//        termToTermStat.replace(term, termToTermStat.get(term).incCf());
//    }
    // ---- start method get and set ----
    public HashMap<String, LexiconElem> getTermToTermStat() {
        return termToTermStat;
    }

    public void setTermToTermStat(HashMap<String, LexiconElem> termToTermStat) {
        this.termToTermStat = termToTermStat;
    }
}
