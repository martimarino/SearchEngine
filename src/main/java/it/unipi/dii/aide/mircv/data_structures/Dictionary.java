
package it.unipi.dii.aide.mircv.data_structures;

import java.util.HashMap;

public class Dictionary {

    // HashMap between a term and its statistics, contained in a DictionaryElem.
    private HashMap<String, DictionaryElem> termToTermStat;

    // constructor without parameters
    public Dictionary() {
        this.termToTermStat = new HashMap<>();
    }

//    public void addTerm(String term, int termId) {
//        if(termToTermStat.containsKey(term))
//            incCf(term);
//        else {
//            System.out.println("ELSE");
//            DictionaryElem le = new DictionaryElem(1, 1, termId);
//            termToTermStat.put(term, le);
//        }
//    }

    /**
     * Function which returns, if present, the DictionaryElem associated with the term passed as a parameter.
     * Otherwise, it creates a new DictionaryElem associated with the term, inserts it in the HashMap and returns it.
     */
    public DictionaryElem getOrCreateTerm(String term, int termCounter) {
        return termToTermStat.computeIfAbsent(term, t -> new DictionaryElem(termCounter, term));
    }

    public DictionaryElem getTermStat(String term) {
        return termToTermStat.getOrDefault(term, null);
    }

    public HashMap<String, DictionaryElem> getTermToTermStat() {
        return termToTermStat;
    }

}
