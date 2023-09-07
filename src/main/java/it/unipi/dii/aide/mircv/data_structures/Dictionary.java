
package it.unipi.dii.aide.mircv.data_structures;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Dictionary {

    // HashMap between a term and its statistics, contained in a DictionaryElem.
    private HashMap<String, DictionaryElem> termToTermStat;

    // constructor without parameters
    public Dictionary() {
        this.termToTermStat = new HashMap<>();
    }

    /**
     * Function which returns, if present, the DictionaryElem associated with the term passed as a parameter.
     * Otherwise, it creates a new DictionaryElem associated with the term, inserts it in the HashMap and returns it.
     */
    public DictionaryElem getOrCreateTerm(String term, int termCounter) {
        return termToTermStat.computeIfAbsent(term, t -> new DictionaryElem(termCounter, term));
    }

    public DictionaryElem getTermStat(String term) {
        return termToTermStat.get(term);
    }

    public HashMap<String, DictionaryElem> getTermToTermStat() {
        return termToTermStat;
    }

    public void sort(){
        termToTermStat = getTermToTermStat().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (a, b) -> { throw new AssertionError(); },
                        LinkedHashMap::new
                ));
    }
}
