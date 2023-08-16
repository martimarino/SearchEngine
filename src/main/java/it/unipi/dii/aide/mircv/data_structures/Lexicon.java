
package it.unipi.dii.aide.mircv.data_structures;

import java.util.HashMap;

public class Lexicon {

    private HashMap<String, LexiconElem> termToTermStat;

    public Lexicon() {
        this.termToTermStat = new HashMap<>();
    }

    public void addTerm(String term, int termId) {
        if(termToTermStat.containsKey(term))
            incCf(term);
        else {
            LexiconElem le = new LexiconElem(1, 1, termId);
            termToTermStat.put(term, le);
        }
    }

    public LexiconElem getTermStat(String term) {
        return termToTermStat.getOrDefault(term, null);
    }

    public void incDf(String term) {
        termToTermStat.replace(term, termToTermStat.get(term).incDf());
    }

    public void incCf(String term){
        termToTermStat.replace(term, termToTermStat.get(term).incCf());
    }

    public HashMap<String, LexiconElem> getTermToTermStat() {
        return termToTermStat;
    }

    public void setTermToTermStat(HashMap<String, LexiconElem> termToTermStat) {
        this.termToTermStat = termToTermStat;
    }
}
