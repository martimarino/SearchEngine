package it.unipi.dii.aide.mircv.data_structures;

/**
 *  Stores unique terms and their statistics
 */
import java.util.*;

public class LexiconElem {
    private int df;     // document frequency
    private int cf;     // collection frequency
//    private String term;
    private int termId;

    public LexiconElem() {
        this.df = 0;
        this.cf = 0;
        this.termId = 0;
    }

    public LexiconElem(int termId) {
        this.df = 1;
        this.cf = 1;
        this.termId = termId;
    }

    public void setDf(int df) { this.df = df; }

    public void setCf(int cf) { this.cf = cf; }

    //public void setTerm(String term) { this.term = term; }

    public void setTermId(int termId) { this.termId = termId; }

    public int getDf() { return df; }

    public int getCf() { return cf; }

    public int getTermId() { return termId; }

    public void incDf(){
        df++;
    }

    public void incCf(){
        cf++;
    }
}
