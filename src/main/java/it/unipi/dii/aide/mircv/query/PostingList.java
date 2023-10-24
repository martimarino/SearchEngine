package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.query.Query.dictionary;

public class PostingList {

    public String term;

    private ArrayList<Posting> list = null;
    private Iterator<Posting> postingIterator;
    private Posting currPosting;
    private SkipList sl = null;     // null if no skipping for that term
    private double idf = 0;
    private double maxScore = 0;

    private int docIdSize;
    private int termFreqSize;
    private int len = 0;

    public PostingList(String term) throws IOException {
        this.term = term;

        DictionaryElem de = dictionary.getTermStat(term);

        if (de == null) {
            System.out.println("Term " + term + " not present in dictionary");
            return;
        }

        len = de.getDf();

        this.docIdSize = de.getDocIdSize();
        this.termFreqSize = de.getTermFreqSize();

        if (de.getSkipArrLen() > 0) {   // if there are skipping blocks read partial postings of the first block
            sl = new SkipList(de.getSkipOffset(), de.getSkipArrLen());
            SkipInfo skipInfo = sl.getCurrSkipInfo();

            if (Flags.isCompressionEnabled())
                list = readCompressedPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getTermFreqSize(), de.getDocIdSize(), postingsToRead());
            else
                list = readPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), postingsToRead());

        } else {    // read all postings
            if (Flags.isCompressionEnabled())
                list = readCompressedPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getTermFreqSize(), de.getDocIdSize(), de.getDf());
            else
                list = readPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getDf());
        }

        assert list != null;
        this.postingIterator = list.iterator();
        this.currPosting = postingIterator.next();
    }


    public int postingsToRead(){
        int fullSkipLen = (int) Math.ceil(Math.sqrt(len));
        if(sl.getSkipInfoIterator().hasNext())
            return fullSkipLen;
        else
            return len-(fullSkipLen*(sl.getArr_skipInfo().size()-1));
    }

    //moves sequentially the iterator to the next posting
    public void next(boolean firstPL) {

        if (postingIterator.hasNext()) {
            currPosting = postingIterator.next();
        } else {
            // if there are other blocks
            if (sl != null && sl.getSkipInfoIterator().hasNext() && firstPL) {
                sl.next();
                SkipInfo si = sl.getCurrSkipInfo();
                list.clear();
                if (Flags.isCompressionEnabled())
                    list.addAll(readCompressedPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), termFreqSize, docIdSize, postingsToRead()));
                else
                    list.addAll(readPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), postingsToRead()));
                postingIterator = list.iterator();
                currPosting = postingIterator.next();
            } else {
                currPosting = null;
            }
        }
    }

    // advances the iterator forward to the next posting with a document identifier greater than or equal to
    // d ⇒ skipping
    public void nextGEQ(int targetDocId, boolean firstPL) {

        assert sl != null;
        if (sl.getCurrSkipInfo() == null) {
            currPosting = null;
            return;
        }

        // if not in the right block
        while (sl.getCurrSkipInfo().getMaxDocId() < targetDocId) { // search right block
            if (!sl.next()) {
                currPosting = null;
                return;
            }
        }

        list.clear();
        SkipInfo si = sl.getCurrSkipInfo();

        if (Flags.isCompressionEnabled())
            list = readCompressedPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), termFreqSize, docIdSize, postingsToRead());
        else
            list = readPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), postingsToRead());

        assert list != null;
        postingIterator = list.iterator();
        currPosting = postingIterator.next();

        if(currPosting != null)
            while(postingIterator.hasNext() && (currPosting.getDocId() < targetDocId))
                next(firstPL);
    }


    public Posting getCurrPosting() {
        return currPosting;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public ArrayList<Posting> getList() {
        return list;
    }

    public void setList(ArrayList<Posting> list) {
        this.list = list;
    }

    public Iterator<Posting> getPostingIterator() {
        return postingIterator;
    }

    public SkipList getSl() {
        return sl;
    }

    public int getDocIdSize() {
        return docIdSize;
    }

    public void setDocIdSize(int docIdSize) {
        this.docIdSize = docIdSize;
    }

    public int getTermFreqSize() {
        return termFreqSize;
    }

    public void setTermFreqSize(int termFreqSize) {
        this.termFreqSize = termFreqSize;
    }

    public int getLen() {
        return len;
    }

    @Override
    public String toString() {
        return "PostingList{" +
                "list=" + list +
                '}';
    }

    public double getIdf() {
        return idf;
    }

    public void setIdf(double idf) {
        this.idf = idf;
    }

    public double getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(double maxScore) {
        this.maxScore = maxScore;
    }
}
