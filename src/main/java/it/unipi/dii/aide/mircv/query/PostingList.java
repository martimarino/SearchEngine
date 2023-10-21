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
    private SkipList sl;     // null if no skipping for that term

    private int docIdSize;
    private int termFreqSize;
    private int len = 0;

    public PostingList(String term) {
        this.term = term;
        this.sl = null;
    }
    public PostingList(String term, ArrayList<Posting> p) {
        this.term = term;
        this.sl = null;
        this.list = p;
    }

    public void load() throws IOException {

        DictionaryElem de = dictionary.getTermStat(term);

        if (de == null) {
            System.out.println("Term " + term + " not present in dictionary");
            return;
        }

        this.docIdSize = de.getDocIdSize();
        this.termFreqSize = de.getTermFreqSize();

        if (de.getSkipArrLen() > 0) {   // if there are skipping blocks read partial postings of the first block
            sl = new SkipList(de.getSkipOffset(), de.getSkipArrLen());
            SkipInfo skipInfo = sl.getCurrSkipInfo();

            if (Flags.isCompressionEnabled())
                list = readCompressedPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getTermFreqSize(), de.getDocIdSize(), de.getSkipArrLen());
            else
                list = readPostingListFromDisk(skipInfo.getDocIdOffset(), skipInfo.getFreqOffset(), de.getSkipArrLen());

        } else {    // read all postings
            if (Flags.isCompressionEnabled())
                list = readCompressedPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getTermFreqSize(), de.getDocIdSize(), de.getDf());
            else
                list = readPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getDf());
        }

        assert list != null;
        this.postingIterator = list.iterator();
        this.currPosting = postingIterator.next();

        len = de.getDf();

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
                    list.addAll(readCompressedPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), termFreqSize, docIdSize, sl.getArr_skipInfo().size()));
                else
                    list.addAll(readPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), sl.getArr_skipInfo().size()));
                postingIterator = list.iterator();
                currPosting = postingIterator.next();
            } else {
                currPosting = null;
            }
        }
    }


    // advances the iterator forward to the next posting with a document identifier greater than or equal to
    // d ⇒ skipping
    public void nextGEQ(int targetDocId) {

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
            list = readCompressedPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), termFreqSize, docIdSize, sl.getArr_skipInfo().size());
        else
            list = readPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), sl.getArr_skipInfo().size());

        assert list != null;
        postingIterator = list.iterator();
        currPosting = postingIterator.next();

        if(currPosting != null)
            while(postingIterator.hasNext() && (currPosting.getDocId() < targetDocId))
                next(false);
    }


    public Posting getCurrPosting() {
        return currPosting;
    }
    public void setCurrPosting(Posting p){ this.currPosting = p;}


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

}
