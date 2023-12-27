package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.utils.FileSystem.docId_channel;
import static it.unipi.dii.aide.mircv.utils.FileSystem.termFreq_channel;

public class PostingList {

    public String term;

    private ArrayList<Posting> list;
    private Iterator<Posting> postingIterator;
    private Posting currPosting;
    private SkipList sl = null;     // null if no skipping for that term
    private double idf;
    private double maxScore = 0;
    private final int len;

    public PostingList (DictionaryElem de) throws IOException {

        this.term = de.getTerm();
        len = de.getDf();

        if (de.getSkipListLen() > 0) {   // if there are skipping blocks read partial postings of the first block
            sl = new SkipList(de.getSkipListOffset(), de.getSkipListLen());
            SkipElem skipElem = sl.getCurrSkipElem();

            if (Flags.isCompressionEnabled())
                list = readCompressedPostingListFromDisk(skipElem.getDocIdOffset(), skipElem.getFreqOffset(), skipElem.getTermFreqBlockLen(), skipElem.getDocIdBlockLen());
            else
                list = readPostingListFromDisk(skipElem.getDocIdOffset(), skipElem.getFreqOffset(), skipElem.getDocIdBlockLen(), docId_channel, termFreq_channel);
        } else {    // read all postings

            if (Flags.isCompressionEnabled())
                list = readCompressedPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getTermFreqSize(), de.getDocIdSize());
            else
                list = readPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getDf(), docId_channel, termFreq_channel);
        }
        assert list != null;
        this.postingIterator = list.iterator();
        this.currPosting = postingIterator.next();
        this.idf = de.getIdf();
    }

    //moves sequentially the iterator to the next posting
    public void next(boolean firstPL) {

        if (postingIterator.hasNext()) {
            currPosting = postingIterator.next();
        } else {
            // if there are other blocks
            if (sl != null && sl.getSkipElemIterator().hasNext() && firstPL) {
                sl.next();
                SkipElem si = sl.getCurrSkipElem();
                list.clear();
                if (Flags.isCompressionEnabled())
                    list.addAll(readCompressedPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), si.getTermFreqBlockLen(), si.getDocIdBlockLen()));
                else
                    list.addAll(readPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), si.getDocIdBlockLen(), docId_channel, termFreq_channel));
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

        boolean isNew = false;

        assert sl != null;
        if (sl.getCurrSkipElem() == null) {
            currPosting = null;
            return;
        }

        // if not in the right block
        while (sl.getCurrSkipElem().getMaxDocId() < targetDocId) { // search right block
            if (!sl.next()) {
                currPosting = null;
                return;
            }
            isNew = true;
        }
        if(isNew)
        {
            list.clear();
            SkipElem si = sl.getCurrSkipElem();

            if (Flags.isCompressionEnabled())
                list = readCompressedPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), si.getTermFreqBlockLen(), si.getDocIdBlockLen());
            else
                list = readPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), si.getDocIdBlockLen(), docId_channel, termFreq_channel);

            assert list != null;
            postingIterator = list.iterator();
            currPosting = postingIterator.next();
        }

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

    public SkipList getSl() {
        return sl;
    }

    public int getLen() {
        return len;
    }

    @Override
    public String toString() {
        return "PostingList{" +
                "term='" + term + '\'' +
                ", list=" + list +
                ", postingIterator=" + postingIterator +
                ", currPosting=" + currPosting +
                ", sl=" + sl +
                ", idf=" + idf +
                ", maxScore=" + maxScore +
                ", len=" + len +
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
