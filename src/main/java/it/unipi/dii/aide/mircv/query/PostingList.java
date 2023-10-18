package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.util.ArrayList;
import java.util.Iterator;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;

public class PostingList {

    public String term;

    public ArrayList<Posting> list;
    public Iterator<Posting> postingIterator;
    public Posting currPosting;
    public final SkipList sl;     // null if no skipping for that term

    int docIdSize;
    int termFreqSize;

    public PostingList (String term, ArrayList<Posting> list, SkipList sl, int termFreqSize, int docIdSize) {
        this.term = term;
        this.list = list;
        this.postingIterator = list.iterator();
        this.sl = sl;
        this.currPosting = postingIterator.next();
        this.docIdSize = docIdSize;
        this.termFreqSize = termFreqSize;
    }

    public PostingList() {
        this.sl = null;
    }

    //moves sequentially the iterator to the next posting
    public boolean next() {
        if (!postingIterator.hasNext()) {
            if (sl != null && currPosting.getDocId() == sl.getCurrSkipInfo().getMaxDocId() && sl.getSkipInfoIterator().hasNext()) {
                list.clear();
                sl.next();
                SkipInfo si = sl.getCurrSkipInfo();
                if (Flags.isCompressionEnabled())
                    list.addAll(readCompressedPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), termFreqSize, docIdSize, sl.getArr_skipInfo().size()));
                else
                    list.addAll(readPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), sl.getArr_skipInfo().size()));
                postingIterator = list.iterator();
            } else {
                currPosting = null;
                return false;
            }
        }

        currPosting = postingIterator.next();
        return true;
    }


    // advances the iterator forward to the next posting with a document identifier greater than or equal to
    // d ⇒ skipping
    public boolean nextGEQ (int targetDocId) {

        if (sl.getCurrSkipInfo() == null) {
            currPosting = null;
            return false;
        }

        // if not in the right block
        while (sl.getCurrSkipInfo().getMaxDocId() < targetDocId) // search right block
            if(!sl.next()) {
                currPosting = null;
                return false;
            }

        list.clear();
        SkipInfo si = sl.getCurrSkipInfo();

        if(Flags.isCompressionEnabled())
            list = readCompressedPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), termFreqSize, docIdSize, sl.getArr_skipInfo().size());
        else
            list = readPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), sl.getArr_skipInfo().size());

        postingIterator = list.iterator();
        currPosting = postingIterator.next();

        return true;
    }

    public Posting getCurrPosting() {
        return currPosting;
    }
    public void setCurrPosting(Posting p){ this.currPosting = p;}
}
