package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.*;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;

public class PostingList {

    public ArrayList<Posting> list;
    public Iterator<Posting> postingIterator;

    public Posting currPosting;

    public SkipList sl;     // null if no skipping for that term

    public PostingList(ArrayList<Posting> list, SkipList sl) {
        this.list = list;
        this.postingIterator = list.iterator();
        this.sl = sl;
    }

    public PostingList() {
        this.sl = null;
    }

    //moves sequentially the iterator to the next posting
    public boolean next (DictionaryElem de) {

        // se ho finito il blocco carico il nuovo
        if (sl != null && currPosting.getDocId() == sl.getCurrSkipInfo().getMaxDocId() && sl.getSkipInfoIterator().hasNext()) {

            list.clear();
            sl.next();

            SkipInfo si = sl.getCurrSkipInfo();

            if(Flags.isCompressionEnabled())
                list = readCompressedPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), de.getTermFreqSize(), de.getDocIdSize(), de.getSkipArrLen());
            else
                list = readPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), de.getSkipArrLen());

        }
        // leggo il primo elemento del
        postingIterator = list.iterator();
        if (!postingIterator.hasNext())
            return false;
        currPosting = postingIterator.next();
        return true;
    }

    // advances the iterator forward to the next posting with a document identifier greater than or equal to
    // d ⇒ skipping
    public boolean nextGEQ (int docId, DictionaryElem de) {

        // if not in the right block
        if(sl.getCurrSkipInfo().getMaxDocId() < docId) {
            while (sl.getCurrSkipInfo().getMaxDocId() < docId)
                if(!sl.next())
                    currPosting = null;
        }
        if(currPosting == null)
            return false;

        list.clear();
        SkipInfo si = sl.getCurrSkipInfo();

        if(Flags.isCompressionEnabled())
            list = readCompressedPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), de.getTermFreqSize(), de.getDocIdSize(), de.getSkipArrLen());
        else
            list = readPostingListFromDisk(si.getDocIdOffset(), si.getFreqOffset(), de.getSkipArrLen());

        postingIterator = list.iterator();
        currPosting = postingIterator.next();

        return true;
    }

    public Posting getCurrPosting() {
        return currPosting;
    }
    public void setCurrPosting(Posting p){ this.currPosting = p;}
}
