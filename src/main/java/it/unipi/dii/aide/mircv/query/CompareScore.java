package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.query.DAATBlock;

import java.util.Comparator;

/**
 * class to compare the block, allows the order of the priority queue
 */
public class CompareScore implements Comparator<DAATBlock> {
    @Override
    public int compare(DAATBlock pb1, DAATBlock pb2) {
        // comparing terms
        int DocIDComparison = Integer.compare(pb1.getDocId(), pb2.getDocId());
        // if the DocID are equal, compare by block number
        if (DocIDComparison == 0) {
            return Double.compare(pb2.getScore(), pb1.getScore());
        }

        return DocIDComparison;
    }
}
