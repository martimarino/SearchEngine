package it.unipi.dii.aide.mircv.query.algorithms;
import it.unipi.dii.aide.mircv.query.*;

import java.util.*;

import static it.unipi.dii.aide.mircv.query.Query.*;


public class MaxScore {

    /**
     * Performs the MaxScore algorithm
     **/
    public static PriorityQueue<ResultBlock> computeMaxScore() {

        PriorityQueue<ResultBlock> resultQueue = new PriorityQueue<>(k,new ResultBlock.CompareResInc());
        ArrayList<Double> ub = new ArrayList<>();

        int n;
        double threshold = 0;
        double score;
        int next;
        int pivot = 0;

        n = ordered_PostingLists.size();

        //compute the upperbound for each query term
        ub.add(0, ordered_PostingLists.get(0).getMaxScore());

        for(int i = 1; i < n; i++)
        {
            ub.add(i, ub.get(i-1) + ordered_PostingLists.get(i).getMaxScore()); // each uppebound is given by the maxscore of the previous ones
        }

        int current = minDocID(ordered_PostingLists);

        while(pivot < n && current != -1){
            score = 0;
            next = Integer.MAX_VALUE;
            //essential postings
            for(int i = pivot; i < n; i++)
            {
                //if the i-th posting list has no scanned postings and the docid of the current posting is the current one to consider, the score is updated
                if( (ordered_PostingLists.get(i).getCurrPosting() != null) && ordered_PostingLists.get(i).getCurrPosting().getDocId() == current)
                {
                    score = score + computeScore(ordered_PostingLists.get(i).getIdf(), ordered_PostingLists.get(i).getCurrPosting());
                    ordered_PostingLists.get(i).next(true); // get the next posting in the posting list, if posting list ended the current posting is set to null

                    if(ordered_PostingLists.get(i).getCurrPosting() == null)
                        continue;
                }
                // next is the next docid of the current postings among all the posting lists
                if((ordered_PostingLists.get(i).getCurrPosting() != null) && (ordered_PostingLists.get(i).getCurrPosting().getDocId() < next))
                    next = ordered_PostingLists.get(i).getCurrPosting().getDocId();
            }
            //non essential postings
            for(int i = pivot - 1; i > 0; i--)
            {
                //if the score plus the upper bound of the non-essential posting list is below the threshold, the non-essential posting list(and the lower too) is not considered
                if(score + ub.get(i) <= threshold)
                    break;

                //it is peak as current element of the posting list the one with docid >= current
                if(ordered_PostingLists.get(i).getSl() != null)
                    ordered_PostingLists.get(i).nextGEQ(current, true);

                if(ordered_PostingLists.get(i).getCurrPosting() == null)
                    continue;

                if((ordered_PostingLists.get(i).getCurrPosting() != null) && (ordered_PostingLists.get(i).getCurrPosting().getDocId() == current))
                    score = score + computeScore(ordered_PostingLists.get(i).getIdf(), ordered_PostingLists.get(i).getCurrPosting());
            }

            //if the queue has not reached the maximum capacity k, every pair docid,score is put inside the queue
            if(resultQueue.size() < k) {
                resultQueue.add(new ResultBlock(current, score));
                threshold = resultQueue.peek().getScore(); // threshold updated as the lower score element of the result queue
            }
            else if(resultQueue.peek().getScore() < score) { // if maximum capacity has been reached, if the new document has an higher score than the last one in the queue
                resultQueue.poll();  // removed element with lower score from the queue
                resultQueue.add(new ResultBlock(current, score)); //new element with higher score inserted in the queue
                threshold = resultQueue.peek().getScore();
            }

            while (pivot < n && ub.get(pivot) <= threshold)
                pivot += 1;

            current = next;

            //no more posting list to read (all current postings are null)
            if (next == Integer.MAX_VALUE)
                current = -1;
        }

        return resultQueue;
    }

}


