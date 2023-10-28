package it.unipi.dii.aide.mircv.query.algorithms;
import it.unipi.dii.aide.mircv.query.*;

import java.io.IOException;
import java.util.*;

import static it.unipi.dii.aide.mircv.query.Query.*;


public class MaxScore {

/*
    private static  PriorityQueue<ResultBlock> resultQueue; //stores the results in increasing order
    private static ArrayList<PostingList> postingLists = new ArrayList<>(); //contains all the posting lists read from disk
    private static ArrayList<Double> idf = new ArrayList<>(); // contains all the idf of each query term
    public static ArrayList<Double> orderedIdf = new ArrayList<>(); //idf ordered for increasing score

    private static ArrayList<Double> ub = new ArrayList<>(); //contains all the max scores (upper bound) of each query term
    public static ArrayList<PostingList> p = new ArrayList<>(); // ordered posting lists for increasing score
*/

    //private static PriorityQueue<ScoreElem> orderByScore; //index, score pairs ordered in increasing order
    /**
     * Performs the MaxScore algorithm
     **/
    public static PriorityQueue<ResultBlock> computeMaxScore() throws IOException {

        PriorityQueue<ResultBlock> resultQueue = new PriorityQueue<>(k,new ResultBlock.CompareRes());
        ArrayList<Double> ub = new ArrayList<>();

        int n;
        double threshold = 0;
        double score;
        int next;
        int pivot = 0;

        n = p.size();

        //compute the upperbound for each query term
        ub.add(0, p.get(0).getMaxScore());

        for(int i = 1; i < n; i++)
        {
            ub.add(i, ub.get(i-1) + p.get(i).getMaxScore()); // each uppebound is given by the maxscore of the previous ones
        }

        int current = minDocID(p);

        while(pivot < n && current != -1){
            score = 0;
            next = Integer.MAX_VALUE;
            //essential postings
            for(int i = pivot; i < n; i++)
            {
                //if the i-th posting list has no scanned postings and the docid of the current posting is the current one to consider, the score is updated
                if( (p.get(i).getCurrPosting() != null) && p.get(i).getCurrPosting().getDocId() == current)
                {
                    score = score + computeScore(p.get(i).getIdf(), p.get(i).getCurrPosting());
                    p.get(i).next(true); // get the next posting in the posting list, if posting list ended the current posting is set to null

                    if(p.get(i).getCurrPosting() == null)
                        continue;
                }
                // next is the next docid of the current postings among all the posting lists
                if((p.get(i).getCurrPosting() != null) && p.get(i).getCurrPosting().getDocId() < next)
                    next = p.get(i).getCurrPosting().getDocId();
            }
            //non essential postings
            for(int i = pivot - 1; i > 0; i--)
            {
                //if the score plus the upper bound of the non-essential posting list is below the threshold, the non-essential posting list(and the lower too) is not considered
                if(score + ub.get(i) <= threshold)
                    break;

                //it is peak as current element of the posting list the one with docid >= current
                if(p.get(i).getSl() != null)
                    p.get(i).nextGEQ(current, true);

                if(p.get(i).getCurrPosting() == null)
                    continue;

                if((p.get(i).getCurrPosting() != null) && (p.get(i).getCurrPosting().getDocId() == current))
                    score = score + computeScore(p.get(i).getIdf(), p.get(i).getCurrPosting());
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


