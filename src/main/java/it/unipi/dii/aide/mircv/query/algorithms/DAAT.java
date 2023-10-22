package it.unipi.dii.aide.mircv.query.algorithms;
import it.unipi.dii.aide.mircv.query.CompareRes;
import it.unipi.dii.aide.mircv.query.PostingList;
import it.unipi.dii.aide.mircv.query.ResultBlock;
import java.util.ArrayList;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.query.Query.*;

public class DAAT {

    public static PriorityQueue<ResultBlock> resultQueue;
    //public static ArrayList<PostingList> postingLists = new ArrayList<>();

    public static void DocumentAtATime() {

        resultQueue = new PriorityQueue<>(k, new CompareRes());

        int current = minDocID(postingLists);

        ArrayList<Boolean> notNext = new ArrayList<>();

        for (int i = 0; i < postingLists.size(); i++) {
            notNext.add(false);
        }

        while (current != -1) {

            double score = 0;
            int next = Integer.MAX_VALUE;

            for (int i = 0; i < postingLists.size(); i++) {

                if ((!notNext.get(i)) && (postingLists.get(i).getCurrPosting().getDocId() == current)) {

                    score = score + computeScore(idf.get(i), postingLists.get(i).getCurrPosting());

                    postingLists.get(i).next();

                    if (postingLists.get(i).getCurrPosting() == null) {
                        notNext.set(i, true);
                        continue;
                    }
                }
                if ((!notNext.get(i)) && postingLists.get(i).getCurrPosting().getDocId() < next) {
                    next = postingLists.get(i).getCurrPosting().getDocId();
                }
            }

            if (resultQueue.size() < k)
                resultQueue.add(new ResultBlock(current, score));
            else if (resultQueue.size() == k && resultQueue.peek().getScore() < score) {
                resultQueue.poll();
                resultQueue.add(new ResultBlock(current, score));
            }

            if (!notNext.contains(false))
                current = -1;
            else
                current = next;
        }

        printResults(resultQueue);
    }
}
