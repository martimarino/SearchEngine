package it.unipi.dii.aide.mircv.query.algorithms;
import it.unipi.dii.aide.mircv.data_structures.Posting;
import it.unipi.dii.aide.mircv.query.ResultBlock;
import java.util.ArrayList;
import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.query.Query.*;

public class DAAT {

    public static PriorityQueue<ResultBlock> DocumentAtATime() {

        PriorityQueue<ResultBlock> resultQueue = new PriorityQueue<>(k, new ResultBlock.CompareRes());

        int current = minDocID(postingLists);

        while (current != -1) {

            double score = 0;
            int next = Integer.MAX_VALUE;

            for (int i = 0; i < postingLists.size(); i++) {
                if ((postingLists.get(i).getCurrPosting() != null) && (postingLists.get(i).getCurrPosting().getDocId() == current)) {
                    score = score + computeScore(postingLists.get(i).getIdf(), postingLists.get(i).getCurrPosting());

                    postingLists.get(i).next(true);

                    if (postingLists.get(i).getCurrPosting() == null)
                        continue;
                }
                if ((postingLists.get(i).getCurrPosting() != null) && postingLists.get(i).getCurrPosting().getDocId() < next) {
                    next = postingLists.get(i).getCurrPosting().getDocId();
                }
            }

            if (resultQueue.size() < k)
                resultQueue.add(new ResultBlock(current, score));
            else if (resultQueue.size() == k && resultQueue.peek().getScore() < score) {
                resultQueue.poll();
                resultQueue.add(new ResultBlock(current, score));
            }

            if (next == Integer.MAX_VALUE)
                current = -1;
            else
                current = next;

        }
        return resultQueue;
    }
}
