package it.unipi.dii.aide.mircv.query.algorithms;
import it.unipi.dii.aide.mircv.query.ResultBlock;

import java.util.PriorityQueue;

import static it.unipi.dii.aide.mircv.query.Query.*;

public class DAAT {

    public static PriorityQueue<ResultBlock> DocumentAtATime() {

        PriorityQueue<ResultBlock> resultQueue = new PriorityQueue<>(k, new ResultBlock.CompareResInc());

        int current = minDocID(all_postingLists);

        while (current != -1) {

            double score = 0;
            int next = Integer.MAX_VALUE;

            for (int i = 0; i < all_postingLists.size(); i++) {
                if ((all_postingLists.get(i).getCurrPosting() != null) && (all_postingLists.get(i).getCurrPosting().getDocId() == current)) {
                    score = score + computeScore(all_postingLists.get(i).getIdf(), all_postingLists.get(i).getCurrPosting());

                    all_postingLists.get(i).next(true);

                    if (all_postingLists.get(i).getCurrPosting() == null)
                        continue;
                }
                if ((all_postingLists.get(i).getCurrPosting() != null) && all_postingLists.get(i).getCurrPosting().getDocId() < next) {
                    next = all_postingLists.get(i).getCurrPosting().getDocId();
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
