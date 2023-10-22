package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.DictionaryElem;
import it.unipi.dii.aide.mircv.data_structures.Flags;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;

public class MaxScore {

    private static PriorityQueue<ResultBlock> resultQueueInverse; //used to print results in decreasing order
    private static  PriorityQueue<ResultBlock> resultQueue; //stores the results in increasing order
    private static ArrayList<PostingList> postingLists; //contains all the posting lists read from disk
    private static ArrayList<Double> idf; // contains all the idf of each query term
    private static ArrayList<Double> ub; //contains all the max scores (upper bound) of each query term
    private static ArrayList<PostingList> p; // ordered posting lists for increasing score
    private static ArrayList<Double> orderedIdf; //idf ordered for increasing score
    private static PriorityQueue<ScoreElem> orderByScore; //index, score pair ordered in increasing order

    public static void computeMaxScore(List<String> query) {

        int k = Query.k;
        idf = new ArrayList<>();
        postingLists = new ArrayList<>();
        resultQueueInverse = new PriorityQueue<>(k,new CompareResInverse());
        resultQueue = new PriorityQueue<>(k,new CompareRes());
        ub = new ArrayList<>();
        p = new ArrayList<>();
        orderedIdf = new ArrayList<>();
        int n = 0;
        double threshold = 0;
        double score;
        int next;
        int pivot = 0;
        double queueMin = Double.MAX_VALUE;

        //HashMap<Integer, Double> orderedScores = new HashMap<>();
        ArrayList<Double> maxScore = new ArrayList<Double>();
        orderByScore = new PriorityQueue<>(query.size(), new CompareIndexScore());
        ArrayList<Boolean> notNext = new ArrayList<>();

        try(
                RandomAccessFile docid_raf = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile tf_raf = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile skip_raf = new RandomAccessFile(SKIP_FILE, "rw")
        ){
            docId_channel = docid_raf.getChannel();
            termFreq_channel = tf_raf.getChannel();
            skip_channel = skip_raf.getChannel();

            int index = 0;
            for (String t : query) {
                DictionaryElem de = Query.dictionary.getTermStat(t);

                if(de == null)
                    continue;

                idf.add(de.getIdf());
                if(Query.scoreType) {
                    orderByScore.add(new ScoreElem(index, de.getMaxBM25()));
                }
                else {
                    orderByScore.add(new ScoreElem(index, de.getMaxTFIDF()));
                }
                PostingList pl = new PostingList(t);
                pl.load();
                postingLists.add(pl);
                index++;
            }

            while(!orderByScore.isEmpty())
            {
                ScoreElem se = orderByScore.poll();
                p.add(postingLists.get(se.getIndex()));
                orderedIdf.add(idf.get(se.getIndex()));
                maxScore.add(se.getScore());
            }

            n = maxScore.size();

            ub.add(0, maxScore.get(0));

            for(int i = 1; i < n; i++)
            {
                ub.add(i, ub.get(i-1) + maxScore.get(i));
            }
            for (int i = 0; i < p.size(); i++) {
                notNext.add(false);
            }

            int current = Query.minDocID(p);

            while(pivot < n && current != -1){
                score = 0;
                next = Integer.MAX_VALUE;

                for(int i = pivot; i < n; i++)
                {
                    if( (!notNext.get(i)) && p.get(i).getCurrPosting().getDocId() == current)
                    {
                        if(Query.scoreType)
                            score = score + Score.computeBM25(orderedIdf.get(i), p.get(i).getCurrPosting());
                        else
                            score = score + Score.computeTFIDF(orderedIdf.get(i), p.get(i).getCurrPosting());

                        p.get(i).next(true);

                        if( p.get(i).getCurrPosting() == null) {
                            notNext.set(i, true);
                            continue;
                        }
                    }
                    if((!notNext.get(i)) && p.get(i).getCurrPosting().getDocId() < next)
                        next = p.get(i).getCurrPosting().getDocId();
                }
                for(int i = pivot - 1; i > 0; i--)
                {
                    if(score + ub.get(i) <= threshold)
                        break;

                    if(p.get(i).getSl() != null)
                        p.get(i).nextGEQ(current, true);

                    if(p.get(i).getCurrPosting() == null)
                    {
                        notNext.set(i, true);
                        continue;
                    }

                    if(p.get(i).getCurrPosting().getDocId() == current && (!notNext.get(i)))
                        if(Query.scoreType)
                            score = score + Score.computeBM25(orderedIdf.get(i), p.get(i).getCurrPosting());
                        else
                            score = score + Score.computeTFIDF(orderedIdf.get(i), p.get(i).getCurrPosting());
                }

                if(resultQueue.size() < k) {
                    resultQueue.add(new ResultBlock(current, score));
                    queueMin = Math.min(score, queueMin);
                    threshold = resultQueue.peek().getScore();
                }
                else if(resultQueue.peek().getScore() < score) {
                    resultQueue.poll();
                    resultQueue.add(new ResultBlock(current, score));
                    threshold = resultQueue.peek().getScore();
                }

                while (pivot < n && ub.get(pivot) <= threshold)
                    pivot += 1;

                current = next;

                if (!notNext.contains(false) || next == Integer.MAX_VALUE)
                    current = -1;
            }

           /* printUI("Results: ");

            if(resultQueue.isEmpty()){
                printUI("No result");
                return;
            }*/

            while(!resultQueue.isEmpty()) {
                ResultBlock r = resultQueue.poll();
                resultQueueInverse.add(r);
            }

            printResults();
           /* printUI("Document \t Score");
            while(!resultQueueInverse.isEmpty()) {
                printUI(resultQueueInverse.peek().getDocId() + " " + String.format("%.3f",resultQueueInverse.poll().getScore()));
            }*/

    } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void printResults(){
        printUI("Results: \n");

        if (resultQueueInverse.isEmpty())
            printUI("No result");

        printUI("Document \t Score");
        while (!resultQueueInverse.isEmpty()) {
            printUI(resultQueueInverse.peek().getDocId() + "\t \t" + String.format("%.3f",resultQueueInverse.poll().getScore()));
        }
    }
}


