package it.unipi.dii.aide.mircv.query;


import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.data_structures.Dictionary;
import it.unipi.dii.aide.mircv.utils.FileSystem;
import it.unipi.dii.aide.mircv.utils.TextProcessor;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.stream.Collectors;

import static it.unipi.dii.aide.mircv.data_structures.CollectionStatistics.readCollectionStatsFromDisk;
import static it.unipi.dii.aide.mircv.query.Score.computeBM25;
import static it.unipi.dii.aide.mircv.query.Score.computeTFIDF;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;
import static it.unipi.dii.aide.mircv.data_structures.Flags.readFlagsFromDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.*;

public final class Query {

    public static final HashMap<Integer, DocumentElement> documentTable = new HashMap<>();
    public static final Dictionary dictionary = new Dictionary();


    public static int k; //number of result to return
    private static String queryType; // if conjunctive ("c") or disjunctive ("d") query
    private static boolean scoreType; //type of score function ("t" = TFIDF or "b" = BM25)

    static PriorityQueue<DAATBlock> pq_DAAT;    // used during DAAT algorithm
    public static PriorityQueue<ResultBlock> pq_res;   // contains results (increasing)
    public static PriorityQueue<ResultBlock> inverse_pq_res;  // contains results (decreasing)

    public static ArrayList<PostingList> postingLists = new ArrayList<>();
    static HashMap<String, PostingList> term_pl = new HashMap<>();

    static HashMap<Integer, Double> index_score = new HashMap<>();
    static HashMap<Integer, Integer> index_len = new HashMap<>();

    public Query()  { throw new UnsupportedOperationException(); }

    public static boolean queryStartControl() {

        // -- control for file into disk
        if (!FileSystem.areThereAllMergedFiles() ||
                !Flags.isThereFlagsFile() ||
                !CollectionStatistics.isThereStatsFile()) {
            printError("Error: missing required files.");
            return false;
        }

        readFlagsFromDisk();
        readCollectionStatsFromDisk();

        // -- control for structures in memory - if not load them from disk
        if (!dictionary.dictionaryIsSet()) {
            long startTime = System.currentTimeMillis();
            Query.dictionary.readDictionaryFromDisk();
            long endTime = System.currentTimeMillis();
            printTime("Dictionary loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
        }
        if (documentTable.isEmpty()) {
            long startTime = System.currentTimeMillis();
            DataStructureHandler.readDocumentTableFromDisk(1);
            long endTime = System.currentTimeMillis();
            printTime("Document Table loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
        }

        return true;
    }

    public void executeQuery(String q, int k, String q_type, boolean score) throws IOException {

            long startTime = System.currentTimeMillis();
        ArrayList<String> query = TextProcessor.preprocessText(q);
            Query.k = k;
            Query.queryType = q_type;
            Query.scoreType = score;
            DocumentAtATime();
            long endTime = System.currentTimeMillis();
            printTime("Query performed in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
    }

    public static void executeQueryPQ(String q, int k, String q_type, boolean scoreType) throws IOException {

        long startTime = System.currentTimeMillis();
        ArrayList<String> query = TextProcessor.preprocessText(q);
        Query.k = k;
        queryType = q_type;
        Query.scoreType = scoreType;
        List<String> query_terms = query.stream().distinct().collect(Collectors.toList());

        pq_DAAT = new PriorityQueue<>(query_terms.size(), new CompareScore());
        pq_res = new PriorityQueue<>(k, new CompareRes());
        inverse_pq_res = new PriorityQueue<>(k, new CompareResInverse());

        try(
                RandomAccessFile docid_raf = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile tf_raf = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile skip_raf = new RandomAccessFile(SKIP_FILE, "rw")
        ){
            docId_channel = docid_raf.getChannel();
            termFreq_channel = tf_raf.getChannel();
            skip_channel = skip_raf.getChannel();

            int index = 0;

            // retrieve the posting list of every query term
            for (String t : query_terms) {

                PostingList pl = new PostingList(t);
                pl.load();

                postingLists.add(pl);
                term_pl.put(t, pl);

                if(pl.getLen() == 0 || pl.getList() == null)
                    continue;

//                index_score.put(index, pl.getScore());
                index_len.put(index, pl.getLen());

                pq_DAAT.add(new DAATBlock(t, pl.getList().get(0).getDocId(), Score.computeTFIDF(dictionary.getTermStat(t).getIdf(), pl.getCurrPosting())));

                index++;
            }

            if(queryType.equals("c"))
                Conjunctive.executeConjunctive();
            else {
                // scelta tra daat e maxscore
                DAATalgorithm();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }


        if (pq_res == null) {
            System.out.println("No results found");
            return;
        }

       while (!pq_res.isEmpty()) {
            inverse_pq_res.add(pq_res.poll());
        }
        printUI("\nResults:\n");
        System.out.format("%15s%15s\n", "DOCID", "SCORE");
        System.out.format("%60s\n", "-".repeat(60));

        while (!inverse_pq_res.isEmpty()) {
            ResultBlock polled = inverse_pq_res.poll();
            System.out.format("%15s%15s\n", polled.getDocId(), String.format("%.3f", polled.getScore()));
        }
        System.out.format("%60s\n", "-".repeat(60));

        long endTime = System.currentTimeMillis();
        printTime("Query performed in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");


        postingLists.clear();
        pq_DAAT.clear();
        pq_res.clear();
        inverse_pq_res.clear();
        index_len.clear();
        index_score.clear();

    }

    private static void DAATalgorithm() {


        DAATBlock acc = null;
        boolean firstIter = true;

        while (!Query.pq_DAAT.isEmpty()) {        // iterate over documents

            DAATBlock pb = Query.pq_DAAT.poll();
            assert pb != null;

            if (firstIter) {
                acc = new DAATBlock(pb.getTerm(), pb.getDocId(), pb.getScore());
                firstIter = false;
            } else {
                if (pb.getDocId() == acc.getDocId()) {
                    acc.setScore(acc.getScore() + pb.getScore());
                } else {
                    if (pq_res.size() == k) {
                        if (acc.getScore() > pq_res.peek().getScore()) {
                            pq_res.remove();
                            pq_res.add(new ResultBlock(acc.getDocId(), acc.getScore()));
                        }
                    } else if (pq_res.size() < k)
                        pq_res.add(new ResultBlock(acc.getDocId(), acc.getScore()));
                    acc = new DAATBlock(pb.getTerm(), pb.getDocId(), pb.getScore());
                }
            }
            //prendo prossimo elemento del termine per cui abbiamo fatto poll e lo metto in pq_DAAT calcolandone lo scoreType
            Iterator<Posting> iterToAdvance =
                    Query.term_pl.get(pb.getTerm()).getPostingIterator();
            if (iterToAdvance.hasNext()) {
                Posting currentPosting = iterToAdvance.next();
                Query.pq_DAAT.add(new DAATBlock(pb.getTerm(), currentPosting.getDocId(), computeTFIDF(dictionary.getTermToTermStat().get(pb.getTerm()).getIdf(), currentPosting)));
            }
        }
    }

    private void DocumentAtATime(){

        PriorityQueue<ResultBlock> resultQueueInverse = new PriorityQueue<>(k,new CompareResInverse());
        PriorityQueue<ResultBlock> resultQueue = new PriorityQueue<>(k,new CompareRes());
        ArrayList<Double> idf = new ArrayList<>();

        try(
                RandomAccessFile docid_raf = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile tf_raf = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile skip_raf = new RandomAccessFile(SKIP_FILE, "rw")
        ){
            docId_channel = docid_raf.getChannel();
            termFreq_channel = tf_raf.getChannel();
            skip_channel = skip_raf.getChannel();

            int current = minDocID(postingLists);

            ArrayList<Boolean> notNext = new ArrayList<>();

            for(int i = 0; i < postingLists.size(); i++) {
                notNext.add(false);
            }

            while(current != -1){

                double score = 0;
                int next = Integer.MAX_VALUE;

                for(int i = 0; i < postingLists.size(); i++)
                {
                    if(postingLists.get(i).getPostingIterator().hasNext()) {
                        if (postingLists.get(i).getCurrPosting().getDocId() == current) {
                            if(scoreType)
                                score = score + computeBM25(idf.get(i), postingLists.get(i).getCurrPosting());
                            else
                                score = score + computeTFIDF(idf.get(i), postingLists.get(i).getCurrPosting());
                            postingLists.get(i).setCurrPosting(postingLists.get(i).getPostingIterator().next());
                        }
                        if(postingLists.get(i).getCurrPosting().getDocId() < next)
                            next = postingLists.get(i).getCurrPosting().getDocId();
                    }else{
                        notNext.set(i, true);
                    }
                }
                if(resultQueue.size() < k)
                    resultQueue.add(new ResultBlock(current, score));
                else if(resultQueue.size() == k && resultQueue.peek().getScore() < score)
                {
                    resultQueue.poll();
                    resultQueue.add(new ResultBlock(current, score));
                }
                current = next;
                if(!notNext.contains(false))
                    current = -1;
            }

            while(!resultQueue.isEmpty()) {
                ResultBlock r = resultQueue.poll();
                resultQueueInverse.add(new ResultBlock(r.getDocId(),r.getScore()));
            }

            while(!resultQueueInverse.isEmpty()) {
                printUI(documentTable.get(resultQueueInverse.poll().getDocId()).getDocno());

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int minDocID(ArrayList<PostingList> pl) {
        ArrayList<Integer> first_docids = new ArrayList<>();

        for(PostingList p : pl){
            if(!p.getList().isEmpty())
                first_docids.add(p.getList().get(0).getDocId());
        }
        if (first_docids.isEmpty()) {
            //case when all posting lists are empty.
            return -1;
        }
        return Collections.min(first_docids);
    }
}