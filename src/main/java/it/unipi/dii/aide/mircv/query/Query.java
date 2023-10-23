package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.data_structures.Dictionary;
import it.unipi.dii.aide.mircv.query.scores.CompareScoreElem;
import it.unipi.dii.aide.mircv.query.scores.Score;
import it.unipi.dii.aide.mircv.query.scores.ScoreElem;
import it.unipi.dii.aide.mircv.utils.FileSystem;
import it.unipi.dii.aide.mircv.utils.TextProcessor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.stream.Collectors;

import static it.unipi.dii.aide.mircv.data_structures.CollectionStatistics.readCollectionStatsFromDisk;
import static it.unipi.dii.aide.mircv.query.Conjunctive.sortByValue;
import static it.unipi.dii.aide.mircv.query.algorithms.MaxScore.computeMaxScore;
import static it.unipi.dii.aide.mircv.query.scores.Score.computeBM25;
import static it.unipi.dii.aide.mircv.query.scores.Score.computeTFIDF;
import static it.unipi.dii.aide.mircv.query.algorithms.DAAT.DocumentAtATime;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;
import static it.unipi.dii.aide.mircv.data_structures.Flags.readFlagsFromDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.*;

public final class Query {

    public static final HashMap<Integer, DocumentElement> documentTable = new HashMap<>();
    public static final Dictionary dictionary = new Dictionary();


    public static int k; //number of result to return
    private static boolean queryType; // if conjunctive true, disjunctive false
    public static boolean scoreType; //type of score function (true = TFIDF or false = BM25)
    public static boolean algorithmType; //true = DAAT, false = MaxScore

    static PriorityQueue<DAATBlock> pq_DAAT;    // used during DAAT algorithm
    public static PriorityQueue<ResultBlock> pq_res;   // contains results (increasing)
    public static PriorityQueue<ResultBlock> inverse_pq_res;  // contains results (decreasing)

    public static ArrayList<PostingList> postingLists = new ArrayList<>(); // contains the posting lists of the query terms
    static HashMap<String, PostingList> term_pl = new HashMap<>();

    static HashMap<Integer, Double> index_score = new HashMap<>();
    static HashMap<Integer, Integer> index_len = new HashMap<>();

    //------------------ //
    //static PriorityQueue<ResultBlock> resultQueueInverse; // contains the final results, in decreasing order of score
    public static ArrayList<Double> idf = new ArrayList<>(); // contains the idf of each query term

    public static ArrayList<PostingList> p = new ArrayList<>(); // ordered posting lists for increasing score
    public static ArrayList<Double> orderedIdf = new ArrayList<>(); //idf ordered for increasing score

    public static ArrayList<Double> maxScore = new ArrayList<>();

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
            DataStructureHandler.readDocumentTableFromDisk(true);
            long endTime = System.currentTimeMillis();
            printTime("Document Table loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
        }

        return true;
    }

    public static void executeQuery(String q, int k, boolean q_type, boolean score, boolean algorithm) throws IOException {

        long startTime = System.currentTimeMillis();
        ArrayList<String> query = TextProcessor.preprocessText(q);
        List<String> query_terms = query.stream().distinct().collect(Collectors.toList());
        Query.k = k;
        Query.queryType = q_type;
        Query.scoreType = score;
        Query.algorithmType = algorithm;
        prepareStructures(query_terms);
        printResults();
        clearStructures();
        long endTime = System.currentTimeMillis();
        printTime("Query performed in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
    }

    private static void clearStructures() {

        if(!postingLists.isEmpty())
            postingLists.clear();

        if(!idf.isEmpty())
            idf.clear();

        if(!pq_res.isEmpty())
            pq_res.clear();

        if(!orderedIdf.isEmpty())
            orderedIdf.clear();

        if(!p.isEmpty())
            p.clear();

        if(!maxScore.isEmpty())
            maxScore.clear();

    }

    public static void executeQueryPQ(String q, int k, boolean q_type, boolean scoreType) throws IOException {

        long startTime = System.currentTimeMillis();
        ArrayList<String> query = TextProcessor.preprocessText(q);
        Query.k = k;
        queryType = q_type;
        Query.scoreType = scoreType;
        List<String> query_terms = query.stream().distinct().collect(Collectors.toList());

        pq_DAAT = new PriorityQueue<>(query_terms.size(), new CompareScore());
        pq_res = new PriorityQueue<>(k, new CompareRes());
        inverse_pq_res = new PriorityQueue<>(k, new CompareResInverse());

        try (
                RandomAccessFile docid_raf = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile tf_raf = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile skip_raf = new RandomAccessFile(SKIP_FILE, "rw")
        ) {
            docId_channel = docid_raf.getChannel();
            termFreq_channel = tf_raf.getChannel();
            skip_channel = skip_raf.getChannel();

            int index = 0;

            if (q.equals("who sings monk theme song "))
                System.out.println();

            // retrieve the posting list of every query term
            for (String t : query_terms) {

                PostingList pl = new PostingList(t);
                pl.load();

                postingLists.add(pl);
                term_pl.put(t, pl);

                if (pl.getLen() == 0 || pl.getList() == null)
                    continue;

//                index_score.put(index, pl.getScore());
                index_len.put(index, pl.getLen());

                pq_DAAT.add(new DAATBlock(t, pl.getList().get(0).getDocId(), Score.computeTFIDF(dictionary.getTermStat(t).getIdf(), pl.getCurrPosting())));

                index++;
            }

            if (queryType)
                Conjunctive.executeConjunctive();
            else {
                // scelta tra daat e maxscore
                DAATalgorithm();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (pq_res.isEmpty()) {
            System.out.println("\n*** No results found ***\n");
            return;
        }

        while (!pq_res.isEmpty()) {
            inverse_pq_res.add(pq_res.poll());
        }
        printUI("\nResults:\n");
        System.out.format("%15s%15s\n", "DOCID", "SCORE");
        System.out.format("%45s\n", "-".repeat(45));

        while (!inverse_pq_res.isEmpty()) {
            ResultBlock polled = inverse_pq_res.poll();
            System.out.format("%15s%15s\n", polled.getDocId(), String.format("%.3f", polled.getScore()));
        }
        System.out.format("%45s\n", "-".repeat(45));

        long endTime = System.currentTimeMillis();
        printTime("Query performed in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");

        postingLists.clear();
        pq_DAAT.clear();
        pq_res.clear();
        inverse_pq_res.clear();
        index_len.clear();
        index_score.clear();
        term_pl.clear();

    }

    public static void prepareStructures(List<String> query) {

        try (
                RandomAccessFile docid_raf = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile tf_raf = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile skip_raf = new RandomAccessFile(SKIP_FILE, "rw")
        ) {
            docId_channel = docid_raf.getChannel();
            termFreq_channel = tf_raf.getChannel();
            skip_channel = skip_raf.getChannel();
/*

            DictionaryElem d = dictionary.getTermStat("berlin");
            ArrayList<Posting> pls = DataStructureHandler.readPostingListFromDisk(d.getOffsetDocId(), d.getOffsetTermFreq(), d.getDf());
            for(Posting p : pls)
                printDebug(p.getDocId() + " " + p.getTermFreq()); //if(p.getDocId() > 8744790 && p.getDocId() < 8744795)
*/

            PriorityQueue<ScoreElem> orderByScore = new PriorityQueue<>(query.size(), new CompareScoreElem());
            int index = 0;

            for (String t : query) {
                DictionaryElem de = dictionary.getTermStat(t);
                if (de == null) {
                    continue;
                }
                idf.add(de.getIdf());
                PostingList pl = new PostingList(t);
                pl.load();
                postingLists.add(pl);
                if(!algorithmType)
                {
                    if(Query.scoreType)
                        orderByScore.add(new ScoreElem(index, de.getMaxBM25()));
                    else
                        orderByScore.add(new ScoreElem(index, de.getMaxTFIDF()));
                }
                index++;
            }
            if(algorithmType)
                pq_res = DocumentAtATime();
            else {
                //idf, posting lists and maxscore are ordered by the order define by orderByScore (increasing value of score)
                while(!orderByScore.isEmpty())
                {
                    ScoreElem se = orderByScore.poll();
                    p.add(postingLists.get(se.getIndex()));
                    orderedIdf.add(idf.get(se.getIndex()));
                    maxScore.add(se.getScore());
                }
                pq_res = computeMaxScore();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
                    System.out.println(new ResultBlock(acc.getDocId(), acc.getScore()));
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

        System.out.println(new ResultBlock(acc.getDocId(), acc.getScore()));
        if (pq_res.size() == k) {
            if (acc.getScore() > pq_res.peek().getScore()) {
                pq_res.remove();
                pq_res.add(new ResultBlock(acc.getDocId(), acc.getScore()));
            }
        } else if (pq_res.size() < k)
            pq_res.add(new ResultBlock(acc.getDocId(), acc.getScore()));
    }
    /**
     *
     * @param pl posting list for which identify the minimum
     * @return the min docID from all the documents in the given posting list
     */
        public static int minDocID(ArrayList<PostingList> pl) {
        ArrayList<Integer> first_docids = new ArrayList<>();

        for(PostingList p : pl){
            if(!p.getList().isEmpty())
                first_docids.add(p.getList().get(0).getDocId());
        }
        if (first_docids.isEmpty()) {
            //case when all posting lists are empty
            return -1;
        }
        return Collections.min(first_docids);
    }

    public static double computeScore(double idf, Posting currentPosting){
        double score;
        if (scoreType)
            score = computeBM25(idf, currentPosting);
        else
            score = computeTFIDF(idf, currentPosting);

        return score;
    }

    public static void printResults(){

        PriorityQueue<ResultBlock> resultQueueInverse = new PriorityQueue<>(k,new CompareResInverse());

        //results from increasing order priority queue to decreasing order one
        while (!pq_res.isEmpty()) {
            ResultBlock r = pq_res.poll();
            resultQueueInverse.add(r);
        }

        printUI("Results: \n");

        if (resultQueueInverse.isEmpty())
            printUI("No result");

        printUI("Document \t Score");
        while (!resultQueueInverse.isEmpty()) {
            printUI(resultQueueInverse.peek().getDocId() + "\t \t" + String.format("%.3f",resultQueueInverse.poll().getScore()));
        }
    }
}