package it.unipi.dii.aide.mircv.query;

import it.unipi.dii.aide.mircv.data_structures.Dictionary;
import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.query.scores.ScoreElem;
import it.unipi.dii.aide.mircv.utils.FileSystem;
import it.unipi.dii.aide.mircv.utils.TextProcessor;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.stream.Collectors;

import static it.unipi.dii.aide.mircv.data_structures.CollectionStatistics.readCollectionStatsFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.Flags.readFlagsFromDisk;
import static it.unipi.dii.aide.mircv.query.algorithms.DAAT.DocumentAtATime;
import static it.unipi.dii.aide.mircv.query.algorithms.MaxScore.computeMaxScore;
import static it.unipi.dii.aide.mircv.query.scores.Score.computeBM25;
import static it.unipi.dii.aide.mircv.query.scores.Score.computeTFIDF;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;

public final class Query {

    public static final HashMap<Integer, DocumentElement> documentTable = new HashMap<>();
    public static final Dictionary dictionary = new Dictionary();

    public static int k; //number of result to return
    private static boolean disj_conj; // (false = disjunctive, true = conjunctive)
    public static boolean tfidf_bm25; // (false = TFIDF or true = BM25)
    public static boolean daat_maxscore; // (false = DAAT, true = MaxScore)

    static HashMap<Integer, Integer> index_len = new HashMap<>();

    //------------------ //

    public static final ArrayList<PostingList> ordered_PostingLists = new ArrayList<>(); // ordered posting lists for increasing score
    public static final ArrayList<PostingList> all_postingLists = new ArrayList<>(); // contains the posting lists of the query terms
    public static PriorityQueue<ResultBlock> pq_res;   // contains results (increasing)

    public Query()  { throw new UnsupportedOperationException(); }

    /***
     * Check if all the necessary files are present in the folder to load them in memory.
     * @return false if necessary files are not in the folder, true if they have been correctly load in memory.
     */
    public static boolean queryStartControl() {

/*        // -- control for file into disk
        if (!FileSystem.areThereAllMergedFiles() ||
                !Flags.isThereFlagsFile() ||
                !CollectionStatistics.isThereStatsFile()) {
            printError("Error: missing required files.");
            return false;
        }*/

        readFlagsFromDisk();
        readCollectionStatsFromDisk();

        printUI("\nConfiguration:");
        printUI(" - stopwords: " + Flags.isSwsEnabled());
        printUI(" - compression: " + Flags.isCompressionEnabled());
        printUI(" - debug mode: " + Flags.isDebug_flag() + "\n");

        // -- control for structures in memory - if not load them from disk
        if (!dictionary.dictionaryIsSet()) {
            long startTime = System.currentTimeMillis();
            Query.dictionary.readDictionaryFromDisk();
            long endTime = System.currentTimeMillis();
            printTime("Dictionary loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
        }

//        if (documentTable.isEmpty()) {
//            long startTime = System.currentTimeMillis();
//            DataStructureHandler.readDocumentTableFromDisk(false);
//            long endTime = System.currentTimeMillis();
//            printTime("Document Table loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
//        }

        return true;
    }

    /***
     * Executes the query passed with the configuration specified.
     * @param q -> query to process
     * @param k -> number of results to return
     * @param disj_conj (query type) -> false = disjunctive, true = conjunctive
     * @param tfidf_bm25 (score type) -> false = TFIDF, true = BM25
     * @param daat_maxscore (algorithm type) -> false = DAAT, true = MaxScore
     * @throws IOException
     */
    public static void executeQuery(String q, int k, String disj_conj, String tfidf_bm25, String daat_maxscore) throws IOException {

        long startTime = System.currentTimeMillis();
        ArrayList<String> query = TextProcessor.preprocessText(q);
        List<String> query_terms = query.stream().distinct().collect(Collectors.toList());
        Query.k = k;
        Query.disj_conj = disj_conj;
        Query.tfidf_bm25 = tfidf_bm25;
        Query.daat_maxscore = daat_maxscore;
        prepareStructures(query_terms);
        printResults();
        clearStructures();
        long endTime = System.currentTimeMillis();
        printTime("Query \"" + q + "\" performed in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
    }

    private static void clearStructures() {

        if(!all_postingLists.isEmpty())
            all_postingLists.clear();

        if(!pq_res.isEmpty())
            pq_res.clear();

        if(!ordered_PostingLists.isEmpty())
            ordered_PostingLists.clear();
    }

    /***
     * Create the necessary data structures, open file channels, read the posting lists of the query terms
     * (entire or first block if the have skipping) and calls the function for the configuration requested.
     * @param query
     */
    public static void prepareStructures(List<String> query) {

        try (
                RandomAccessFile docid_raf = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile tf_raf = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile skip_raf = new RandomAccessFile(SKIP_FILE, "rw")
        ) {
            docId_channel = docid_raf.getChannel();
            termFreq_channel = tf_raf.getChannel();
            skip_channel = skip_raf.getChannel();

            PriorityQueue<ScoreElem> orderByScore = new PriorityQueue<>(query.size(), new ScoreElem.CompareScoreElem());
            int index = 0;
            for (String t : query) {
                DictionaryElem de = dictionary.getTermStat(t);
                if (de == null)
                    continue;

                PostingList pl = new PostingList(de);
                if(daat_maxscore.equals("m"))
                {
                    double score;
                    if(tfidf_bm25.equals("b"))
                        score = de.getMaxBM25();
                    else
                        score = de.getMaxTFIDF();
                    pl.setMaxScore(score);
                    orderByScore.add(new ScoreElem(index, score));
                }
                if(disj_conj.equals("c"))
                {
                    if (pl.getLen() == 0 || pl.getList() == null)
                        continue;

                    index_len.put(index, pl.getLen());
                }
                all_postingLists.add(pl);
                index++;
            }

            if(disj_conj) {
                pq_res = Conjunctive.executeConjunctive();

            }else {
                if (daat_maxscore.equals("d"))
                    pq_res = DocumentAtATime();
                else {
                    //idf, posting lists and maxscore are ordered by the order define by orderByScore (increasing value of score)
                    while (!orderByScore.isEmpty()) {
                        ordered_PostingLists.add(all_postingLists.get(orderByScore.poll().getIndex()));
                    }
                    pq_res = computeMaxScore();
                    orderByScore.clear();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        double score = 0;
        if (tfidf_bm25)
            score = computeBM25(idf, currentPosting, false);
        else
            score = computeTFIDF(idf, currentPosting);

        return score;
    }

    public static void printResults(){

        PriorityQueue<ResultBlock> resultQueueInverse = new PriorityQueue<>(k,new ResultBlock.CompareResDec());

        //results from increasing order priority queue to decreasing order one
        while (!pq_res.isEmpty()) {
            ResultBlock r = pq_res.poll();
            resultQueueInverse.add(r);
        }

        printUI("\nResults: \n");

        if (resultQueueInverse.isEmpty()) {
            printUI("No results");
            return;
        }

        printUI(String.format("\t%-15s%-15s", "Document", "Score"));
        printUI(String.format("%30s", "-".repeat(30)));

        while (!resultQueueInverse.isEmpty()) {
            ResultBlock polled = resultQueueInverse.poll();
            printUI(String.format("\t%-15s%-15s", polled.getDocId(), String.format("%.3f", polled.getScore())));
        }
        printUI(String.format("%30s\n", "-".repeat(30)));

    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {

        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());
        Map<K, V> result = new LinkedHashMap<>();

        for (Map.Entry<K, V> entry : list)
            result.put(entry.getKey(), entry.getValue());

        return result;
    }
}