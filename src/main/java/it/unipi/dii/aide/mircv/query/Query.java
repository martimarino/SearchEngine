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
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.query.Score.computeBM25;
import static it.unipi.dii.aide.mircv.query.Score.computeTFIDF;
import static it.unipi.dii.aide.mircv.utils.FileSystem.*;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.Flags.readFlagsFromDisk;
import static it.unipi.dii.aide.mircv.query.Conjunctive.executeConjunctive;
import static it.unipi.dii.aide.mircv.utils.Constants.*;

public final class Query {

    public static final HashMap<Integer, DocumentElement> documentTable = new HashMap<>();
    public static final Dictionary dictionary = new Dictionary();

    public static List<String> query_terms;
    public static int k; //number of result to return

    private static String queryType; // if conjunctive ("c") or disjunctive ("d") query
    private static boolean scoreType; //type of score function ("t" = TFIDF or "b" = BM25)

    static PriorityQueue<DAATBlock> pq_DAAT;    // used during DAAT algorithm
    private static PriorityQueue<ResultBlock> pq_res;   // contains results (increasing)
    public static PriorityQueue<ResultBlock> inverseResultQueue;  // contains results (decreasing)

    public Query()  { throw new UnsupportedOperationException(); }

    public static boolean queryStartControl() throws IOException {

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

    public static void executeQuery(String q, int k, String q_type, boolean score) throws IOException {

            long startTime = System.currentTimeMillis();
        ArrayList<String> query = TextProcessor.preprocessText(q);
            Query.k = k;
            Query.queryType = q_type;
            Query.scoreType = score;
            DocumentAtATime(query);
            long endTime = System.currentTimeMillis();
            printTime("Query performed in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
    }
    public static void executeQueryPQ(String q, int k, String q_type, boolean score) throws IOException {

        long startTime = System.currentTimeMillis();
        ArrayList<String> query = TextProcessor.preprocessText(q);
        Query.k = k;
        pq_res = new PriorityQueue<>(k, new CompareRes());
        Query.queryType = q_type;
        Query.scoreType = score;
        query_terms = query.stream().distinct().collect(Collectors.toList());

        DAATalgorithm();
        long endTime = System.currentTimeMillis();
        printTime("Query performed in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
    }

    private static void DAATalgorithm() {

        HashMap<String, PostingList> postingLists = new HashMap<>();
        inverseResultQueue = new PriorityQueue<>(k, new CompareResInverse());

        try(
                RandomAccessFile docid_raf = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile tf_raf = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile skip_raf = new RandomAccessFile(SKIP_FILE, "rw")
        ){
        docId_channel = docid_raf.getChannel();
        termFreq_channel = tf_raf.getChannel();
        skip_channel = skip_raf.getChannel();

        if(queryType.equals("c")) {
            executeConjunctive();
        } else {
            pq_DAAT = new PriorityQueue<>(query_terms.size(), new CompareScore());
            // retrieve the posting list of every query term
            for (String t : query_terms) {
                DictionaryElem de = dictionary.getTermStat(t);

                if (de == null) {
                    System.out.println("Term " + t + " not present in dictionary");
                    return;
                }

                PostingList pl;
                if (Flags.isCompressionEnabled())
                    pl = new PostingList(t, readCompressedPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getTermFreqSize(), de.getDocIdSize(), de.getDf()), null, de.getTermFreqSize(), de.getDocIdSize());
                else
                    pl = new PostingList(t, readPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getDf()), null, de.getTermFreqSize(), de.getDocIdSize());

                double score;
                if (scoreType)
                    score = computeBM25(de.getIdf(), pl.getCurrPosting());
                else
                    score = computeTFIDF(de.getIdf(), pl.getCurrPosting());


                postingLists.put(t, pl);
                pq_DAAT.add(new DAATBlock(t, pl.list.get(0).getDocId(), computeTFIDF(dictionary.getTermToTermStat().get(t).getIdf(), pl.list.get(0))));
                postingLists.get(t).next();
            }

            DAATBlock acc = null;
            boolean firstIter = true;
            int counter = 0;        // number of occurrencies of the document in the different posting lists

            while (!pq_DAAT.isEmpty()) {        // iterate over documents

                DAATBlock pb = pq_DAAT.poll();
                assert pb != null;
                //currIndex

                if (firstIter) {
                    acc = new DAATBlock(pb.getTerm(), pb.getDocId(), pb.getScore());
                    firstIter = false;
                } else {
                    if (pb.getDocId() == acc.getDocId()) {
                        acc.setScore(acc.getScore() + pb.getScore());
                        counter++;
                    } else {
                        if (pq_res.size() == k) {
                            if (acc.getScore() > pq_res.peek().getScore()) {
                                pq_res.poll();
                                pq_res.add(new ResultBlock(documentTable.get(acc.getDocId()).getDocno(), acc.getDocId(), acc.getScore()));
                            }
                        } else if (pq_res.size() < k)
                            pq_res.add(new ResultBlock(documentTable.get(acc.getDocId()).getDocno(), acc.getDocId(), acc.getScore()));
                        acc = new DAATBlock(pb.getTerm(), pb.getDocId(), pb.getScore());
                        counter = 0;
                    }
                }
                //prendo prossimo elemento del termine per cui abbiamo fatto poll e lo metto in pq_DAAT calcolandone lo score
                Iterator<Posting> iterToAdvance = postingLists.get(pb.getTerm()).postingIterator;
                if (iterToAdvance.hasNext()) {
                    Posting currentPosting = iterToAdvance.next();
                    pq_DAAT.add(new DAATBlock(pb.getTerm(), currentPosting.getDocId(), computeTFIDF(dictionary.getTermToTermStat().get(pb.getTerm()).getIdf(), currentPosting)));
                }
            }

            if (pq_res == null) {
                System.out.println("No results found");
                return;
            }

            for (int i = 0; i < k && !pq_res.isEmpty(); i++) {
                inverseResultQueue.add(pq_res.poll());
            }

        }

        // print results for disjunctive or conjunctive
        if(inverseResultQueue.isEmpty())
            printUI("No results");
        else {
            printUI("\nResults:\n");
            System.out.println("DOCNO  DOCID    SCORE");
            while (!inverseResultQueue.isEmpty()) {
                System.out.println(inverseResultQueue.peek().getDocNo() + inverseResultQueue.peek().getDocId() + String.format("%.3f", inverseResultQueue.peek().getScore()));
                inverseResultQueue.poll();
            }
        }

    } catch (Exception e) {
        e.printStackTrace();
    }
}

    private static void DocumentAtATime(ArrayList<String> query){

        ArrayList<PostingList> postingLists = new ArrayList<>();
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

            for (String t : query) {
                DictionaryElem de = dictionary.getTermStat(t);
                if(de == null) {
                    continue;
                }
                idf.add(de.getIdf());
                printDebug("MAXBM25: " + de.getMaxBM25() + " MAXTFIDF: " + de.getMaxTFIDF());
                PostingList pl;
                if(Flags.isCompressionEnabled())
                    pl = new PostingList(t, readCompressedPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getTermFreqSize(), de.getDocIdSize(), de.getDf()), null, de.getTermFreqSize(), de.getDocIdSize());
                else
                    pl = new PostingList(t, readPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getDf()), null, de.getTermFreqSize(), de.getDocIdSize());
                postingLists.add(pl);
            }

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
                    if(postingLists.get(i).postingIterator.hasNext()) {
                        if (postingLists.get(i).getCurrPosting().getDocId() == current) {
                            if(scoreType)
                                score = score + computeBM25(idf.get(i), postingLists.get(i).getCurrPosting());
                            else
                                score = score + computeTFIDF(idf.get(i), postingLists.get(i).getCurrPosting());
                            postingLists.get(i).setCurrPosting(postingLists.get(i).postingIterator.next());
                        }
                        if(postingLists.get(i).getCurrPosting().getDocId() < next)
                            next = postingLists.get(i).getCurrPosting().getDocId();
                    }else{
                        notNext.set(i, true);
                    }
                }
                if(resultQueue.size() < k)
                    resultQueue.add(new ResultBlock("", current, score));
                else if(resultQueue.size() == k && resultQueue.peek().getScore() < score)
                {
                    resultQueue.poll();
                    resultQueue.add(new ResultBlock("", current, score));
                }
                current = next;
                if(!notNext.contains(false))
                    current = -1;
            }

            while(!resultQueue.isEmpty()) {
                ResultBlock r = resultQueue.poll();
                resultQueueInverse.add(new ResultBlock("", r.getDocId(),r.getScore()));
            }

            while(!resultQueueInverse.isEmpty()) {
                printUI(documentTable.get(resultQueueInverse.poll().getDocId()).getDocno());

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int minDocID(ArrayList<PostingList> pl) {
        ArrayList<Integer> first_docids = new ArrayList<>();

        for(PostingList p : pl){
            if(!p.list.isEmpty())
                first_docids.add(p.list.get(0).getDocId());
        }
        if (first_docids.isEmpty()) {
            //case when all posting lists are empty.
            return -1;
        }
        return Collections.min(first_docids);
    }
}