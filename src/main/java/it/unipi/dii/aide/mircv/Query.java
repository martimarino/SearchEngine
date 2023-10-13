package it.unipi.dii.aide.mircv;


import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.data_structures.Dictionary;
import it.unipi.dii.aide.mircv.query.*;
import it.unipi.dii.aide.mircv.utils.FileSystem;
import it.unipi.dii.aide.mircv.utils.TextProcessor;

import it.unipi.dii.aide.mircv.utils.FileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;

import static it.unipi.dii.aide.mircv.data_structures.CollectionStatistics.readCollectionStatsFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.Flags.readFlagsFromDisk;
import static it.unipi.dii.aide.mircv.score.Score.computeTFIDF;
import static it.unipi.dii.aide.mircv.utils.Constants.*;

public final class Query {

    static ArrayList<String> query;

    public static HashMap<Integer, DocumentElement> documentTable = new HashMap<>();    // docID to DocElement
    static Dictionary dictionary = new Dictionary();

    static int k;
    static HashMap<Integer, Double> topKresults = new HashMap<>();
    static ArrayList<String> query_terms;

    private static String queryType;

    static PriorityQueue<DAATBlock> pq_DAAT;    // used during DAAT algorithm
    static PriorityQueue<ResultBlock> pq_res;   // contains results

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
        if (!dictionary.dictionaryIsSet())
        {
            long startTime = System.currentTimeMillis();
            Query.dictionary.readDictionaryFromDisk();
            long endTime = System.currentTimeMillis();
            printTime( "Dictionary loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
        }
        if(documentTable.isEmpty())
        {
            long startTime = System.currentTimeMillis();
            DataStructureHandler.readDocumentTableFromDisk(false);
            long endTime = System.currentTimeMillis();
            printTime("Document Table loaded in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
        }

        return true;
    }

    public static void executeQuery(String q, int k, String q_type) throws IOException {

        long startTime = System.currentTimeMillis();
        query = TextProcessor.preprocessText(q);
        Query.k = k;
        DocumentAtATime(query, k);
        long endTime = System.currentTimeMillis();
        printTime("Query performed in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");

    }
    public static void executeQueryPQ(String q, int k, String q_type) throws IOException {
        long startTime = System.currentTimeMillis();
        query_terms = TextProcessor.preprocessText(q);
        Query.k = k;
        pq_res = new PriorityQueue<>(k, new CompareRes());
        Query.queryType = q_type;
        DAATalgorithm();
        long endTime = System.currentTimeMillis();
        printTime("Query performed in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
    }

    private static void DAATalgorithm() {

        HashMap<String, PostingList> postingLists = new HashMap<>();

        try (
                RandomAccessFile docid_raf = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile termfreq_raf = new RandomAccessFile(TERMFREQ_FILE, "rw");
                RandomAccessFile skip_raf = new RandomAccessFile(SKIP_FILE, "rw");

                FileChannel docIdChannel = docid_raf.getChannel();
                FileChannel termFreqChannel = termfreq_raf.getChannel();
                FileChannel skipChannel = skip_raf.getChannel()
        ) {
            if(queryType.equals("c")) {
                Conjunctive.executeConjunctive(query_terms, k, docIdChannel, termFreqChannel, skipChannel);
                return;
            }
            pq_DAAT = new PriorityQueue<>(query_terms.size(), new CompareScore());
            // retrieve the posting list of every query term
            for (String t : query_terms) {
                DictionaryElem de = dictionary.getTermStat(t);
                SkipList sl = new SkipList(de.getSkipOffset(), de.getSkipArrLen(), skipChannel);
                PostingList pl = new PostingList(readPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getDf(), docIdChannel, termFreqChannel), sl);
                postingLists.put(t, pl);
                pq_DAAT.add(new DAATBlock(t, pl.list.get(0).getDocId(), computeTFIDF(dictionary.getTermToTermStat().get(t).getTerm(), pl.list.get(0))));
                postingLists.get(t).postingIterator.next();
            }

            DAATBlock acc = null;
            boolean firstIter = true;
            int counter = 0;        // number of occurrencies of the document in the different posting lists

            while (!pq_DAAT.isEmpty()) {        // iterate over documents

                DAATBlock pb = pq_DAAT.poll();
                assert pb != null;
                //currIndex

                if(firstIter) {
                    acc = new DAATBlock(pb.getTerm(), pb.getDocId(), pb.getScore());
                    firstIter = false;
                } else {
                    if (pb.getDocId() == acc.getDocId()) {
                        acc.setScore(acc.getScore() + pb.getScore());
                        counter++;
                    } else {
                        if (pq_res.size() == k) {
                            assert pq_res.peek() != null;
                            if (acc.getScore() > pq_res.peek().getScore()) {
                                if (queryType.equals("c")) {
                                    if (counter == postingLists.size()) {
                                        pq_res.poll();
                                        pq_res.add(new ResultBlock(documentTable.get(acc.getDocId()).getDocno(), acc.getDocId(), acc.getScore()));
                                    } else {

                                    }
                                } else if(queryType.equals("d")){
                                    pq_res.poll();
                                    pq_res.add(new ResultBlock(documentTable.get(acc.getDocId()).getDocno(), acc.getDocId(), acc.getScore()));
                                }
                            }
                        }else if(pq_res.size() < k)
                            pq_res.add(new ResultBlock(documentTable.get(acc.getDocId()).getDocno(), acc.getDocId(), acc.getScore()));
                        acc = new DAATBlock(pb.getTerm(), pb.getDocId(), pb.getScore());
                        counter = 0;
                    }
                }
                //prendo prossimo elemento del termine per cui abbiamo fatto poll e lo metto in pq_DAAT calcolandone lo score
                Iterator<Posting> iterToAdvance = postingLists.get(pb.getTerm()).postingIterator;
                if(iterToAdvance.hasNext()){
                    Posting currentPosting = iterToAdvance.next();
                    pq_DAAT.add(new DAATBlock(pb.getTerm(), currentPosting.getDocId(), computeTFIDF(pb.getTerm(), currentPosting)));
                }

            }

            if(pq_res == null) {
                printUI("No results found");
                return;
            }

            PriorityQueue<ResultBlock> inverseResultQueue = new PriorityQueue<>(k,new CompareResInverse());

            for (int i = 0; i < k && !pq_res.isEmpty(); i++) {
                inverseResultQueue.add(pq_res.poll());
            }
            while(!inverseResultQueue.isEmpty())
                printUI(inverseResultQueue.poll().toString());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static double computeTFIDF(String term, Posting p) {

        double tf = 1 + Math.log10(p.getTermFreq());
        double idf = dictionary.getTermStat(term).getIdf();
        return tf*idf;
    }


    public static double computeTfidf(Double idf, Posting p) {

        double tf = 1 + Math.log10(p.getTermFreq());
        return tf*idf;
    }

    private static void DocumentAtATime(ArrayList<String> query, int k){

        ArrayList<PostingList> postingLists = new ArrayList<>();
        PriorityQueue<ResBlock> resultQueueInverse = new PriorityQueue<>(k,new CompareResInv());
        PriorityQueue<ResultBlock> resultQueue = new PriorityQueue<>(k,new CompareRes());
        ArrayList<Double> idf = new ArrayList<>();

        try (
                RandomAccessFile docid_raf = new RandomAccessFile(DOCID_FILE, "rw");
                RandomAccessFile termfreq_raf = new RandomAccessFile(TERMFREQ_FILE, "rw");

                FileChannel docIdChannel = docid_raf.getChannel();
                FileChannel termFreqChannel = termfreq_raf.getChannel();
        ) {
            for (String t : query) {
                DictionaryElem de = dictionary.getTermStat(t);
                if(de == null) {
                    continue;
                }
                idf.add(de.getIdf());
                PostingList pl = new PostingList(readPostingListFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), de.getDf(), docIdChannel, termFreqChannel));
                postingLists.add(pl);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        int current = minDocID(postingLists);

        ArrayList<Boolean> notNext = new ArrayList<Boolean>();
        Posting[] currentPostings = new Posting[postingLists.size()];

        for(int i = 0; i < postingLists.size(); i++) {
            notNext.add(false);
            if (postingLists.get(i).postingIterator.hasNext())
                currentPostings[i] = postingLists.get(i).postingIterator.next();
        }

        while(current != -1){
            double score = 0;
            int next = Integer.MAX_VALUE;

            for(int i = 0; i < postingLists.size(); i++)
            {
                if(postingLists.get(i).postingIterator.hasNext()) {
                    if (currentPostings[i].getDocId() == current) {
                        score = score + computeTfidf(idf.get(i), currentPostings[i]);
                        currentPostings[i] = postingLists.get(i).postingIterator.next();
                    }
                    if(currentPostings[i].getDocId() < next)
                        next = currentPostings[i].getDocId();
                }else{
                    notNext.set(i, true);
                }
            }
            if(resultQueue.size() < k)
            resultQueue.add(new ResultBlock("", current, score));
            else if(resultQueue.size() == k && resultQueue.peek().score < score)
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
            resultQueueInverse.add(new ResBlock(r.docId, r.score));
        }

        int index = 0;

        while(!resultQueueInverse.isEmpty()) {
            if (index < k) {
                printUI(resultQueueInverse.poll().toString());
                index++;
            }else{
                break;
            }
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



    private static class ResBlock{
        private int docId;
        private double score;


        public ResBlock(int docId, double score) {
            this.docId = docId;
            this.score = score;
        }

        public int getDocId() {
            return docId;
        }

        public void setDocId(int docId) {
            this.docId = docId;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return "ResBlock{" +
                    "docId=" + docId +
                    ", score=" + score +
                    '}';
        }
    }

    private static class CompareResInv implements Comparator<ResBlock> {
        @Override
        public int compare(ResBlock pb1, ResBlock pb2) {

            int scoreCompare = Double.compare(pb2.getScore(), pb1.getScore());

            if(scoreCompare == 0)
                return Integer.compare(pb1.getDocId(), pb2.getDocId());

            return scoreCompare;
        }
    }

    /**
     * class to define DAATBlock. The priority queue contains instances of DAATBlock
     */
    private static class DAATBlock {

        private String term;
        private int docId;                  // DocID
        private double score;     // reference to the posting list (index in the array of posting lists of the query) containing DcoID

        // constructor with parameters
        public DAATBlock(String term, int docId, double score) {
            this.term = term;
            this.docId = docId;
            this.score = score;
        }

        public int getDocId() {
            return docId;
        }

        public double getScore() {
            return score;
        }

        public String getTerm() {
            return term;
        }

        public void setTerm(String term) {
            this.term = term;
        }

        public void setDocId(int docId) {
            this.docId = docId;
        }

        public void setScore(double score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return "DAATBlock{" +
                    "term='" + term + '\'' +
                    ", docId=" + docId +
                    ", score=" + score +
                    '}';
        }

    }







        public int getDocId() {
            return docId;
        }

        public double getScore() {
            return score;
        }

        public String getDocNo() {
            return docNo;
        }

        public void setDocNo(String docNo) {
            this.docNo = docNo;
        }

        public void setDocId(int docId) {
            this.docId = docId;
        }

        public void setScore(double score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return "ResultBlock{" +
                    "docNo=" + docNo +
                    ", docId=" + docId +
                    ", score=" + score +
                    '}';
        }

    }
    private static class CompareRes implements Comparator<ResultBlock> {
        @Override
        public int compare(ResultBlock pb1, ResultBlock pb2) {

            int scoreCompare = Double.compare(pb1.getScore(), pb2.getScore());

            if(scoreCompare == 0)
                return Integer.compare(pb1.getDocId(), pb2.getDocId());

            return scoreCompare;
        }
    }

    private static class CompareResInverse implements Comparator<ResultBlock> {
        @Override
        public int compare(ResultBlock pb1, ResultBlock pb2) {

            int scoreCompare = Double.compare(pb2.getScore(), pb1.getScore());

            if(scoreCompare == 0)
                return Integer.compare(pb1.getDocId(), pb2.getDocId());

            return scoreCompare;
        }
    }

    private static class PostingList {

        public ArrayList<Posting> list;
        public Iterator<Posting> postingIterator;

        public PostingList(ArrayList<Posting> list) {
            this.list = list;
            this.postingIterator = list.iterator();
        }

    }

}