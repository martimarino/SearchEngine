package it.unipi.dii.aide.mircv;


import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.data_structures.Dictionary;
import it.unipi.dii.aide.mircv.query.*;
import it.unipi.dii.aide.mircv.utils.FileSystem;
import it.unipi.dii.aide.mircv.utils.TextProcessor;

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

    static ArrayList<String> query_terms;

    public static HashMap<Integer, DocumentElement> documentTable = new HashMap<>();    // docID to DocElement
    public static Dictionary dictionary = new Dictionary();

    private static int k;
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









}