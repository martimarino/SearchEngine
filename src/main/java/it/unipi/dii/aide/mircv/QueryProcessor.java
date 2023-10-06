package it.unipi.dii.aide.mircv;
import it.unipi.dii.aide.mircv.compression.Unary;
import it.unipi.dii.aide.mircv.data_structures.*;
import it.unipi.dii.aide.mircv.data_structures.Dictionary;
import it.unipi.dii.aide.mircv.utils.FileSystem;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;

import static it.unipi.dii.aide.mircv.data_structures.CollectionStatistics.readCollectionStatsFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readCompressedPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.Flags.readFlagsFromDisk;
//import static it.unipi.dii.aide.mircv.data_structures.PartialIndexBuilder.dictionaryBlockOffsets;
import static it.unipi.dii.aide.mircv.utils.Constants.*;

/**
 * Class to manage and execute query
 */
public final class QueryProcessor {

    // indicate whether order all or only first "numberOfResults" results from hash table. TEST VARIABLE
    private static boolean orderAllHashMap = false;
    // HashMap for containing the DocID and document score related. DID -> doc score
    private static final HashMap<Integer, Double> tableDAAT = new HashMap<>();
    // HashMap for containing the top "numberOfResults" DocID of the document related to score value. doc score -> ArrayList of DID
    private static final HashMap<Double, ArrayList<Integer>> scoreToDocID = new HashMap<>();
    // HashMap for containing the score values for which "numberOfResults" docs have already been found. Score -> true or false
    private static final HashMap<Double, Boolean> scoreWithMaxDoc = new HashMap<>();
    public static HashMap<Integer, DocumentElement> documentTable = new HashMap<>();    // hash table DocID to related DocElement
    static it.unipi.dii.aide.mircv.data_structures.Dictionary dictionary = new Dictionary();    // dictionary in memory

    static PriorityQueue<QueryProcessor.PostingBlock> pq;

    /**
     * fuction to manage the query request. Prepare and execute the query and return the results.
     *
     * @param query             is the query of the users (in words)
     * @param isConjunctive     indicates whether the query is of conjunctive type
     * @param isDisjunctive     indicates whether the query is of disjunctive type
     * @param numberOfResults   the number of results to be returned by the query
     * @return  an ArrayList of integer that representing an ordered list of DocIDs
     */
    public static ArrayList<Integer> queryManager(String query, boolean isConjunctive, boolean isDisjunctive, int numberOfResults)
    {
        ArrayList<Integer> rankedResults = new ArrayList<>();   // ArrayList that contain the ranked results of query
        ArrayList<String> processedQuery;                       // array list for containing the query term

        // take user's choices that affecting the query execution
        boolean scoringFunc = Flags.isScoringEnabled();      // take user's choice about using scoring function

        try{
            // processed the query to obtain the term
            printDebug("Query before processed: " + query);
            processedQuery = TextProcessor.preprocessText(query); // Preprocessing of document text
            printDebug("Query after processed: " + processedQuery);
            printDebug(dictionary.getTermStat("0000").toString());

            // control for correct form
            if ( (isConjunctive && isDisjunctive) || !(isConjunctive || isDisjunctive))     // query is Conjunctive or Disjunctive cannot be both or neither
            {
                printError("Error: query is Conjunctive or Disjunctive cannot be both or neither.");  // mex of error
                return rankedResults;
            }

            DAATAlgorithm(processedQuery,isConjunctive, isDisjunctive,numberOfResults);        // apply DAAT, result in tableDAAT

            rankedResults = getRankedResults(numberOfResults);          // get ranked results
            tableDAAT.clear();                                          // clear HashMap
            scoreToDocID.clear();                                       // clear HashMap
            scoreWithMaxDoc.clear();                                    // clear HashMap

        } catch (IOException e) {
            e.printStackTrace();
        }

        return rankedResults;           // return the ranked results of query
    }

    /**
     * function that checks whether all files and resources required to execute the query are available
     *
     * @return  true -> if all checks are passed,then can proceed with the execution of a query
     *          false -> if at least one check is failed (one file missed), then can't proceed with the execution of a query
     */
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
            dictionary.readDictionaryFromDisk();
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

    /**
     * function for apply the Document at a Time algorithm
     *
     * @param processedQuery    array list for containing the query term
     * @param isConjunctive     indicates whether the query is of conjunctive type
     * @param isDisjunctive     indicates whether the query is of disjunctive type
     */
    private static void DAATAlgorithm(ArrayList<String> processedQuery, boolean isConjunctive, boolean isDisjunctive, int numberOfResults) throws FileNotFoundException {

        ArrayList<Integer> ordListDID;          // ordered list of the DocID present in the all posting lists of the term present in the query
        ArrayList<Posting>[] postingLists;      // contains all the posting lists for each term of the query

        Posting currentP;                       // support var
        int currentDID = 0;                     // DID of the current doc processed in algorithm
        int[] postingListsIndex;                // contain the current position index for the posting list of each term in the query

        double partialScore = 0;                // var that contain partial score

        long startTime,endTime;                 // variables to calculate the execution time


        postingLists = retrieveAllPostListsFromQuery(processedQuery);   // take all posting lists of query terms

        // control check for empty posting lists (the terms are not present in the document collection)
        if (postingLists.length == 0)
        {
            printUI("The term in query there aren't in collection.");
            return;     // exit to function
        }

        ordListDID = DIDOrderedListOfQuery(postingLists);           // take ordered list of DocID
        postingListsIndex = getPostingListsIndex(postingLists);     // get the index initialized   NEW VERSION

        startTime = System.currentTimeMillis();           // end time of hash map ordering

        // scan all Doc retrieved and calculate score TFIDF
        for (Integer integer : ordListDID) {

            currentDID = integer;     // update the DID, document of which to calculate the score
            partialScore = 0;                   // reset var

            // default case is query Disjunctive
            // take all values and calculating the scores in the posting related to currentDID
            for (int j = 0; j < postingLists.length; j++) {
                // check if the posting lists of j-th isn't at the end AND if the j-th term of the query is present in the doc identify by currentDID
                if ((postingListsIndex[j] < postingLists[j].size()) && (postingLists[j].get(postingListsIndex[j]).getDocId() == currentDID)) {
                    currentP = postingLists[j].get(postingListsIndex[j]);              // take posting
                    postingListsIndex[j]++;                         // update index of current value

                    //System.out.println("DAAT, prescoring -- df = " + DataStructureHandler.postingListLengthFromTerm(processedQuery.get(j)));

                    // calculate TFIDF for this term and currentDID and sum to partial score
                    String term = processedQuery.get(j);
                    assert term != null;
                    int df = dictionary.getTermToTermStat().get(term).getDf();
                    partialScore += ScoringTFIDF(currentP.getTermFreq(), df);

                    printDebug("DAAT: posting del termine: " + processedQuery.get(j) + " in array pos: " + j + " ha DID: " + currentDID + " and partialScore: " + partialScore);
                } else if (isConjunctive) {
                    // must take only the document in which there are all term (DID that compare in all posting lists of the terms)
                    partialScore = 0;       // reset the partial score
                    // if all postings in one posting lists have already been seen the next documents in the posting lists cannot contain all the terms in the query
                    if (postingListsIndex[j] >= postingLists[j].size()) {
                        endTime = System.currentTimeMillis();           // end time of hash map ordering
                        // shows query execution time
                        System.out.println(ANSI_YELLOW + "\n*** DAAT execute in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
                        return;             // exit from function
                    } else
                        break;              // exit from the for and go to next Document
                }
            }

            // save score
            if (partialScore != 0) {
                //tableDAAT.put(currentDID,partialScore);     // add DID and related score to HashMap   OLD VERSION
                if (!scoreWithMaxDoc.containsKey(partialScore)) {
                    tableDAAT.put(currentDID, partialScore);     // add DID and related score to HashMap     NEW VERSION
                    addToScoreToDocID(partialScore, currentDID, numberOfResults); // add DID to the related DID in hashmap
                }
                printDebug("Final TFIDF scoring for DID = " + currentDID + " is: " + tableDAAT.get(currentDID));
            }
        }

        endTime = System.currentTimeMillis();           // end time of hash map ordering
        // shows query execution time
        System.out.println(ANSI_YELLOW + "\n*** DAAT execute in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
    }

    /**
     * function to calculate TFIDF for one term and one document
     *
     * @param termFreq          term frequency of the term in the document
     * @param postListLength    number of documents in which the term occurs
     * @return  the TFIDF score for one term and one document. The total score for a document will be the sum of the
     *          result of this function for each term that is both in the document and in the query
     */
    private static Double ScoringTFIDF(int termFreq, int postListLength)
    {
        double TFweight, IDFweight, scoreTFIDF;     // variables to calculate the TFIDF score value

        // control to avoid log and division to 0
        if (termFreq == 0 || postListLength == 0)
            return (double) 0;

        TFweight = (1 + Math.log10(termFreq));      // calculate TF weight
        IDFweight = Math.log10(((double) CollectionStatistics.getNDocs() / postListLength));    // calculate IDF weight
        scoreTFIDF = TFweight * IDFweight;          // calculate TFIDF weight from Tf and IDF weight values

        printDebug("ScoringTFIDF - TFweight = " + TFweight + " IDFweight = " + IDFweight + " scoreTFIDF = " + scoreTFIDF);

        return scoreTFIDF;
    }

    // -------- start: utilities function --------

    /**
     * function to elaborate all docs and related scores to obtain the ranked list of results
     *
     * @param numResults    number of result(DocID) to return to the user
     * @return  an ordered ArrayList that represent the top numResults results for the query
     */
    private static ArrayList<Integer> getRankedResults(int numResults)
    {
        ArrayList<Integer> rankedResults = new ArrayList<>();   // array list to contain the top "numResults" docs
        ArrayList<Double> orderedList = new ArrayList<>();      // contain scores of all docs
        long startTime, endTime;            // variables to calculate the execution time

        //control check
        if (numResults < 0 || tableDAAT.isEmpty())
            return rankedResults;

        printDebug("HashMAp: " + tableDAAT);

        // take ranked list of DocID
        for (Map.Entry<Integer, Double> entry : tableDAAT.entrySet()) {
            orderedList.add(entry.getValue());
        }

        System.out.println("\n*** REMOVE DUPLICATES orderedLISt - before size: " + orderedList.size());
        startTime = System.currentTimeMillis();         // start time of hash map ordering
        Set<Double> set = new HashSet<>(orderedList);
        orderedList.clear();
        orderedList.addAll(set);
        endTime = System.currentTimeMillis();           // end time of hash map ordering
        System.out.println(ANSI_YELLOW + "\n*** REMOVE DUPLICATE orderedList in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
        System.out.println("\n*** REMOVE DUPLICATES orderedLISt - after size: " + orderedList.size());

        startTime = System.currentTimeMillis();         // start time of hash map ordering
        orderedList.sort(Collections.reverseOrder());
        endTime = System.currentTimeMillis();           // end time of hash map ordering
        System.out.println(ANSI_YELLOW + "\n*** ORDER orderedList in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

        printDebug("Order results...");
        // true in testing phase -> order and show all results (required long time)
        if (orderAllHashMap)
        {
            startTime = System.currentTimeMillis();         // start time of hash map ordering
            for (double num : orderedList) {
                for (Map.Entry<Integer, Double> entry : tableDAAT.entrySet()) {
                    if (entry.getValue() == num && !rankedResults.contains(entry.getKey())) {
                        rankedResults.add(entry.getKey());
                    }
                }
            }

            printDebug("Total ranked results: " + rankedResults);

            // if the ranked results are more than numResults, cut the last results
            if (rankedResults.size() > numResults)
            {
                List<Integer> ord = rankedResults.subList(0,numResults);    // retrieve only the first numResults DocID
                rankedResults = new ArrayList<>(ord);
                System.out.println("Cut ranked results: " + rankedResults);
            }
            endTime = System.currentTimeMillis();           // end time of hash map ordering
            // shows query execution time
            printTime("\n*** TOTAL HashMap ordered in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
        }
        else
        {
            /*// old version
            startTime = System.currentTimeMillis();         // start time of hash map ordering
            for (double num : orderedList) {
                for (Map.Entry<Integer, Double> entry : tableDAAT.entrySet()) {
                    if (entry.getValue() == num && !rankedResults.contains(entry.getKey())) {
                        rankedResults.add(entry.getKey());
                        if (iterator < numResults)
                            iterator++;
                        else
                        {
                            endTime = System.currentTimeMillis();           // end time of hash map ordering
                            // shows query execution time
                            printTime("\n*** PARTIAL HashMap ordered in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")");
                            return rankedResults;
                        }
                    }
                }
            }
            //*/

            //new version
            int iterator = 1;                   // iterator to stop the ordering
            ArrayList<Integer> retrievDocIDs;   // list to get the arraylist of DocID related to a score

            startTime = System.currentTimeMillis();         // start time of hash map ordering
            //remove duplicate
            for (double num : orderedList)
            {
                if (scoreToDocID.containsKey(num))
                {
                    // take DocID of the document that have num as score
                    retrievDocIDs = scoreToDocID.get(num);
                    scoreToDocID.remove(num);
                    System.out.println("\n*** retrieveDocIDs size: " + retrievDocIDs.size() + "\nretrieveDocIDs array list: " + retrievDocIDs);
                    // scan all DocID retrieved
                    for (Integer i : retrievDocIDs)
                    {
                        rankedResults.add(i);
                        if (iterator < numResults)
                            iterator++;
                        else
                        {
                            endTime = System.currentTimeMillis();           // end time of hash map ordering
                            // shows query execution time
                            System.out.println(ANSI_YELLOW + "\n*** PARTIAL HashMap ordered in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
                            return rankedResults;
                        }
                    }
                }
                else
                {
                    System.out.println(ANSI_RED + "ERROR in scoreToDocID hashMap there isn't a Doc for the score: " + num + ANSI_RESET);
                }
            }
            //*/
        }

        return rankedResults;
    }

    /**
     * function to retrieve all the posting lists for each term of the query passed as parameter
     *
     * @param processedQuery    ArrayList of the processed ter of the query
     * @return  an array of posting lists (ArrayList of posting). the array has length equal to the number of terms,
     *          and the i-th position in the array contains the posting list of the i-th term in the processedQuery
     */
    private static ArrayList<Posting>[] retrieveAllPostListsFromQuery(ArrayList<String> processedQuery)
    {
        // array of arrayList (posting list) that contain all the posting lists for each term in the query
        ArrayList<Posting>[] postingLists = new ArrayList[processedQuery.size()];
        int iterator = 0;               // iterator for saving posting lists term in correct position

        try(
            // open complete files to read the postingList
            RandomAccessFile docidFile = new RandomAccessFile(DOCID_FILE, "rw");
            RandomAccessFile termfreqFile = new RandomAccessFile(TERMFREQ_FILE, "rw");

            // FileChannel
            FileChannel docIdChannel = docidFile.getChannel();
            FileChannel termFreqChannel = termfreqFile.getChannel()
        ) {
            // take posting list for each term in query
            for (String term : processedQuery)
            {
                printDebug("DAAT: retrieve posting list of  " + term);

                DictionaryElem de = dictionary.getTermToTermStat().get(term);

                if (dictionary.getTermToTermStat().containsKey(term))

                    if(Flags.isCompressionEnabled())
                        postingLists[iterator] = readCompressedPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(), de.getTermFreqSize(), de.getDocIdSize(), de.getDf(), docIdChannel, termFreqChannel); //read compressed posting list
                    else // take the postingList of term
                        postingLists[iterator] = readPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(),de.getDf(),docIdChannel,termFreqChannel);

                iterator++;                 // update iterator
            }
            return postingLists;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * function that given the posting lists of each term in a given query returns an ordered list of the DocIDs
     * present in the all posting lists
     *
     * @param postingLists  the posting lists of each term in the query
     * @return  an ordered ArrayList of the DocIDs in the posting lists
     */
    private static ArrayList<Integer> DIDOrderedListOfQuery(ArrayList<Posting>[] postingLists) throws FileNotFoundException {

        // ordered list of the DocID present in the all posting lists of the term present in the query
        ArrayList<Integer> orderedList = new ArrayList<>();
        LinkedHashMap<Integer, Integer> hashDocID = new LinkedHashMap<>();  //hashmap to get all DocID without copies
        long startTime,endTime;                 // variables to calculate the execution time

        // /* OLD VERSION -- start
        int currentDocID = 0;                                // var to contain the current DocID

        startTime = System.currentTimeMillis();         // start time to take th DocID list
        // scan all posting lists passed as parameters
        for (int i = 0; i < postingLists.length; i++)
        {
            // scan all DocID in the i-th posting list
            for (Posting p : postingLists[i])
            {
                currentDocID = p.getDocId();            // take DocID in the current posting
                if (i == 0)
                    hashDocID.put(currentDocID,1);      // add DocID
                // control check for duplicate DocID, do only after first posting list
                else if (!hashDocID.containsKey(currentDocID))
                {
                    hashDocID.put(currentDocID,1);      // add DocID
                }
            }
        }
        for (Map.Entry<Integer, Integer> entry : hashDocID.entrySet()) {
            orderedList.add(entry.getKey());
        }
        endTime = System.currentTimeMillis();          // end time to take th DocID list
        System.out.println(ANSI_YELLOW + "\n*** TAKE DID LIST (no PQ) in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

        startTime = System.currentTimeMillis();         // start time of DocID list ordering
        Collections.sort(orderedList);          // order the list of DocID
        endTime = System.currentTimeMillis();           // end time of DocID list ordering
        // shows query execution time
        System.out.println(ANSI_YELLOW + "\n*** ORDERED DID LIST (no PQ) in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

        printDebug("Ordered List of DocID for the query:  " + orderedList);     // print orderedList

        System.out.println("Ordered List (no PQ) of DocID dim: " + orderedList.size());     // print orderedList
        hashDocID.clear();          // clear linkHashMap
        // */ OLD VERSION - end

        ///* NEW VERSION (priority queue) - start
        // clear the previus work without PQ
        orderedList = new ArrayList<>();
        hashDocID.clear();

        // create PQ
        //PriorityQueue<QueryProcessor.PostingBlock> pq = new PriorityQueue<>(postingLists.length, new CompareTerm());
        pq = new PriorityQueue<>(postingLists.length, new CompareTerm());
        int[] postingListsIndex = getPostingListsIndex(postingLists); // contain the current position index for the posting list of each term in the query
        QueryProcessor.PostingBlock currentPostingBlock;     // var that contain the PostBlock extract from pq in the current iteration

        startTime = System.currentTimeMillis();         // start time to take th DocID list
        // take first DocID from posting lists
        for (int i = 0; i < postingLists.length; i++)
        {
            pq.add(new QueryProcessor.PostingBlock(postingLists[i].get(0).getDocId(), i));     // add to the priority queue a TermBlock element (term + its blocks number)
        }

        while(!pq.isEmpty()) //
        {
            //qSystem.out.println("PQ:\n" + pq);               // print priority queue
            currentPostingBlock = pq.poll();                // take lowest element (DID and index)
            currentDocID = currentPostingBlock.getDID();
            hashDocID.put(currentDocID,1);  // put DocID in hashtable
            // scann all current position in posting list and update indexes
            for (int i = 0; i < postingLists.length; i++)
            {
                // check if the DocID in the posting list is the same in currentPostingBlock and check if there is another posting in the posting lists
                if ( (postingListsIndex[i] < postingLists[i].size()) && (currentDocID == postingLists[i].get(postingListsIndex[i]).getDocId()) )
                {
                    // check if is the posting list of the currentPostingBlock
                    if ( i != currentPostingBlock.getIndex())
                    {
                        pq.poll();                  // remove one element in pq
                    }
                    postingListsIndex[i]++;     // update index
                    // check if there is another posting in the posting lists
                    if (postingListsIndex[i] < postingLists[i].size())
                        pq.add(new QueryProcessor.PostingBlock(postingLists[i].get(postingListsIndex[i]).getDocId(), i));  // insert new posting in pq
                }
            }
        }
        // pass from hashMap to ArrayList
        for (Map.Entry<Integer, Integer> entry : hashDocID.entrySet()) {
            orderedList.add(entry.getKey());
        }
        endTime = System.currentTimeMillis();           // end time of DocID list ordering
        // shows query execution time
        System.out.println(ANSI_YELLOW + "\n*** TAKE AND ORDERED DID LIST (PQ) in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

        System.out.println("Ordered List (PQ) of DocID dim: " + orderedList.size());     // print orderedList
        // NEW VERSION (priority queue) - end
        // */

        return orderedList;
    }

    /**
     * function to put in hashMap the document related its score
     *
     * @param score             score of the document
     * @param DocID             DocID of the document
     * @param numberOfResults   maximum number of document to keep for each score
     */
    private static void addToScoreToDocID (double score, int DocID,int numberOfResults)
    {
        ArrayList<Integer> values;

        if (scoreToDocID.containsKey(score))        // contains key, add DocID to the arrayList
        {
            if (scoreToDocID.get(score).size() >= numberOfResults)      // SEE NOTE 0
            {
                scoreWithMaxDoc.put(score,true);                        // SEE NOTE 1
                return;
            }
            // get value from HashMap
            ArrayList<Integer> oldValues = scoreToDocID.get(score);
            //System.out.println("*** addToScoreToDocID: old value = " + scoreToDocID.get(score) + " for score: " + score);
            values = new ArrayList<>(oldValues.subList(0,oldValues.size()));
            values.add(DocID);                  // add current DID
            scoreToDocID.put(score,values);     // update to hashMap
            //System.out.println("*** addToScoreToDocID: new value = " + scoreToDocID.get(score) + " for score: " + score);
        }
        else        // First DocID create an arrayList with one element
        {
            values = new ArrayList<>();
            values.add(DocID);
            scoreToDocID.put(score,values);     // add to hashMap
            //System.out.println("*** addToScoreToDocID: new value = " + scoreToDocID.get(score) + " for score: " + score);
        }
    }

    // new version to substitute remove wit get in the posting lists

    /**
     * function to create an array of indexes for posting lists
     *
     * @param postingLists  the posting lists of each term in the query
     * @return  an array that contains the index for the current posting (position) for each posting lists of the term
     *          in the query
     */
    private static int[] getPostingListsIndex (ArrayList<Posting>[] postingLists)
    {
        int[] postingListsIndex = new int[postingLists.length];

        // set the index to 0 for each posting lists of the term in the query
        for (int i = 0; i < postingLists.length; i++)
        {
            postingListsIndex[i] = 0;       // set index to 0
        }

        return postingListsIndex;
    }
    // -------- end: utilities function --------

    // -------- start: utilities for priority queue --------
    /**
     * class to define PostingBlock. The priority queue contains instances of PostingBlock
     */
    private static class PostingBlock {
        int DocID;                  // DocID
        int indexOfPostingList;     // reference to the posting list (index in the array of posting lists of the query) containing DcoID

        // constructor with parameters
        public PostingBlock(int DocID, int indexOfPostingList) {
            this.DocID = DocID;
            this.indexOfPostingList = indexOfPostingList;
        }

        public int getDID() {
            return DocID;
        }

        public int getIndex() {
            return indexOfPostingList;
        }

        @Override
        public String toString() {
            return "PB{" +
                    "DocID = '" + DocID + '\'' +
                    ", index of pl =" + indexOfPostingList +
                    '}';
        }
    }

    /**
     * class to compare the block, allows the order of the priority queue
     */
    private static class CompareTerm implements Comparator<QueryProcessor.PostingBlock> {
        @Override
        public int compare(QueryProcessor.PostingBlock pb1, QueryProcessor.PostingBlock pb2) {
            // comparing terms
            int DocIDComparison = Integer.compare(pb1.getDID(), pb2.getDID());
            // if the DocID are equal, compare by block number
            if (DocIDComparison == 0) {
                return Integer.compare(pb1.getIndex(), pb2.getIndex());
            }

            return DocIDComparison;
        }
    }
    // -------- end: utilities for priority queue --------
}

/*
 * NOTE:
 * 0 - The idea behind the reasoning is that if I have to return to the user the "numberOfResults" documents with the
 *     best result, if for each result I collect at least the first "numberOfResults" documents with that result and
 *     don't register the others at the end I will have given the same best "numberOfResults" results as if I had
 *     registered all the documents.
 * 1 - the maximum required number of documents (numberOfResults) have been scored, advise don't take any more
 *     documents with this score
 */