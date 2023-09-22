package it.unipi.dii.aide.mircv;
import it.unipi.dii.aide.mircv.data_structures.DataStructureHandler;
import it.unipi.dii.aide.mircv.data_structures.DocumentElement;
import it.unipi.dii.aide.mircv.data_structures.Posting;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.*;

import static it.unipi.dii.aide.mircv.data_structures.CollectionStatistics.readCollectionStatsFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.readPostingListFromDisk;
import static it.unipi.dii.aide.mircv.data_structures.Flags.readFlagsFromDisk;
import static it.unipi.dii.aide.mircv.utils.Constants.*;
/**
 * Class to manage and execute query
 */
public class QueryProcessor {

    // used for the test
    private static boolean ordereAllHashMAp = false;        // indicate whether order all or only first "numberOfResults" results from hash table
    // HashMap for containing the DocID and sum of Term Frequency related. DID -> sTermFreq
    private static final HashMap<Integer, Double> tableDAAT = new HashMap<>();

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
        ArrayList<String> processedQuery;
        ArrayList<Posting> postingList = new ArrayList<>();     // contain the posting list of term
        DictionaryElem de;// array list for containing the query term

        // take user's choices that affecting the query execution
        DataStructureHandler.readFlagsFromDisk();           // take from disk
        boolean ScoringFunc = Flag.isScoringEnabled();      // take user's choice about using scoring function ...

        try{
            // processed the query to obtain the term
            System.out.println("Query before processed: " + query);
            processedQuery = TextProcessor.preprocessText(query); // Preprocessing of document text
            System.out.println("Query after processed: " + processedQuery);

            // control for correct form
            if ( (isConjunctive && isDisjunctive) || !(isConjunctive || isDisjunctive))     // query is Conjunctive or Disjunctive cannot be both or neither
            {
                System.out.println(ANSI_RED + "Error: query is Conjunctive or Disjunctive cannot be both or neither." + ANSI_RESET);  // mex of error
                return rankedResults;
            }

            DAATAlgorithm(processedQuery,isConjunctive, isDisjunctive);        // apply DAAT, result in tableDAAT

            rankedResults = getRankedResults(numberOfResults);          // get ranked results
            tableDAAT.clear();                                          // clear HashMap

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
    public static boolean queryStartControl()
    {
        // -- control for file into disk
        // check if there are all merged files into disk
        if(!DataStructureHandler.areThereAllMergedFiles())
        {
            System.out.println(ANSI_RED + "Error: there aren't all files in 'Merged' folder." + ANSI_RESET);  // mex of error
            return false;
        }

        // check if there is the file for user choices of flags
        if (!DataStructureHandler.isThereFlagsFile())
        {
            System.out.println(ANSI_RED + "Error: there isn't the file for user's choices of flags." + ANSI_RESET);  // mex of error
            return false;
        }

        // check if there is the file for the statistics of the collection
        if (!DataStructureHandler.isThereStatsFile())
        {
            System.out.println(ANSI_RED + "Error: there isn't the file for the collection statistics." + ANSI_RESET);  // mex of error
            return false;
        }

        // -- control for structures in memory
        // check if dictionary in memory is set
        if (!DataStructureHandler.dictionaryIsSet())
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
     * @param ProcessedQuery    array list for containing the query term
     * @param isConjunctive     indicates whether the query is of conjunctive type
     * @param isDisjunctive     indicates whether the query is of disjunctive type
     */
    private static void DAATAlgorithm(ArrayList<String> ProcessedQuery, boolean isConjunctive, boolean isDisjunctive)
    {
        // ordered list of the DocID present in the all posting lists of the term present in the query
        ArrayList<Integer> ordListDID;
        ArrayList<Posting>[] postingLists;      // contains all the posting lists for each term of the query
        Posting currentP;                       // support var
        int currentDID = 0;                     // DID of the current doc processed in algorithm
        double partialScore = 0;                   // var that contain partial score

        postingLists = retrieveAllPostListsFromQuery(ProcessedQuery);   // take all posting lists of query terms
        // control check for empty posting lists (the terms are not present in the document collection)
        if (postingLists.length == 0)
        {
            System.out.println(ANSI_CYAN + "The term in query there aren't in collection." + ANSI_RESET);
            return;     // exit to function
        }

        ordListDID = DIDOrderedListOfQuery(postingLists);               // take ordered list of DocID

        // scan all Doc retrieved and calculate score TFIDF
        for (int i = 0; i < ordListDID.size(); i++)
        {
            currentDID = ordListDID.get(i);     // update the DID, document of which to calculate the score
            partialScore = 0;                   // reset var

            // default case is query Disjunctive
            // take all values and calculating the scores in the posting related to currentDID
            for (int j = 0; j < postingLists.length; j++)
            {
                // check if the posting lists of j-th is empty AND if the j-th term of the query is present in the doc identify by currentDID
                if (!postingLists[j].isEmpty() && (postingLists[j].get(0).getDocId() == currentDID))
                {
                    currentP = postingLists[j].remove(0);               // take and remove posting
                    //System.out.println("DAAT, prescoring -- df = " + DataStructureHandler.postingListLengthFromTerm(ProcessedQuery.get(j)));

                    // calculate TFIDF for this term and currentDID and sum to partial score
                    partialScore += ScoringTFIDF(currentP.getTermFreq(), DataStructureHandler.postingListLengthFromTerm(ProcessedQuery.get(j)));

                    //if (verbose)
                    System.out.println("DAAT: posting del termine: " + ProcessedQuery.get(j) + " in array pos: " + j + " ha DID: " + currentDID + " and partialScore: " + partialScore);
                }
                else if (isConjunctive)
                {
                    // must take only the document in which there are all term (DID that compare in all posting lists of the terms)
                    partialScore = 0;       // reset the partial score
                    // if one posting lists is empty the next documents in the posting lists cannot contain all the terms in the query
                    if (postingLists[j].isEmpty())
                        return;             // exit from function
                    else
                        break;              // exit from the for and go to next Document
                }
            }

            // save score
            if (partialScore != 0)
            {
                tableDAAT.put(currentDID,partialScore);     // add DID and related score to HashMap
                //if (verbose)
                System.out.println("Final TFIDF scoring for DID = " + currentDID + " is: " + tableDAAT.get(currentDID));
            }

        }
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
        double TFweight;
        double IDFweight;
        double scoreTFIDF;

        // control to avoid log and division to 0
        if (termFreq == 0 || postListLength == 0)
            return (double) 0;

        TFweight = (1 + Math.log10(termFreq));
        IDFweight = Math.log10(((double) DataStructureHandler.collection.getnDocs() / postListLength));
        scoreTFIDF = TFweight * IDFweight;

        //if(verbose)
        System.out.println("ScoringTFIDF - TFweight = " + TFweight + " IDFweight = " + IDFweight + " scoreTFIDF = " + scoreTFIDF);

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
        ArrayList<Integer> rankedResults = new ArrayList<>();
        ArrayList<Double> orederedList = new ArrayList<>();

        //control check
        if (numResults < 0 || tableDAAT.isEmpty())
            return rankedResults;

        //if(verbose)
        System.out.println("HashMAp: " + tableDAAT);

        // take ranked list of DocID
        for (Map.Entry<Integer, Double> entry : tableDAAT.entrySet()) {
            orederedList.add(entry.getValue());
        }
        orederedList.sort(Collections.reverseOrder());

        // true in testing phase -> order and show all results (required long time)
        if (ordereAllHashMAp)
        {
            long startTime = System.currentTimeMillis();         // start time of hash map ordering
            for (double num : orederedList) {
                for (Map.Entry<Integer, Double> entry : tableDAAT.entrySet()) {
                    if (entry.getValue() == num && !rankedResults.contains(entry.getKey())) {
                        rankedResults.add(entry.getKey());
                    }
                }
            }
            long endTime = System.currentTimeMillis();           // end time of hash map ordering
            // shows query execution time
            System.out.println(ANSI_YELLOW + "\n*** TOTAL HashMap ordered in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

            System.out.println("Total ranked results: " + rankedResults);

            // if the ranked results are more than numResults, cut the last results
            if (rankedResults.size() > numResults)
            {
                List<Integer> ord = rankedResults.subList(0,numResults);    // retrieve only the first numResults DocID
                rankedResults = new ArrayList<>(ord);
                System.out.println("Cut ranked results: " + rankedResults);
            }
        }
        else
        {
            int iterator = 1;                   // iterator to stop the ordering
            long startTime = System.currentTimeMillis();         // start time of hash map ordering
            for (double num : orederedList) {
                for (Map.Entry<Integer, Double> entry : tableDAAT.entrySet()) {
                    if (entry.getValue() == num && !rankedResults.contains(entry.getKey())) {
                        rankedResults.add(entry.getKey());
                        if (iterator < numResults)
                            iterator++;
                        else
                            return rankedResults;
                    }
                }
            }
            long endTime = System.currentTimeMillis();           // end time of hash map ordering
            // shows query execution time
            System.out.println(ANSI_YELLOW + "\n*** PARTIAL HashMap ordered in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
        }

        return rankedResults;
    }

    /**
     * function to retrieve all the posting lists for each term of the query passed as parameter
     *
     * @param ProcessedQuery    ArrayList of the processed ter of the query
     * @return  an array of posting lists (ArrayList of posting). the array has length equal to the number of terms,
     *          and the i-th position in the array contains the posting list of the i-th term in the ProcessedQuery
     */
    private static ArrayList<Posting>[] retrieveAllPostListsFromQuery(ArrayList<String> ProcessedQuery)
    {

        // array of arrayList (posting list) that contain all the posting lists for each term iin the query
        ArrayList<Posting>[] postingLists = new ArrayList[ProcessedQuery.size()];
        int iterator = 0;               // iterator for saving posting lists term in correct position
        try(
        // open complete files to read the postingList
        RandomAccessFile docidFile = new RandomAccessFile(DOCID_FILE, "rw");
        RandomAccessFile termfreqFile = new RandomAccessFile(TERMFREQ_FILE, "rw");
        // FileChannel
        FileChannel docIdChannel = docidFile.getChannel();
        FileChannel termFreqChannel = termfreqFile.getChannel();) {
            // take posting list for each term in query
            for (String term : ProcessedQuery) {
                DictionaryElem de = dictionary.getTermToTermStat().get(term);
                if (dictionary.getTermToTermStat().containsKey(term))
                {
                    // take the postingList of term
                    postingLists[iterator] = readPostingListFromDisk(de.getOffsetDocId(),de.getOffsetTermFreq(),de.getDf(),docIdChannel,termFreqChannel);
                }
                printDebug("DAAT: retrieve posting list of  " + term);
                //postingLists[iterator] = DataStructureHandler.getPostingListFromTerm(term, docIdChannel, termFreqChannel); // take posting list related term
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
    private static ArrayList<Integer> DIDOrderedListOfQuery(ArrayList<Posting>[] postingLists)
    {
        // ordered list of the DocID present in the all posting lists of the term present in the query
        ArrayList<Integer> orderedList = new ArrayList<>();
        int currentDocID = 0;                                   //

        // scan all posting lists passed as parameters
        for (int i = 0; i < postingLists.length; i++)
        {
            // scan all DocID in the i-th posting list
            for (Posting p : postingLists[i])
            {
                currentDocID = p.getDocId();            // take DocID in the current posting
                // control check for duplicate DocID, do only after first posting list
                if (!orderedList.contains(currentDocID))
                {
                    orderedList.add(currentDocID);      // add DocID
                }
            }
        }

        Collections.sort(orderedList);          // order the list of DocID

         System.out.println("Ordered List of DocID for the query:  " + orderedList);     // print orderedList

        return orderedList;
    }

    // -------- end: utilities function --------
}

/*
 * Specification
 * in memoria si ha la document table e il dizionario
 *
 * - Obbligatorie:
 * -- query deve durare meno di 1", una volta fatta la query devo avere subito i risultati
 * -- deve utilizzare qualcosa di simile alla semplice interfaccia presentata nella classe, basata sulle operazioni openList(),
 * -- closeList(), next(), getDocid(), getFreq() su Inverted INdex. In questo modo, problemi come l'input del file e la tecnica
 *    di compressione dell'inverted index dovrebbero essere completamente completamente nascosti al processore di query di livello superiore.
 * -- Il programma deve restituire i primi 10 o 20 risultati in base alla funzione di punteggio TFIDF.
 * -- Implementare query congiuntive e disgiuntive.
 *
 * - Punto opzionale
 * -- Implementare il BM25 o altre funzioni di scoring, come ad es. quelle basate sui modelli linguistici.
 */