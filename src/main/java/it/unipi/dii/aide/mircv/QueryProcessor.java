package it.unipi.dii.aide.mircv;
import it.unipi.dii.aide.mircv.data_structures.DataStructureHandler;
import it.unipi.dii.aide.mircv.data_structures.DocumentElement;
import it.unipi.dii.aide.mircv.data_structures.Posting;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.*;

import static it.unipi.dii.aide.mircv.utils.Constants.*;

/**
 * Class to manage and execute query
 */
public class QueryProcessor {

    // used for the test
    private static boolean ordereAllHashMAp = false;        // indicate whether order all or only first "numberOfResults" results from hash table
    // HashMap for containing the DocID and sum of Term Frequency related. DID -> sTermFreq
    private static final HashMap<Integer, Double> tableDAAT = new HashMap<>();
    private static final HashMap<Double, ArrayList<Integer>> scoreToDocID = new HashMap<>();
    private static final HashMap<Double, Boolean> scoreWithMaxDoc = new HashMap<>();

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

            DAATAlgorithm(processedQuery,isConjunctive, isDisjunctive,numberOfResults);        // apply DAAT, result in tableDAAT

            rankedResults = getRankedResults(numberOfResults);          // get ranked results
            tableDAAT.clear();                                          // clear HashMap
            scoreToDocID.clear();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return rankedResults;           // return the ranked results of query
    }

    /**
     * function that checks whether all files and resources required to execute the query are available
     *
     * @return  true -> if all checks are passed,then can proceed with the execution of a query
     *          false -> if at least one check is failed, then can't proceed with the execution of a query
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
            System.out.println(ANSI_RED + "Error: dictionary in memory isn't set." + ANSI_RESET);  // mex of error
            return false;
        }

        return true;        // there are all
    }

    /**
     * function for apply the Document at a Time algorithm
     *
     * @param ProcessedQuery    array list for containing the query term
     * @param isConjunctive     indicates whether the query is of conjunctive type
     * @param isDisjunctive     indicates whether the query is of disjunctive type
     */
    private static void DAATAlgorithm(ArrayList<String> ProcessedQuery,boolean isConjunctive, boolean isDisjunctive, int numberOfResults)
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

        long startTime = System.currentTimeMillis();           // end time of hash map ordering

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

                    if (verbose)
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
                //if (scoreWithMaxDoc.containsKey(partialScore))
                //    addToScoreToDocID(partialScore,currentDID,numberOfResults); // add score and related DID to HashMap
                if (verbose)
                    System.out.println("Final TFIDF scoring for DID = " + currentDID + " is(HasMap): " + tableDAAT.get(currentDID) + " is(var): " + partialScore);
            }
        }

        long endTime = System.currentTimeMillis();           // end time of hash map ordering
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
        double TFweight;
        double IDFweight;
        double scoreTFIDF;

        // control to avoid log and division to 0
        if (termFreq == 0 || postListLength == 0)
            return (double) 0;

        TFweight = (1 + Math.log10(termFreq));
        IDFweight = Math.log10(((double) DataStructureHandler.collection.getnDocs() / postListLength));
        scoreTFIDF = TFweight * IDFweight;

        if(verbose)
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
        ArrayList<Double> orderedList = new ArrayList<>();
        long startTime;
        long endTime;

        //control check
        if (numResults < 0 || tableDAAT.isEmpty())
            return rankedResults;

        if(verbose)
            System.out.println("HashMAp: " + tableDAAT);

        // take ranked list of DocID
        for (Map.Entry<Integer, Double> entry : tableDAAT.entrySet()) {
            orderedList.add(entry.getValue());
        }
        //orderedList.sort(Collections.reverseOrder());     // old version

        System.out.println("Order results...");
        // true in testing phase -> order and show all results (required long time)
        if (ordereAllHashMAp)
        {
            startTime = System.currentTimeMillis();         // start time of hash map ordering

            //orderedList.sort(Collections.reverseOrder());       // new version
            for (double num : orderedList) {
                for (Map.Entry<Integer, Double> entry : tableDAAT.entrySet()) {
                    if (entry.getValue() == num && !rankedResults.contains(entry.getKey())) {
                        rankedResults.add(entry.getKey());
                    }
                }
            }

            if (verbose)
                System.out.println("Total ranked results: " + rankedResults);

            // if the ranked results are more than numResults, cut the last results
            if (rankedResults.size() > numResults)
            {
                List<Integer> ord = rankedResults.subList(0,numResults);    // retrieve only the first numResults DocID
                rankedResults = new ArrayList<>(ord);
                if (verbose)
                    System.out.println("Cut ranked results: " + rankedResults);
            }
            endTime = System.currentTimeMillis();           // end time of hash map ordering
            // shows query execution time
            System.out.println(ANSI_YELLOW + "\n*** TOTAL HashMap ordered in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
        }
        else
        {
            int iterator = 1;                   // iterator to stop the ordering

            // old version
            /*
            System.out.println("\n*** REMOVE DUPLICATES orderedLISt - before size: " + orderedList.size());
            startTime = System.currentTimeMillis();         // start time of hash map ordering
            Set<Double> set = new HashSet<>(orderedList);
            orderedList.clear();
            orderedList.addAll(set);
            endTime = System.currentTimeMillis();           // end time of hash map ordering
            System.out.println(ANSI_YELLOW + "\n*** REMOVE DUPLICATE orderedList in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
            */
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
                            System.out.println(ANSI_YELLOW + "\n*** PARTIAL HashMap ordered in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
                            return rankedResults;
                        }
                    }
                }
            }
            //*/

            /*
            //new version
            ArrayList<Integer> retrievDocIDs;

            System.out.println("\n*** REMOVE DUPLICATES orderedLISt - before size: " + orderedList.size());
            startTime = System.currentTimeMillis();         // start time of hash map ordering
            Set<Double> set = new HashSet<>(orderedList);
            orderedList.clear();
            orderedList.addAll(set);
            endTime = System.currentTimeMillis();           // end time of hash map ordering
            System.out.println(ANSI_YELLOW + "\n*** REMOVE DUPLICATE orderedList in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

            startTime = System.currentTimeMillis();         // start time of hash map ordering
            orderedList.sort(Collections.reverseOrder());
            endTime = System.currentTimeMillis();           // end time of hash map ordering
            System.out.println(ANSI_YELLOW + "\n*** ORDER orderedList in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

            System.out.println("\n*** REMOVE DUPLICATES orderedLISt - after size: " + orderedList.size());

            startTime = System.currentTimeMillis();         // start time of hash map ordering
            //remove duplicate
            for (double num : orderedList)
            {
                if (scoreToDocID.containsKey(num))
                {
                    // take DocID of the document that have num as score
                    retrievDocIDs = scoreToDocID.get(num);
                    scoreToDocID.remove(num);
                    System.out.println("\n*** retrieveDocIDs size: " + retrievDocIDs.size());
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
            }
            */
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

        // take posting list for each term in query
        for (String term : ProcessedQuery)
        {
            if (verbose)
                System.out.println("DAAT: retrieve posting list of  " + term);
            postingLists[iterator] = DataStructureHandler.getPostingListFromTerm(term); // take posting list related term
            iterator++;                 // update iterator
        }

        return postingLists;
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
        HashMap<Integer, Integer> hashDocID = new HashMap<>();
        int currentDocID = 0;                                   //

        // new version
        long startTime = System.currentTimeMillis();         // start time of DocID list ordering

        // scan all posting lists passed as parameters
        for (int i = 0; i < postingLists.length; i++)
        {
            // scan all DocID in the i-th posting list
            for (Posting p : postingLists[i])
            {
                currentDocID = p.getDocId();            // take DocID in the current posting
                // control check for duplicate DocID, do only after first posting list
                if (!hashDocID.containsKey(currentDocID))
                {
                    hashDocID.put(currentDocID,1);      // add DocID
                }
            }
        }
        for (Map.Entry<Integer, Integer> entry : hashDocID.entrySet()) {
            orderedList.add(entry.getKey());
        }
        long endTime = System.currentTimeMillis();           // end time of DocID list ordering
        System.out.println(ANSI_YELLOW + "\n*** TAKE DID LIST in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

        /* old version
        long startTime = System.currentTimeMillis();         // start time of DocID list ordering

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
        long endTime = System.currentTimeMillis();           // end time of DocID list ordering
        System.out.println(ANSI_YELLOW + "\n*** TAKE DID LIST in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);
        */
        startTime = System.currentTimeMillis();         // start time of DocID list ordering
        Collections.sort(orderedList);          // order the list of DocID
        endTime = System.currentTimeMillis();           // end time of DocID list ordering
        // shows query execution time
        System.out.println(ANSI_YELLOW + "\n*** ORDERED DID LIST in " + (endTime - startTime) + " ms (" + formatTime(startTime, endTime) + ")" + ANSI_RESET);

        if (verbose)
            System.out.println("Ordered List of DocID for the query:  " + orderedList);     // print orderedList

        hashDocID.clear();

        return orderedList;
    }

    private static void addToScoreToDocID (double score, int DocID,int numberOfResults)
    {
        ArrayList<Integer> values;

        if (scoreToDocID.containsKey(score))        // contains key, add DocID to the arrayList
        {
            if (scoreToDocID.get(score).size() >= numberOfResults)   // SEE NOTE 0
            {
                scoreWithMaxDoc.put(score,true);        // SEE NOTE 1
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

    // -------- end: utilities function --------
}

/*
* NOTE:
* 0 -
* 1 - the maximum required number of documents (numberOfResults) have been scored this way, I advise not to take
*     any more documents with this score
*/

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