package it.unipi.dii.aide.mircv;
import it.unipi.dii.aide.mircv.data_structures.DataStructureHandler;
import it.unipi.dii.aide.mircv.data_structures.DocumentElement;
import it.unipi.dii.aide.mircv.data_structures.Posting;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static it.unipi.dii.aide.mircv.utils.Constants.*;

/**
 * This class
 */
public class QueryProcessor {

    // HashMap for containing the DocID and sum of Term Frequency related. DID -> sTermFreq
    private static final HashMap<Integer, Integer> tableDAAT = new HashMap<>();

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

            DAATAlgorithm(processedQuery,isConjunctive, isDisjunctive);        // apply DAAT, result in tableDAAT

            // apply scoring function

            // ranked result

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

        return true;        // there are all
    }

    /**
     * function for apply the Document at a Time algorithm
     *
     * @param ProcessedQuery    array list for containing the query term
     * @param isConjunctive     indicates whether the query is of conjunctive type
     * @param isDisjunctive     indicates whether the query is of disjunctive type
     */
    private static void DAATAlgorithm(ArrayList<String> ProcessedQuery,boolean isConjunctive, boolean isDisjunctive)
    {
        ArrayList<Posting>[] postings = new ArrayList[ProcessedQuery.size()];   //
        int iterator = 0;               // iterator for saving posting lists term in correct position

        // take posting list for each term in query
        for (String term : ProcessedQuery)
        {
            // TO DO +++++++++++++++++            // take posting list related term
            //postings[iterator] =
            iterator++;                 // update iterator
            if (verbose)
                System.out.println("DAAT: retrieve posting list of  " + term);
        }
        iterator = 0;       // reset iterator

        // read and sum TermFrequency for each document in the posting list
        // iterate until there are no more postings to analyse
        while(true)
        {
            if (isConjunctive)          // query is conjunctive
            {

            }
            else if (isDisjunctive)     // query is isDisjunctive
            {

            }
            break;
        }
    }

    // function to scoring and retrieve the ranked results from values obtained by DAAT algorithm
    private static ArrayList<Integer> ScoringTFIDF()
    {
        ArrayList<Integer> rankedResults = new ArrayList<>();

        return rankedResults;
    }
}

/**
 * implementazione (ranked retrieval)
 * 1) Conjunctive (AND) vs. Disjunctive (OR)
 * 2.1) Term At Time: scorro la postings di ogni termine e segno il punteggio di ogni documento
 * 2.2) Doc At Time: altra strategia è attraversare in parallelo le posting list dei vari termini in contemporanea.
 *      prendo il documento con il DID più piccolo, guardo in tutte le posting list e per quel documento calcolo il
 *      punteggio totale, finale.
 * Usare coda prioritaria per mettere doc con punteggio e poi prenderli (ci saranno casi di parità, gestirli)
 * ) Scoring function: TFIDF -> Wtd = (1 + log (tf_td)) * log(N/df_t)  se tf_td > 0  sennò 0
 *      dove TFtd = term frequency del term t in document d
 *      dove N = numero totale dei documenti    e   df_t = numero dei documenti che contengono t
 */


/**
 * Specification
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

/**
 * Note:
 * For calculate scores:
 * - TAAT: pros -> simple, cache-friendly       cons -> a lot of memory for partial scores, difficult in boolean or phrasal query
 * - DAAT: pros -> requires smaller memory than TAAT, support boolean and phrasal query
 *         cons -> lesser cache-friendly than TAAT
 */