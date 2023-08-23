package it.unipi.dii.aide.mircv.data_structures;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static it.unipi.dii.aide.mircv.data_structures.DataStructureHandler.*;

/**
 * class to merge the InverteIndex
 */
public class IndexMerger {

    private static PriorityQueue<TermBlock> pq = new PriorityQueue<>(dictionaryBlocks.size() == 0 ? 1 : dictionaryBlocks.size(), new CompareTerm());

    public IndexMerger() {

    }

    // abaco 1 2 4
    // alveare 3
    // abaco 1 abaco 2 alveare 3 abaco 4
    // abaco 1 abaco 2 abaco 4 alveare 3

    /**
     *  function to merge the block of the inverted index
     */
    public static void mergeBlocks() {

        // 1. get blocks of dictionary from file
        // 2. open all files in read for each block (leggo da offset 1 elemento; nell'ultimo fino a channel.size())
        // 3. open one file in output for the index (risultato finale) scriviamo informazioni da lista di c
        // 4. pick all the first terms of each block and order them basing on the lexicographic order and block index
        //    -> si mette nella lista di candidati, si dovrà mantenere informazione per ogni termine in quale blocco lo
        //       si è letto (per aggiornamenti futuri della posizione di lettura)
        // 5.  merge delle posting list degli indici parziali dei termini nel relativo blocco
        //    -> (avremo lista con candidati senza ripetizioni di termini)
        // 6. prendo dalla lista candidati(ordinata) primo term in ordine lessicografico e scrivo nel file output
        //      (se tutto ordinato correttamente si avrà certezza che term sarà con valori finali e non ci sarà da aggiornarlo
        // 7. aggiorno posizione in lettura nel buffer dei blocchi da cui si è letto il primo termine della lista candidati (passo sopra)
        // 7.1. aggiorno lista candidati, tolgo il primo termine.
        // 7.2. aggiorno il vocabolario per quel termine e scrivo su file
        // 8. rieseguo dal punto 4. :
        //      finchè non è terminata la lettura di tutti i blocchi (terminata la lettura del buffer)
        //      finchè non è vuota la lista dei candidati

        int nrBlocks = DataStructureHandler.dictionaryBlocks.size();    // dictionary number
        long vocsize = 4 + 4 + 4 + 8 + 8; // Size in bytes of df, cf, termId, offset
        DataStructureHandler.getBlocksFromDisk(); // 1: get blocks of dictionary from file

        MappedByteBuffer buffer;
        // array containing the current read offset for each blocks
        ArrayList<Long> currentBlockOffset = new ArrayList<>(nrBlocks);
/*        ArrayList<Posting> mergedPosting = new ArrayList<>(); // iterate over pq to find elements to merge*/
        currentBlockOffset.addAll(dictionaryBlocks);
        //DictionaryElem de;

        //boolean isFirst = true;
        try (
                // 2: open channels for reading the partial vocabulary file, the output index file and the output vocabulary file
                FileChannel channel = new RandomAccessFile(DataStructureHandler.PARTIAL_VOCABULARY_FILE, "rw").getChannel();
                FileChannel IndexChannel = new RandomAccessFile(DataStructureHandler.INVERTED_INDEX_FILE, "rw").getChannel();
                // 3: open the file in output for the index
                FileChannel outIndexChannel = new RandomAccessFile(DataStructureHandler.INVERTED_INDEX_FILE, "w").getChannel();
                FileChannel outDictionaryChannel = new RandomAccessFile(DataStructureHandler.VOCABULARY_FILE, "w").getChannel()
        ) {
            // scroll through all blocks
            for(int i = 0; i <=  nrBlocks; i++) {
                buffer = channel.map(FileChannel.MapMode.READ_ONLY, DataStructureHandler.dictionaryBlocks.get(i), TERM_DIM); // get first term of the block
                CharBuffer.allocate(TERM_DIM); //allocate a charbuffer of the dimension reserved to term
                CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
                // 4: add the first term and block number to priority queue
                pq.add(new TermBlock(charBuffer.toString().split("\0")[0], i)); //add to the priority queue a term block element (term + its blocks number)
            }
            // build temp structures
            DictionaryElem tempDE = new DictionaryElem();       // empty temporary DictionaryELem, contains the accumulated data for each term
            PostingList tempPL = new PostingList();             //

            //5: merging the posting list
            while(!pq.isEmpty()) {

                /**
                 * abbiamo la coda prioritaria ordinata
                 * prendere il primo elemento (term, blockid) e aggiornare l'offset del blocco corrispondente
                 * - aggiornare il vocabolario finale
                 * -- primo volta term: aggiungi
                 * -- term già trovato: while finché si trova lo stesso term
                 * --- update
                 * - aggiornare l'inverted index finale
                 * -- primo elemento: aggiungi
                 * -- term già trovato: update
                 */

                TermBlock currentTermBlock = pq.poll();        // get lowest term
                String term = currentTermBlock.getTerm();
                int block_id = currentTermBlock.getBlock();
                // get current elem of dictionary
                DictionaryElem currentDE = getDictionaryElemFromDisk(currentBlockOffset.get(block_id), channel);
                // get current postings
                PostingList currentPL = DataStructureHandler.readIndexElemFromDisk(currentDE.getOffsetDocId(), currentDE.getOffsetTermFreq(), term, currentDE.getDf());

                //get all term data for that block from the vocabulary
                if (tempDE.getTermId() == 0) {        // first time term found
                    tempDE = currentDE;     // set temp DE
                    tempPL = currentPL;     // set temp PL
                } else {    // term already found
                    if (term.equals(tempDE.getTerm())) { // same term found, temporary structures update
                        // update DictionaryElem
                        tempDE.addCf(currentDE.getCf());        // update Cf
                        tempDE.addDf(currentDE.getDf());        // update Df
                        // update offsetTermFreq
                        // update offsetDocId

                        // update InvertedIndex, add the current postings to the previus postings for the same term
                        tempPL.extend(currentPL);
                        /****** da chiedere ******
                         * nell'estensione della postingList non si deve controllare duplicati perchè scorriamo i
                         * documenti quindi in blocchi diversi ci saranno dati da documenti diversi quindi in blocchi
                         * diversi dell'inverted index ci saranno postings contenenti docID diversi
                         */
                    } else {    // write to disk
                        // write DictionaryElem to disk
                        storeDictionaryIntoDisk(tempDE, VOCABULARY_FILE);

                        // write InvertedIndexElem to disk
    //                    buffer = outIndexChannel.map(FileChannel.MapMode.READ_WRITE, DataStructureHandler.dictionaryBlocks.get(i), TERM_DIM); // get first term of the block
    //                    CharBuffer.allocate(TERM_DIM); //allocate a charbuffer of the dimension reserved to term
    //                    CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);

                        // reput current in pq
                        pq.add(currentTermBlock);

                        // reset temp structures
                        tempDE.setTermId(0);
                        tempPL.setPostings(new ArrayList<Posting>());

                        //continue;   da scommentare se dopo c'è solo aggiornamento offset
                    }

                }

                //update position of reading from the dictionary file
                currentBlockOffset.set(block_id, DataStructureHandler.dictionaryBlocks.get(block_id) + vocsize);

                //read posting list for the specified term
        //        PostingList pl = DataStructureHandler.readIndexElemFromDisk(de.getOffsetDocId(), de.getOffsetTermFreq(), term, de.getDf());

                // read next term from the vocabulary of the previous index block
                buffer = channel.map(FileChannel.MapMode.READ_ONLY, currentBlockOffset.get(block_id), TERM_DIM); // get first term of the block
                CharBuffer.allocate(TERM_DIM); //allocate a charbuffer of the dimension reserved to term
                CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
                pq.add(new TermBlock(charBuffer.toString().split("\0")[0], block_id)); //add to the priority queue a term block element (term + its blocks number)



                // after read next term if it is different from the previous (set to true if first)
                //isFirst = false;
            }



        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * class
     */
    private static class TermBlock {

        String term;
        int block;

        public TermBlock(String term, int block) {
            this.term = term;
            this.block = block;
        }

        public void setTerm(String term) {
            this.term = term;
        }

        public void setBlock(int block) {
            this.block = block;
        }

        public String getTerm() {
            return term;
        }

        public int getBlock() {
            return block;
        }

    }

    /**
     * class to compare
     */
    private static class CompareTerm implements java.util.Comparator<TermBlock> {
        /**
         *
         * @param tb1
         * @param tb2
         * @return
         */
        @Override
        public int compare(TermBlock tb1, TermBlock tb2) {
            if (tb1.getTerm().compareTo(tb2.getTerm()) == 0) {
                if(tb1.getBlock()<tb2.getBlock())
                    return -1;
                else
                    return 1;
            } else {
                return tb1.getTerm().compareTo(tb2.getTerm());
            }
        }
    }

}
