package it.unipi.dii.aide.mircv.data_structures;

/**
 *  Stores documents and their statistics
 */
import java.util.*;

public class DocumentTable{
    // new implementation
    private final HashMap<Integer, DocumentElement> docIdToDocElem;     // hash table  DocID to related DocElement

    // constructor
    public DocumentTable(HashMap<Integer, DocumentElement> docIdToDocElem) {
        this.docIdToDocElem = docIdToDocElem;
    }
    // constructor without parameters
    public DocumentTable() {
        this.docIdToDocElem = new HashMap<>();
    }

    public HashMap<Integer, DocumentElement> getDocIdToDocElem() {
        return docIdToDocElem;
    }

    public void setDocIdToDocElem(String docno, int docid, int doclength){
        this.docIdToDocElem.put(docid, new DocumentElement(docno, doclength, docid));
    }


    // old implementation
/*    private final HashMap<Integer, String> docIdToDocno;
    private final HashMap<Integer, Integer> docIdToDocLength;*/


 /*   public DocumentTable() {
        docIdToDocno = new HashMap<>();
        docIdToDocLength = new HashMap<>();
    }

    *//**
     * Add document metadata to the document table.
     *
     * @param docId     The document ID.
     * @param docno     The document number.
     * @param docLength The length of the document.
     *//*
    public void addDocument(int docId, String docno, int docLength) {
        docIdToDocno.put(docId, docno);
        docIdToDocLength.put(docId, docLength);
    }

    *//**
     * Get the document number associated with a given document ID.
     *
     * @param docId The document ID to retrieve the document number for.
     * @return The document number, or an empty string if the document ID is not found.
     *//*
    public String getDocno(int docId) {
        return docIdToDocno.getOrDefault(docId, "");
    }

    *//**
     * Get the document length associated with a given document ID.
     *
     * @param docId The document ID to retrieve the document length for.
     * @return The document length, or 0 if the document ID is not found.
     *//*
    public int getDocLength(int docId) {
        return docIdToDocLength.getOrDefault(docId, 0);
    }
*/}
