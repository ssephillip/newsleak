package uhh_lt.newsleak.download;


import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import uhh_lt.newsleak.types.TpDocument;

import java.io.*;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class TransparenzResourceDownloader {


    /** Number of total documents in the Solr Index. */
    private int totalDocuments = 0;

    /** Current document number. */
    private int currentDocument = 0;



    /** Number of inner documents. */
    int numOfInnerPdf = 0;


    /** Number of documents in the Solr Index where at least one of the necessary fields is missing or malformed. */
    int malformedSolrDocCounter = 0;

    /** The solr client */
    HttpSolrClient solrClient;

    String solrCoreAddress;

    Iterator<TpDocument> tpDocumentsIterator;


    public static void main(String[] args){
        TransparenzResourceDownloader transparenzResourceDownloader = new TransparenzResourceDownloader("http://localhost:8983/solr/simfin");
        Instant start = Instant.now();
        try {
            transparenzResourceDownloader.download("/home/phillip/BA/data/transparenz2/", 150000);
        }catch(InstantiationException e){
            System.out.println("Couldn't retrieve IDs!");
            e.printStackTrace();
        }
        Instant end = Instant.now();
        System.out.println("Downloaded all documents in "+ Duration.between(start,end).getSeconds()+" seconds");
    }


    public TransparenzResourceDownloader(String solrCoreAddress){
        super();
        this.solrCoreAddress = solrCoreAddress;

        solrClient = new HttpSolrClient.Builder(solrCoreAddress).build();

    }







    public void download(String path, int numOfDocs) throws InstantiationException{
        List<TpDocument> tpDocuments = new ArrayList<>();

        //       SolrDocumentList solrDocuments = getOuterDocIdsFromSolrIndex(numOfDocs);
        SolrDocumentList solrDocuments = getAllOuterDocumentsFromSolr(numOfDocs);
        totalDocuments = solrDocuments.size();

        for(SolrDocument solrDoc: solrDocuments) {
            currentDocument++;

            List<TpDocument> innerDocuments = getAllInnerDocumentsFromOuterDoc(solrDoc);

            tpDocuments.addAll(innerDocuments);
        }

        tpDocumentsIterator = tpDocuments.iterator();
        //to empty the cache with certainty
        solrDocuments = new SolrDocumentList();

        downloadAllDocuments(tpDocuments, path);

    }


    private void downloadAllDocuments(List<TpDocument> tpDocuments, String path) {
        System.out.println("Starting to download files.");
        currentDocument = 0;
        totalDocuments = tpDocuments.size();

        for(TpDocument tpDocument: tpDocuments){
                currentDocument++;
                System.out.println("Downloading document '"+currentDocument+"' of '"+totalDocuments+"'");
                downloadDocument(tpDocument, path);

        }
    }


    private void downloadDocument(TpDocument tpDocument, String path){
            String urlString = tpDocument.getResUrl();
            String docFormat = tpDocument.getResFormat().toLowerCase();
            String docId = tpDocument.getOuterId()+"_"+tpDocument.getInnerId();
            String docName = tpDocument.getResName();
            String pathToFile = path+docId+"."+docFormat;

            try {
                URL url = new URL(urlString);
                InputStream in = new BufferedInputStream(url.openStream());
                OutputStream out = new BufferedOutputStream(new FileOutputStream(pathToFile));

                for (int i; (i = in.read()) != -1; ) {
                    out.write(i);
                }
                in.close();
                out.close();
            }catch(Exception e){
                System.out.println("Couldn't download dcoument.");
                e.printStackTrace();
            }

    }


    /**
     * Gets one outer document from the solr index.
     * @return List of SolrDocument
     * @throws IOException
     */
    private List<TpDocument> getInnerDocsFromSolrIndex(SolrDocument outerDocument) throws IOException {
        List<TpDocument> innerDocuments = new ArrayList<>();

        if(currentDocument%20 == 0) {
            System.out.println("Getting outer document number '" + currentDocument + "' of '" + totalDocuments + "' from index " + solrCoreAddress);
        }

        innerDocuments = getAllInnerDocumentsFromOuterDoc(outerDocument);

        return innerDocuments;
    }



    public SolrDocumentList getAllOuterDocumentsFromSolr(int numberOfDocs) throws InstantiationException{
        QueryResponse response = null;

        SolrQuery documentQuery = new SolrQuery("res_format:\"PDF\"");
        documentQuery.addField("id");
        documentQuery.addField("res_format");
        documentQuery.addField("res_url");
        documentQuery.addField("res_name");
        documentQuery.addField("title");
        documentQuery.addField("publishing_date");
        documentQuery.setRows(numberOfDocs); //TODO evtl. weg, da immer nur ein ergebnis kommen sollte


        try {

            response = solrClient.query(documentQuery);

            if (response == null) {
                throw new InstantiationException(); //TODO 2019-07-11 ps: ist diese Exception hier korrekt?
            }


        } catch (Exception e) {
            System.out.println("Failed retrieving outer documents from index "+solrCoreAddress);

            e.printStackTrace();
            throw new InstantiationException(); //TODO 2019-08-20 ps: sinnvolle exception schmeißen
        }

        return response.getResults();
    }



    public List<TpDocument> getAllInnerDocumentsFromOuterDoc(SolrDocument outerDocument){
        List<TpDocument> innerDocuments = new ArrayList<>();
        List<String> outerDocResUrls = (List<String>) outerDocument.getFieldValue("res_url");

        if(currentDocument%100 == 0) {
            System.out.println("Getting outer document number '" + currentDocument + "' of '" + totalDocuments + "' from index " + solrCoreAddress);
        }

        if(outerDocResUrls != null){
            for (int i = 0; i < outerDocResUrls.size(); i++){
                TpDocument innerDocument = getInnerDocFromOuterDoc(outerDocument, i);
                if(innerDocument != null){
                    innerDocuments.add(innerDocument);
                }else{
                    innerDocuments = new ArrayList<>();
                    System.out.println("Failed retrieving outer document from index "+solrCoreAddress);
                    break;
                }
            }
        }

        return innerDocuments;
    }


    /**
     * Gets an inner document from the given outer document (SolrDocument).
     * The relativeInnerId is the position in the lists of the outer document, where the information for the desired inner document is located.
     * A SolrDocument in the TransparenzPortal is often actually a group of documents.
     * Each document in this group is called inner document.
     * The SolrDocument itself is called outer document.
     * Whenever a field has the prefix "res" it referrs to a inner document (e.g. res_fulltext refers to the fulltext a an inner document).
     * @param solrDoc A SolrDocument retrieved from the Transparenz Portal solr index
     * @param relativeInnerId An int specifying which of the inner documents shall be extracted.
     * @return TpDocument The inner document "extracted" from the given outer document.
     */
    private TpDocument getInnerDocFromOuterDoc(SolrDocument solrDoc, int relativeInnerId) {
        TpDocument tpDocument = new TpDocument();

        List<String> docResFormats = (List<String>) solrDoc.getFieldValue("res_format");
        List<String> docResUrls = (List<String>) solrDoc.getFieldValue("res_url");
        List<String> docResNames = (List<String>) solrDoc.getFieldValue("res_name");
        String outerId = (String) solrDoc.getFieldValue("id");
        String outerName = (String) solrDoc.getFieldValue("title");
        String date = getDateAsString((Date) solrDoc.getFieldValue("publishing_date"));



        try {
 //           System.out.println("Processing inner document "+relativeInnerId+" from outer document "+outerId+"."); //TODO log-level korrekt?
            if (!isSolrDocWellFormed(docResFormats, docResUrls, docResNames, outerId)) {
                throw new IllegalArgumentException();
            }

            String innerDocFormat = docResFormats.get(relativeInnerId);
            String url = docResUrls.get(relativeInnerId);
            String name = docResNames.get(relativeInnerId);
            tpDocument.setResFormat(innerDocFormat);
            tpDocument.setInnerId(String.valueOf(relativeInnerId));
            tpDocument.setResUrl(url);
            tpDocument.setResName(name);
            tpDocument.setOuterId(outerId);
            tpDocument.setTitle(outerName);
            tpDocument.setDate(date);
        } catch (IllegalArgumentException e) {
            /** A SolrDocument is malformed if some mandatory information is missing.
             *  E.g. the number of fulltexts does not match the number of inner documents. */
            malformedSolrDocCounter++;
            System.out.println("Malformed outer document: "+outerId+". Discarding document."); //TODO evtl. level fine oder finest
            return null;
        }

        return tpDocument;
    }




    /**
     * Gets all outer document ids from the solr index that match the query.
     * The additional lists are retrieved to verify that the documents are well formed.
     * @return SolrDocumentList
     * @throws InstantiationException
     */
    private SolrDocumentList getOuterDocIdsFromSolrIndex(int amountOfDocs) throws InstantiationException {
        QueryResponse response = null;

        SolrQuery documentQuery = new SolrQuery("res_format:\"PDF\"");
        documentQuery.addField("id");
        documentQuery.addField("res_format");
        documentQuery.addField("res_url");
        documentQuery.setRows(amountOfDocs);

        try {
           System.out.println("Getting outer document ids from index " + solrCoreAddress+" with filter '"+documentQuery.getQuery()+"'");
            response = solrClient.query(documentQuery);

            if (response == null) {
                throw new IOException(); //TODO 2019-07-11 ps: ist diese Exception hier korrekt?
            }
        } catch (SolrServerException | IOException e) {
            System.out.println("Failed retrieving outer document ids from index "+solrCoreAddress);
            e.printStackTrace();
            throw new InstantiationException();
        }

        return response.getResults();
    }


    public synchronized TpDocument getNextTpDocument(){
      return null;
    }


    //TODO java-doc comments
    public String getDateAsString(Date date){
        String dateString;

        if(date != null) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            String year = Integer.toString(calendar.get(Calendar.YEAR));
            String month = Integer.toString(calendar.get(Calendar.MONTH)+1);
            String day = Integer.toString(calendar.get(Calendar.DAY_OF_MONTH));

            if(month.length()==1){
                month = "0"+month;
            }
            if(day.length()==1){
                day = "0"+day;
            }

            dateString = year+"-"+month+"-"+day;


        }else{
            dateString = "1900-01-01";
        }

        return dateString;
    }


    /**
     * Tests if an outer document is well formed.
     * An outer document (SolrDocument) is well formed if all the mandatory fields (i.e. the parameters of this method)
     * are Non-Null AND Non-Empty AND if all lists have the same length.
     *
     * @param docResFormats The list of file-formats of the inner documents
     * @param docResUrls The list of URLs to the original files of the inner documents
     * @param outerId The ID of the outer document (containing the inner documents)
     * @return boolean - True if the outer document is wellformed.
     */
    private boolean isSolrDocWellFormed(List<String> docResFormats, List<String> docResUrls, List<String> docResNames, String outerId) {
        //TODO ps 2019-08-20 auch abfragen ob die listen empty sind
        return !(docResFormats == null || docResUrls == null || docResNames==null || outerId == null ||
                docResFormats.isEmpty() || docResUrls.isEmpty() || docResNames.isEmpty() ||
                docResFormats.size() != docResUrls.size() || docResFormats.size() != docResNames.size());
    }
}