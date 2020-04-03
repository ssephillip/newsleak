import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class TransparenzResourceDownloader {


    /** Number of outer documents in the Solr Index. */
    private int numOfOuterDocs = 0;

    private int totalNumOfInnerDocs = 0;

    private int filteredNumOfInnerDocs = 0;

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





    public TransparenzResourceDownloader(String solrCoreAddress){
        super();
        this.solrCoreAddress = solrCoreAddress;

        solrClient = new HttpSolrClient.Builder(solrCoreAddress).build();

    }







    public void download(String path, String pathToStats, List<String> formatsToDownload, int numOfDocs, int startFrom, int downloadUntil, int numOfThreads) throws InstantiationException{
        List<TpDocument> tpDocuments = new ArrayList<>();
        Instant startTime = Instant.now();

        SolrDocumentList solrDocuments = getAllOuterDocumentsFromSolr();
        numOfOuterDocs = solrDocuments.size();
        int outerDocsProcessed = 0;

        for(SolrDocument solrDoc: solrDocuments) {
            outerDocsProcessed++;

            List<TpDocument> innerDocuments = getAllInnerDocumentsFromOuterDoc(solrDoc, outerDocsProcessed);

            tpDocuments.addAll(innerDocuments);
        }
        totalNumOfInnerDocs = tpDocuments.size();
        tpDocuments.removeIf(tp -> !formatsToDownload.contains(tp.getResFormat()));
        filteredNumOfInnerDocs = tpDocuments.size();
        tpDocumentsIterator = tpDocuments.iterator();

        System.out.println("Time for getting and extracting documents: "+Duration.between(startTime, Instant.now()).getSeconds());
        downloadAllDocuments(tpDocuments, path, numOfDocs, startFrom, downloadUntil, numOfThreads);


        writeStatsWhenFinished(startTime, pathToStats, formatsToDownload, numOfThreads);


    }



    public SolrDocumentList getAllOuterDocumentsFromSolr() throws InstantiationException{
        QueryResponse response = null;



        SolrQuery documentQuery = new SolrQuery("*:*");
        documentQuery.addField("id");
        documentQuery.addField("res_format");
        documentQuery.addField("res_url");
        documentQuery.addField("res_name");
        documentQuery.addField("title");
        documentQuery.addField("publishing_date");
        documentQuery.setRows(Integer.MAX_VALUE); //TODO evtl. weg, da immer nur ein ergebnis kommen sollte


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



    public List<TpDocument> getAllInnerDocumentsFromOuterDoc(SolrDocument outerDocument, int outerDocsProcessed){
        List<TpDocument> innerDocuments = new ArrayList<>();
        List<String> docResFormats = (List<String>) outerDocument.getFieldValue("res_format");
        List<String> docResUrls = (List<String>) outerDocument.getFieldValue("res_url");
        List<String> docResNames = (List<String>) outerDocument.getFieldValue("res_name");
        String outerId = (String) outerDocument.getFieldValue("id");


        if(outerDocsProcessed%100 == 0) {
            System.out.println("Getting inner documents from outer document number '" + outerDocsProcessed + "' of '" + numOfOuterDocs + "' from index " + solrCoreAddress);
        }

        if(isSolrDocWellFormed(docResFormats, docResUrls, docResNames, outerId)){
            for (int i = 0; i < docResUrls.size(); i++){
                TpDocument innerDocument = getInnerDocFromOuterDoc(outerDocument, i);
                innerDocuments.add(innerDocument);
            }
        }else{
            malformedSolrDocCounter++;
            System.out.println("Malformed outer document: "+outerId+". Discarding document.");
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
     * @assert solrDoc is wellformed
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

        String innerDocFormat = docResFormats.get(relativeInnerId);
        String url = docResUrls.get(relativeInnerId);
        String name = docResNames.get(relativeInnerId);
        tpDocument.setResFormat(innerDocFormat.toLowerCase());
        tpDocument.setInnerId(String.valueOf(relativeInnerId));
        tpDocument.setResUrl(url);
        tpDocument.setResName(name);
        tpDocument.setOuterId(outerId);
        tpDocument.setTitle(outerName);
        tpDocument.setDate(date);

        return tpDocument;
    }


    private void downloadAllDocuments(List<TpDocument> tpDocuments, String path, int numOfDocsToDownload, int startFrom, int downloadUntil, int numOfThreads){
        final TpDocumentProvider tpDocumentProvider = TpDocumentProvider.getInstance(tpDocuments, numOfDocsToDownload);
        final int start = startFrom;
        final int end = downloadUntil;
        final int totalNumOfInnerDocs = filteredNumOfInnerDocs;

        for(int i= 0; i < numOfThreads; i++){
            Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    downloadDocuments(tpDocumentProvider, path, start, end, numOfDocsToDownload, totalNumOfInnerDocs);
                }
            });
            thread.start();
        }
    }

    private void downloadDocuments(TpDocumentProvider tpDocumentProvider, String path, int startFrom, int downloadUntil, int numOfDocsToDownload, int totalNumOfInnerDocs) {
        System.out.println("Starting to download files.");
        int threadNumber = tpDocumentProvider.getCurrentThreadCount();

        while(true) {
            TpDocument tpDocument = tpDocumentProvider.getNextTpDocument();
            if(tpDocument != null) {
                int current = tpDocumentProvider.getCurrent();
                if(current >= startFrom && current<downloadUntil && current < numOfDocsToDownload) {
                    System.out.println("Thread '"+threadNumber+"'Downloading document '"+current+"' of '"+totalNumOfInnerDocs+"' with the ID: '"+tpDocument.getOuterId()+"_"+tpDocument.innerId);
                    downloadDocument(tpDocument, tpDocumentProvider, path);
                }
            }else{
                System.out.println("Thread '"+threadNumber+"' finished");
                break;
            }
        }

    }



    private void downloadDocument(TpDocument tpDocument,  TpDocumentProvider tpDocumentProvider, String path){
            String urlString = tpDocument.getResUrl();
            String docFormat = tpDocument.getResFormat().toLowerCase();
            String docId = tpDocument.getOuterId()+"_"+tpDocument.getInnerId();
            String docName = tpDocument.getResName();
            String pathToFile = path+docId+"."+docFormat;

            try {
                URL url = new URL(urlString);
                URLConnection urlConnection = url.openConnection();
                urlConnection.setReadTimeout(1000000);
                urlConnection.connect();
                InputStream in = new BufferedInputStream(urlConnection.getInputStream());
                OutputStream out = new BufferedOutputStream(new FileOutputStream(pathToFile));

                for (int i; (i = in.read()) != -1; ) {
                    out.write(i);
                }
                in.close();
                out.close();
                tpDocumentProvider.incrementNumOfDocsDownloaded();
            }catch(Exception e){
                System.out.println("Couldn't download dcoument.");
                e.printStackTrace();
                tpDocumentProvider.incrementNumOfDocsFailed();
            }

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



    private void writeStatsWhenFinished(Instant startTime, String pathToStats, List<String> formatsToDownload, int numOfThreads) {
        String pathToTempStats = pathToStats+"--temp.txt";

        while(true){
            TpDocumentProvider tpDocumentProvider = TpDocumentProvider.getInstance();
            if(tpDocumentProvider != null && tpDocumentProvider.isFinished()){
                Instant endTime = Instant.now();
                long secondsElapsed = Duration.between(startTime, endTime).toMillis()/1000;
                writeDownloadStatsToFile(secondsElapsed, tpDocumentProvider.getNumOfInnerDocsDownloaded(), tpDocumentProvider.getNumOfInnerDocsFailed(), formatsToDownload, pathToStats, numOfThreads);
                break;
            }else if(tpDocumentProvider != null &&  tpDocumentProvider.getCurrent()%500==0){ //TODO determine time for temp-stat print better (e.g. with a callback)
                //TODO zwischen stats ausbauen wenn fertig, da zwischen stats funktion zu unstabil.
                Instant endTime = Instant.now();
                long secondsElapsed = Duration.between(startTime, endTime).toMillis()/1000;
                writeDownloadStatsToFile(secondsElapsed, tpDocumentProvider.getNumOfInnerDocsDownloaded(), tpDocumentProvider.getNumOfInnerDocsFailed(), formatsToDownload, pathToTempStats, numOfThreads);

            }

            try {
                Thread.sleep(100);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    private void writeDownloadStatsToFile(long secondsElapsed, int numOfDocsDownloaded, int numOfDocsFailedToDownload, List<String> downloadedFormats, String filePath, int numOfThreads){
        try {
            FileWriter fileWriter = new FileWriter(filePath, true);
            fileWriter.write("--------------------------------------------------");
            fileWriter.write("--------------------------------------------------");
            fileWriter.write("Run: "+Instant.now().toString()+"\n");
            fileWriter.write("Number of threads: "+numOfThreads+"\n");
            fileWriter.write("Total number of outer documents: "+numOfOuterDocs+"\n");
            fileWriter.write("Number of malformed outer documents: "+malformedSolrDocCounter+"\n");
            fileWriter.write("Total number of inner documents: "+totalNumOfInnerDocs+"\n");
            fileWriter.write("Number of inner documents of desired formats: "+filteredNumOfInnerDocs+"\n");
            fileWriter.write("Number of inner documents downloaded: "+numOfDocsDownloaded+"\n");
            fileWriter.write("Number of inner documents failed to download: "+numOfDocsFailedToDownload+"\n");
            fileWriter.write("Time elapsed: "+secondsElapsed+"\n");
            fileWriter.write("Documents per second: "+((double) numOfDocsDownloaded)/secondsElapsed+"\n");
            fileWriter.write("Formats downloaded: "+downloadedFormats.toString()+"\n");
            fileWriter.write("--------------------------------------------------");
            fileWriter.write("--------------------------------------------------");


            fileWriter.flush();
            fileWriter.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }


}