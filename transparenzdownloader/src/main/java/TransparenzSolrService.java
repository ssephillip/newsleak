import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TransparenzSolrService {

    /** The solr client */
    HttpSolrClient solrClient;

    String solrCoreAddress;

    /** Number of outer documents in the Solr Index. */
    int numOfOuterDocs;

    int totalNumOfInnerDocs;

    int filteredNumOfInnerDocs; //TODO umbenennen zu numOfRelevantInnerDocs

    int malformedSolrDocCounter;


    public TransparenzSolrService(HttpSolrClient solrClient, String solrCoreAddress){
        super();
        this.solrClient = solrClient;
        this.solrCoreAddress = solrCoreAddress;

        numOfOuterDocs = 0;
        totalNumOfInnerDocs = 0;
        filteredNumOfInnerDocs = 0;
        malformedSolrDocCounter = 0;
    }


    public List<TpResource> getAllInnerDocumentsFromSolr() throws InstantiationException{
        List<TpResource> tpResources = new ArrayList<>();

        SolrDocumentList solrDocuments = getAllOuterDocumentsFromSolr();
        numOfOuterDocs = solrDocuments.size();
        int outerDocsProcessed = 0;

        for(SolrDocument solrDoc: solrDocuments) {
            outerDocsProcessed++;

            List<TpResource> tempTpResources = getAllInnerDocumentsFromOuterDoc(solrDoc, outerDocsProcessed);

            tpResources.addAll(tempTpResources);
        }

        totalNumOfInnerDocs = tpResources.size();

        return tpResources;
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
        documentQuery.setRows(Integer.MAX_VALUE);


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




    public int getNumOfOuterDocs() {
        return numOfOuterDocs;
    }

    public void setNumOfOuterDocs(int numOfOuterDocs) {
        this.numOfOuterDocs = numOfOuterDocs;
    }

    public int getTotalNumOfInnerDocs() {
        return totalNumOfInnerDocs;
    }

    public void setTotalNumOfInnerDocs(int totalNumOfInnerDocs) {
        this.totalNumOfInnerDocs = totalNumOfInnerDocs;
    }

    public int getFilteredNumOfInnerDocs() {
        return filteredNumOfInnerDocs;
    }

    public void setFilteredNumOfInnerDocs(int filteredNumOfInnerDocs) {
        this.filteredNumOfInnerDocs = filteredNumOfInnerDocs;
    }


    public int getMalformedSolrDocCounter() {
        return malformedSolrDocCounter;
    }

    public void setMalformedSolrDocCounter(int malformedSolrDocCounter) {
        this.malformedSolrDocCounter = malformedSolrDocCounter;
    }



}
