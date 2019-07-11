package uhh_lt.newsleak.reader;


import org.apache.commons.csv.CSVRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import uhh_lt.newsleak.types.Metadata;
import uhh_lt.newsleak.types.TpDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TransparenzReader extends NewsleakReader {

    /** The Constant PARAM_DEFAULT_LANG. */
    public static final String PARAM_DEFAULT_LANG = "defaultLanguage";

    /** The default language. */
    @ConfigurationParameter(name = PARAM_DEFAULT_LANG, mandatory = false, defaultValue = "deu")
    private String defaultLanguage;

    /** The URL to the Solr-Server. */
    String SOLR_ADDRESS = "http://localhost:8983/solr/";

    /** The name of the Solr Core */
    String SOLR_CORE_NAME = "simfin";

    /** The URL to the Solr Core */
    String solrCoreAddress = SOLR_ADDRESS + SOLR_CORE_NAME;

    /** Number of total documents in the Solr Index. */
    private int totalDocuments = 0;

    /** Current document number. */
    private int currentDocument = 0;

    /** Iterator holding all documents from the Solr Index. */
    Iterator<TpDocument> tpDocumentIterator = null;


    /**
     * Internal Variables
     *
     */

    /** Number of documents where at least one of the necessary fields is missing in the Solr Index. */
    int malformedSolrDocCounter = 0;

    /** Number of inner documents. */
    int numOfInnerPdf = 0;


    // Main method for test purposes
    public static void main(String[] args){
        TransparenzReader tpReader = new TransparenzReader();
        try {
            tpReader.initialize(null);
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);

        HttpSolrClient solrClient =  new HttpSolrClient.Builder(solrCoreAddress).build();
        SolrQuery idQuery = new SolrQuery("res_format:\"PDF\"");
        idQuery.addField("id");
        idQuery.addField("res_format");
        idQuery.addField("res_url");
        idQuery.addField("res_fulltext");
        idQuery.setRows(Integer.MAX_VALUE);


        QueryResponse response = null;
        try {
            response = solrClient.query(idQuery);
        }catch(SolrServerException | IOException e){
            e.printStackTrace(); //TODO 2019-06-24, ps: vernünftiges Error-Handling einbauen
        }

        //TODO 2019-30-06, ps: sinnvoll abfangen, wenn response == null ist


        SolrDocumentList solrDocuments = response.getResults();



        List<TpDocument> allTpDocuments = new ArrayList<>();

        for(SolrDocument solrDocument: solrDocuments){
           List<TpDocument> innerTpDocuments = getTpDocsFromInnerDocs(solrDocument);
           allTpDocuments.addAll(innerTpDocuments);

        }

        totalDocuments = allTpDocuments.size();
        tpDocumentIterator = allTpDocuments.iterator();

        System.out.println("Number of malformed SolrDocuments: "+ malformedSolrDocCounter);
        System.out.println("Average number of inner PDF: "+ ((float) numOfInnerPdf)/solrDocuments.size());
        System.out.println("blub");



    }


//    private SolrDocumentList getDocumentsFromSolrIndex(){
//
//    }

    private List<TpDocument> getTpDocsFromInnerDocs(SolrDocument solrDoc){

        List<TpDocument> tpDocuments = new ArrayList<>();

        List<String> docResFormats = (List<String>) solrDoc.getFieldValue("res_format");
        List<String> docResUrls = (List<String>) solrDoc.getFieldValue("res_url");
        List<String> docResFulltexts = (List<String>) solrDoc.getFieldValue("res_fulltext");
        String tpId = (String) solrDoc.getFieldValue("id");

        if(docResFormats == null || docResUrls == null || docResFulltexts == null || tpId == null){
            //TODO 2019-06-30, ps: Log the error und evtl. korrekte fehler behandlung
            System.out.println("Malformed SolrDocument \\p TpId: "+ tpId);
            malformedSolrDocCounter++;
            return new ArrayList<>();
        }


        int numOfInnerDocs = docResFormats.size();

        try {
            for (int i = 0; i < numOfInnerDocs; i++) {
                String innerDocFormat = docResFormats.get(i);
                if (innerDocFormat.equals("PDF")) {
                    numOfInnerPdf++;
                    String url = docResUrls.get(i);
                    String fulltext = docResFulltexts.get(i);


                    TpDocument tpDocument = new TpDocument();
                    tpDocument.setResFormat(innerDocFormat);
                    tpDocument.setNewsleakId(url);
                    tpDocument.setResUrl(url);
                    tpDocument.setResFulltext(fulltext);
                    tpDocument.setTpId(tpId);

                    tpDocuments.add(tpDocument);
                }
            }
        }catch(Exception e){
            //TODO 2019-06-30 ps: vernünftig loggen; warsch. vernünftiges error handling einbauen
            e.printStackTrace();
            return new ArrayList<>();
        }

        return tpDocuments;
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.uima.collection.CollectionReader#getNext(org.apache.uima.cas.CAS)
     */
    public void getNext(CAS cas) throws IOException, CollectionException {
        currentDocument++;
        JCas jcas;
        try {
            jcas = cas.getJCas();
        } catch (CASException e) {
            throw new CollectionException(e);
        }

        // Set document data
        TpDocument document = tpDocumentIterator.next();
        String docId = Integer.toString(currentDocument);

        if(document.getResFulltext() == null){
            System.out.println("blub");
        }
        jcas.setDocumentText(cleanBodyText(document.getResFulltext()));
        jcas.setDocumentLanguage("de");

        // Set metadata
        Metadata metaCas = new Metadata(jcas);
        metaCas.setDocId(docId);
        metaCas.setTimestamp("1900-01-01"); //TODO 2019-07-04 ps: richtiges Datum verwenden
        metaCas.addToIndexes();

        // metadata
        // is assumed to be provided from external prcessing in a separate file

    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#getProgress()
     */
    public Progress[] getProgress() {
        return new Progress[] { new ProgressImpl(Long.valueOf(currentDocument).intValue(),
                Long.valueOf(totalDocuments).intValue(), Progress.ENTITIES) };
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#hasNext()
     */
    public boolean hasNext() throws IOException, CollectionException {
        if (currentDocument > maxRecords)
            return false;
        return tpDocumentIterator.hasNext();
    }


}