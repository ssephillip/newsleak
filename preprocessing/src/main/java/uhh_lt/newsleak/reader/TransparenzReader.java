package uhh_lt.newsleak.reader;


import com.google.gson.JsonElement;
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
import org.apache.uima.fit.descriptor.ExternalResource;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import uhh_lt.newsleak.resources.MetadataResource;
import uhh_lt.newsleak.types.Metadata;
import uhh_lt.newsleak.types.TpDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransparenzReader extends NewsleakReader {

    /** The Constant RESOURCE_METADATA. */
    public static final String RESOURCE_METADATA = "metadataResource";

    /** The metadata resource. */
    @ExternalResource(key = RESOURCE_METADATA)
    private MetadataResource metadataResource;

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
     */

    /** Number of documents where at least one of the necessary fields is missing in the Solr Index. */
    int malformedSolrDocCounter = 0;

    /** Number of inner documents. */
    int numOfInnerPdf = 0;


    // Main method for test purposes
    public static void main(String[] args) {
        TransparenzReader tpReader = new TransparenzReader();
        try {
            tpReader.initialize(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);
        List<TpDocument> allTpDocuments = new ArrayList<>();

        SolrDocumentList solrDocuments = getDocumentsFromSolrIndex();

        for (SolrDocument solrDocument : solrDocuments) {
            List<TpDocument> innerTpDocuments = getInnerDocsAsTpDocs(solrDocument);
            allTpDocuments.addAll(innerTpDocuments);
        }

        totalDocuments = allTpDocuments.size();
        currentDocument = 0;
        tpDocumentIterator = allTpDocuments.iterator();


        printDebugStatistics(solrDocuments.size());
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

        jcas.setDocumentText(document.getResUrl()+"\t"+document.getResFulltext());


        // Set metadata
        //TODO brauche ich das?
        Metadata metaCas = new Metadata(jcas);
        metaCas.setDocId(docId);
        metaCas.setTimestamp("1900-01-01"); //TODO 2019-07-04 ps: richtiges Datum verwenden
        metaCas.addToIndexes();



        // write external metadata
        ArrayList<List<String>> metadata = new ArrayList<List<String>>();

        // filename, subject, path
        String fileName = "";
        String field = document.getTitle(); //Das ist nicht wirklich der Filename
        if (field != null) {
            fileName = field;
            metadata.add(metadataResource.createTextMetadata(docId, "filename", fileName));
        }
        field = document.getResName();
        if (field != null) {
            metadata.add(metadataResource.createTextMetadata(docId, "subject", field));
        } else {
            if (!fileName.isEmpty()) {
                metadata.add(metadataResource.createTextMetadata(docId, "subject", fileName));
            }
        }

        field = document.getResUrl();
        if(field != null) {
            // Source Id
            metadata.add(metadataResource.createTextMetadata(docId, "Link", field));
        }

        // file-type
        field = "PDF";
        if (field != null) {
            metadata.add(metadataResource.createTextMetadata(docId, "filetype", field));
        }

        metadataResource.appendMetadata(metadata);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#getProgress()
     */
    public Progress[] getProgress() {
        return new Progress[]{new ProgressImpl(Long.valueOf(currentDocument).intValue(),
                Long.valueOf(totalDocuments).intValue(), Progress.ENTITIES)};
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


    private String removeMultWhitespaces(String text){
        Pattern p = Pattern.compile("[\\s\\v]+");
        Matcher m = p.matcher(text);
        String cleanedText = m.replaceAll(" ");

        return cleanedText;
    }


    private SolrDocumentList getDocumentsFromSolrIndex() throws ResourceInitializationException {
        QueryResponse response = null;

        HttpSolrClient solrClient = new HttpSolrClient.Builder(solrCoreAddress).build();
        SolrQuery documentQuery = new SolrQuery("res_format:\"PDF\"");
        documentQuery.addField("id");
        documentQuery.addField("res_format");
        documentQuery.addField("res_url");
        documentQuery.addField("res_fulltext");
        documentQuery.addField("res_name");
        documentQuery.addField("title");
        documentQuery.setRows(Integer.MAX_VALUE);


        try {
            response = solrClient.query(documentQuery);

            if (response == null) {
                throw new IOException(); //TODO 2019-07-11 ps: ist diese Exception hier korrekt?
            }
        } catch (SolrServerException | IOException e) {
            e.printStackTrace(); //TODO 2019-06-24, ps: vernünftiges Error-Handling einbauen (loggen und nicht nur den stack printen)
            throw new ResourceInitializationException();
        }


        return response.getResults();
    }




    private List<TpDocument> getInnerDocsAsTpDocs(SolrDocument solrDoc) {

        List<TpDocument> tpDocuments = new ArrayList<>();

        List<String> docResFormats = (List<String>) solrDoc.getFieldValue("res_format");
        List<String> docResUrls = (List<String>) solrDoc.getFieldValue("res_url");
        List<String> docResFulltexts = (List<String>) solrDoc.getFieldValue("res_fulltext");
        List<String> docResNames = (List<String>) solrDoc.getFieldValue("res_name");
        String outerId = (String) solrDoc.getFieldValue("id");
        String outerName = (String) solrDoc.getFieldValue("title");


        try {

            if (!isSolrDocWellFormed(docResFormats, docResUrls, docResFulltexts, outerId)) {
                throw new IllegalArgumentException();  //TODO 2019-06-30, ps: Log the error
            }


            int numOfInnerDocs = docResFormats.size();

            for (int i = 0; i < numOfInnerDocs; i++) {
                String innerDocFormat = docResFormats.get(i);

                if (innerDocFormat.equals("PDF")) {
                    String url = docResUrls.get(i);
                    String fulltext = docResFulltexts.get(i);
                    String name = null;
                    if(docResNames != null) {
                        name = docResNames.get(i);
                    }

                    TpDocument tpDocument = new TpDocument();
                    tpDocument.setResFormat(innerDocFormat);
                    tpDocument.setInnerId(url);
                    tpDocument.setResUrl(url);
                    tpDocument.setResFulltext(removeMultWhitespaces(fulltext).trim());
                    tpDocument.setResName(name);
                    tpDocument.setOuterId(outerId);
                    tpDocument.setTitle(outerName);

                    tpDocuments.add(tpDocument);
                    numOfInnerPdf++;
                }
            }
        } catch (IllegalArgumentException e) {
            malformedSolrDocCounter++;
            System.out.println("Malformed document: "+outerId); //TODO 2019-06-30 ps: vernünftig loggen; warsch. vernünftiges error handling einbauen
            return new ArrayList<>();
        }

        return tpDocuments;
    }


    private boolean isSolrDocWellFormed(List<String> docResFormats, List<String> docResUrls, List<String> docResFulltexts, String outerId) {

        return !(docResFormats == null || docResUrls == null || docResFulltexts == null || outerId == null ||
                docResFormats.size() != docResUrls.size() || docResFormats.size() != docResFulltexts.size());
    }

    private void printDebugStatistics(int numOfSolrDocs) {
        System.out.println("Number of malformed SolrDocuments: " + malformedSolrDocCounter);
        System.out.println("Average number of inner PDF: " + ((float) numOfInnerPdf) / numOfSolrDocs);
    }

}