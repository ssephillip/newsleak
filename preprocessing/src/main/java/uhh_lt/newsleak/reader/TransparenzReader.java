package uhh_lt.newsleak.reader;


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
import org.apache.uima.util.Level;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import uhh_lt.newsleak.resources.MetadataResource;
import uhh_lt.newsleak.types.Metadata;
import uhh_lt.newsleak.types.TpDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    /** The Constant TRANSPARENZ_CORE_ADDRESS. */
    public static final String TRANSPARENZ_CORE_ADDRESS = "transparenzcoreaddress";

    /** The URL to the core of the (solr) Transparenz index*/
    @ConfigurationParameter(name = TRANSPARENZ_CORE_ADDRESS, mandatory = true)
    private String solrCoreAddress;

    /** Number of total documents in the Solr Index. */
    private int totalDocuments = 0;

    /** Current document number. */
    private int currentDocument = 0;

    /** Iterator holding all documents from the Solr Index. */
    Iterator<TpDocument> tpDocumentIterator = null;

    /** Number of documents in the Solr Index where at least one of the necessary fields is missing or malformed. */
    int malformedSolrDocCounter = 0;

    /** Number of inner documents. */
    int numOfInnerPdf = 0;


    /**
     * Gets all documents from the solr index (that match the query) and stores them in an iterator for later processing.
     * @param context
     * @throws ResourceInitializationException
     */
    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);
        logger = context.getLogger();

        List<TpDocument> allTpDocuments = new ArrayList<>();
        SolrDocumentList solrDocuments = getDocumentsFromSolrIndex();

        logger.log(Level.INFO, "Total number of outer documents: " + solrDocuments.size());
        logger.log(Level.INFO, "Getting inner documents from outer documents.");
        for (SolrDocument solrDocument : solrDocuments) {
            List<TpDocument> innerTpDocuments = getInnerDocsAsTpDocs(solrDocument);
            allTpDocuments.addAll(innerTpDocuments);
        }

        totalDocuments = allTpDocuments.size();
        currentDocument = 0;
        tpDocumentIterator = allTpDocuments.iterator();

        logger.log(Level.INFO, "Total number of inner documents: " + totalDocuments);
        logger.log(Level.INFO, "Number of malformed SolrDocuments: " + malformedSolrDocCounter);
        logger.log(Level.FINEST, "Average number of inner PDF: " + ((float) numOfInnerPdf) / solrDocuments.size());
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
        logger.log(Level.INFO, "Proceessing document: " + docId);

        jcas.setDocumentText(document.getResFulltext());

        // Set metadata
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
        field = document.getResFormat();
        if (field != null) {
            metadata.add(metadataResource.createTextMetadata(docId, "filetype", field));
        }

        // TP-ID
        field = document.getOuterId();
        if (field != null) {
            metadata.add(metadataResource.createTextMetadata(docId, "transparenz-id", field));
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


    /**
     * Gets all documents from the solr index that match the query.
     * @return List of SolrDocument
     * @throws ResourceInitializationException
     */
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
            logger.log(Level.INFO, "Getting documents from index " + solrCoreAddress+" with filter '"+documentQuery.getQuery()+"'");
            response = solrClient.query(documentQuery);

            if (response == null) {
                throw new IOException(); //TODO 2019-07-11 ps: ist diese Exception hier korrekt?
            }
        } catch (SolrServerException | IOException e) {
            logger.log(Level.SEVERE, "Failed retrieving documents from index "+solrCoreAddress);
            e.printStackTrace();
            throw new ResourceInitializationException();
        }


        return response.getResults();
    }


    /**
     * Gets all inner documents from the given SolrDocument.
     * A SolrDocument in the TransparenzPortal is often actually a group of documents.
     * Each document in this group is called inner document.
     * The SolrDocument itself is called outer document.
     * Whenever a field has the prefix "res" it referrs to a inner document (e.g. res_fulltext refers to the fulltext a an inner document).
     * @param solrDoc A SolrDocument retrieved from the Transparenz Portal solr index
     * @return List<TpDocument> A list of inner documents "extracted" from the given outer document.
     */
    private List<TpDocument> getInnerDocsAsTpDocs(SolrDocument solrDoc) {

        List<TpDocument> tpDocuments = new ArrayList<>();

        List<String> docResFormats = (List<String>) solrDoc.getFieldValue("res_format");
        List<String> docResUrls = (List<String>) solrDoc.getFieldValue("res_url");
        List<String> docResFulltexts = (List<String>) solrDoc.getFieldValue("res_fulltext");
        List<String> docResNames = (List<String>) solrDoc.getFieldValue("res_name");
        String outerId = (String) solrDoc.getFieldValue("id");
        String outerName = (String) solrDoc.getFieldValue("title");


        try {
            logger.log(Level.FINEST, "Getting inner documents for: "+outerId+"."); //TODO log level korrekt?

            if (!isSolrDocWellFormed(docResFormats, docResUrls, docResFulltexts, outerId)) {
                throw new IllegalArgumentException();
            }


            int numOfInnerDocs = docResFormats.size();

            for (int i = 0; i < numOfInnerDocs; i++) {
                logger.log(Level.FINEST, "Processing inner document "+i+" from outer document "+outerId+".");

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
                    tpDocument.setResFulltext(fulltext);
                    tpDocument.setResName(name);
                    tpDocument.setOuterId(outerId);
                    tpDocument.setTitle(outerName);

                    tpDocuments.add(tpDocument);
                    numOfInnerPdf++;
                }
            }
        } catch (IllegalArgumentException e) {
            /** A SolrDocument is malformed if some mandatory information is missing.
             *  E.g. the number of fulltexts does not match the number of inner documents. */
            malformedSolrDocCounter++;
            logger.log(Level.INFO, "Malformed document: "+outerId+". Discarding document."); //TODO evtl. level fine oder finest
            return new ArrayList<>();
        }


        logger.log(Level.FINEST, "Found "+tpDocuments.size()+" inner documents for "+outerId+".");
        return tpDocuments;
    }

    /**
     * Tests if an outer document is wellformed.
     * An outer document (SolrDocument) is wellformed if all the mandatory fields (i.e. the parameters of this method)
     * are Non-Null and Non-Empty AND if all lists have the same length.
     *
     * @param docResFormats The list of file-formats of the inner documents
     * @param docResUrls The list of URLs to the original files of the inner documents
     * @param docResFulltexts The list of fulltexts of the inner documents
     * @param outerId The ID of the outer document (containing the inner documents)
     * @return boolean - True if the outer document is wellformed.
     */
    private boolean isSolrDocWellFormed(List<String> docResFormats, List<String> docResUrls, List<String> docResFulltexts, String outerId) {

        return !(docResFormats == null || docResUrls == null || docResFulltexts == null || outerId == null ||
                docResFormats.size() != docResUrls.size() || docResFormats.size() != docResFulltexts.size());
    }

}