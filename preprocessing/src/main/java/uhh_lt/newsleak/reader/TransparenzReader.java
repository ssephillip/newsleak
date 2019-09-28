package uhh_lt.newsleak.reader;


import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
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

    /** Iterator holding all the outer ids from the Solr Index. */
    Iterator<String> innerIdsIterator = null;

    /** Number of documents in the Solr Index where at least one of the necessary fields is missing or malformed. */
    int malformedSolrDocCounter = 0;

    /** Number of inner documents. */
    int numOfInnerPdf = 0;

    /** The solr client */
    HttpSolrClient solrClient;


    /**
     * Gets all inner document ids from the solr index (that match the query) and stores them in an iterator for later processing.
     * @param context
     * @throws ResourceInitializationException
     */
    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);
        logger = context.getLogger();
        solrClient = new HttpSolrClient.Builder(solrCoreAddress).build();

        List<String> allInnerIds = new ArrayList<>();
        SolrDocumentList solrDocuments = getOuterDocIdsFromSolrIndex();

        logger.log(Level.INFO, "Total number of outer documents: " + solrDocuments.size());
        logger.log(Level.INFO, "Getting inner ids from outer documents.");
        for (SolrDocument solrDocument : solrDocuments) {
            List<String> innerIds = getInnerIds(solrDocument);
            allInnerIds.addAll(innerIds);
        }

        totalDocuments = allInnerIds.size();
        currentDocument = 0;
        innerIdsIterator = allInnerIds.iterator();
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.uima.collection.CollectionReader#getNext(org.apache.uima.cas.CAS)
     */
    public void getNext(CAS cas) throws IOException, CollectionException {
        currentDocument++; //TODO ps 2019-08-20: evtl. ans ende der methode verschieben?
        JCas jcas;
        try {
            jcas = cas.getJCas();
        } catch (CASException e) {
            throw new CollectionException(e);
        }

        String innerId = innerIdsIterator.next();
        Integer relativeInnerId = Integer.valueOf(innerId.split("_")[0]);
        String outerId = innerId.split("_")[1];
        SolrDocument solrDocument = getOuterDocumentFromSolrIndex(outerId);
        TpDocument document = getInnerDocFromOuterDoc(solrDocument, relativeInnerId);
        if(document == null){
            throw new CollectionException(); //TODO ps 2019-08-21: was für eine sinnvolle exception kann man hier schmeißen
        }

        String docId = Integer.toString(currentDocument);
        logger.log(Level.INFO, "Processing document: " + docId);

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
        return innerIdsIterator.hasNext();
    }


    /**
     * Gets all outer document ids from the solr index that match the query.
     * The additional lists are retrieved to verify that the documents are well formed.
     * @return SolrDocumentList
     * @throws ResourceInitializationException
     */
    private SolrDocumentList getOuterDocIdsFromSolrIndex() throws ResourceInitializationException {
        QueryResponse response = null;

        SolrQuery documentQuery = new SolrQuery("res_format:\"PDF\"");
        documentQuery.addField("id");
        documentQuery.addField("res_format");
        documentQuery.addField("res_url");
        documentQuery.setRows(Integer.MAX_VALUE);

        try {
            logger.log(Level.INFO, "Getting outer document ids from index " + solrCoreAddress+" with filter '"+documentQuery.getQuery()+"'");
            response = solrClient.query(documentQuery);

            if (response == null) {
                throw new IOException(); //TODO 2019-07-11 ps: ist diese Exception hier korrekt?
            }
        } catch (SolrServerException | IOException e) {
            logger.log(Level.SEVERE, "Failed retrieving outer document ids from index "+solrCoreAddress);
            e.printStackTrace();
            throw new ResourceInitializationException();
        }

        return response.getResults();
    }


    /**
     * Gets one outer document from the solr index.
     * @return List of SolrDocument
     * @throws IOException
     */
    private SolrDocument getOuterDocumentFromSolrIndex(String outerId) throws IOException {
        QueryResponse response = null;


        SolrQuery documentQuery = new SolrQuery("id:"+outerId);
        documentQuery.addField("id");
        documentQuery.addField("res_format");
        documentQuery.addField("res_url");
        documentQuery.addField("res_fulltext");
        documentQuery.addField("res_name");
        documentQuery.addField("title");
        documentQuery.setRows(Integer.MAX_VALUE); //TODO evtl. weg, da immer nur ein ergebnis kommen sollte


        try {
            logger.log(Level.INFO, "Getting outer document '"+outerId+"' from index " + solrCoreAddress);
            response = solrClient.query(documentQuery);

            if (response == null) {
                throw new IOException(); //TODO 2019-07-11 ps: ist diese Exception hier korrekt?
            }
        } catch (SolrServerException | IOException e) {
            logger.log(Level.SEVERE, "Failed retrieving outer document '"+outerId+"' from index "+solrCoreAddress);
            e.printStackTrace();
            throw new IOException(); //TODO 2019-08-20 ps: sinnvolle exception schmeißen
        }

        return response.getResults().get(0);
    }

    /**
     * Creates ids for the inner documents found within the given outer document (SolrDocument).
     * An inner id has the form "relativeInnerId_outerId".
     * The relativeInnerId is the position in the lists of the outer document, where the information for the desired inner document is located.
     * A SolrDocument in the TransparenzPortal is often actually a group of documents.
     * Each document in this group is called inner document.
     * The SolrDocument itself is called outer document.
     * @param solrDoc A SolrDocument retrieved from the Transparenz Portal solr index
     * @return List<String> The list of inner ids for the given outer document
     */
    private List<String> getInnerIds(SolrDocument solrDoc) {
        List<String> innerIds = new ArrayList<>();

        List<String> docResFormats = (List<String>) solrDoc.getFieldValue("res_format");
        List<String> docResUrls = (List<String>) solrDoc.getFieldValue("res_url");
        String outerId = (String) solrDoc.getFieldValue("id");

        try {
            logger.log(Level.FINEST, "Getting inner document ids for: "+outerId+"."); //TODO log level korrekt?

            if (docResFormats == null || docResFormats.isEmpty() || docResFormats.size() != docResUrls.size()) {
                throw new IllegalArgumentException();
            }

            int numOfInnerDocs = docResFormats.size();
            for (int i = 0; i < numOfInnerDocs; i++) {
                String innerDocFormat = docResFormats.get(i);

                if (innerDocFormat.equals("PDF")) {
                    String innerId = i + "_" + outerId;
                    innerIds.add(innerId);
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

        logger.log(Level.FINEST, "Found "+innerIds.size()+" inner documents for "+outerId+".");
        return innerIds;
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
        List<String> docResFulltexts = (List<String>) solrDoc.getFieldValue("res_fulltext");
        List<String> docResNames = (List<String>) solrDoc.getFieldValue("res_name");
        String outerId = (String) solrDoc.getFieldValue("id");
        String outerName = (String) solrDoc.getFieldValue("title");

        try {
            logger.log(Level.FINEST, "Processing inner document "+relativeInnerId+" from outer document "+outerId+"."); //TODO log-level korrekt?
            if (!isSolrDocWellFormed(docResFormats, docResUrls, docResFulltexts, docResNames, outerId)) {
                throw new IllegalArgumentException();
            }

            String innerDocFormat = docResFormats.get(relativeInnerId);
            String url = docResUrls.get(relativeInnerId);
            String fulltext = docResFulltexts.get(relativeInnerId);
            String name = docResNames.get(relativeInnerId);
            tpDocument.setResFormat(innerDocFormat);
            tpDocument.setInnerId(String.valueOf(relativeInnerId));
            tpDocument.setResUrl(url);
            tpDocument.setResFulltext(fulltext);
            tpDocument.setResName(name);
            tpDocument.setOuterId(outerId);
            tpDocument.setTitle(outerName);
        } catch (IllegalArgumentException e) {
            /** A SolrDocument is malformed if some mandatory information is missing.
             *  E.g. the number of fulltexts does not match the number of inner documents. */
            malformedSolrDocCounter++;
            logger.log(Level.INFO, "Malformed outer document: "+outerId+". Discarding document."); //TODO evtl. level fine oder finest
            return null;
        }

        return tpDocument;
    }


    /**
     * Tests if an outer document is well formed.
     * An outer document (SolrDocument) is well formed if all the mandatory fields (i.e. the parameters of this method)
     * are Non-Null AND Non-Empty AND if all lists have the same length.
     *
     * @param docResFormats The list of file-formats of the inner documents
     * @param docResUrls The list of URLs to the original files of the inner documents
     * @param docResFulltexts The list of fulltexts of the inner documents
     * @param outerId The ID of the outer document (containing the inner documents)
     * @return boolean - True if the outer document is wellformed.
     */
    private boolean isSolrDocWellFormed(List<String> docResFormats, List<String> docResUrls, List<String> docResFulltexts, List<String> docResNames, String outerId) {
        //TODO ps 2019-08-20 auch abfragen ob die listen empty sind
        return !(docResFormats == null || docResUrls == null || docResFulltexts == null || docResNames==null || outerId == null ||
                docResFormats.isEmpty() || docResUrls.isEmpty() || docResFulltexts.isEmpty() || docResNames.isEmpty() ||
                docResFormats.size() != docResUrls.size() || docResFormats.size() != docResFulltexts.size() || docResFormats.size() != docResNames.size());
    }

    @Override
    public void close() throws IOException{
        super.close();
    }




}