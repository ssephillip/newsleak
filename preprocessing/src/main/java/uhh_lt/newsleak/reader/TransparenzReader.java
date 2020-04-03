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
import uhh_lt.newsleak.types.TpResource;

import java.io.IOException;
import java.util.*;

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
    private int totalNumOfTpResources = 0;

    /** Current document number. */
    private int currentTpResource = 0;

    /** Iterator holding all the outer ids from the Solr Index. */
    Iterator<String> absoluteResourceIdsIterator = null;

    /** Number of documents in the Solr Index where at least one of the necessary fields is missing or malformed. */
    int malformedDatasetCounter = 0;

    /** Number of inner documents. */
    int numOfPdfTpResources = 0;

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

        List<String> allAbsoluteResourceIds = new ArrayList<>();
        SolrDocumentList datasets = getDatasetIdsFromSolrIndex();

        logger.log(Level.INFO, "Total number of datasets: " + datasets.size());
        logger.log(Level.INFO, "Getting Transparenzportal resources from dataset.");
        for (SolrDocument dataset : datasets) {
            List<String> absoluteResourceIds = getAbsoluteResourceIds(dataset);
            allAbsoluteResourceIds.addAll(absoluteResourceIds);
        }

        totalNumOfTpResources = allAbsoluteResourceIds.size();
        currentTpResource = 0;
        absoluteResourceIdsIterator = allAbsoluteResourceIds.iterator();
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.uima.collection.CollectionReader#getNext(org.apache.uima.cas.CAS)
     */
    public void getNext(CAS cas) throws IOException, CollectionException {
        currentTpResource++; //TODO ps 2019-08-20: evtl. ans ende der methode verschieben?
        JCas jcas;
        try {
            jcas = cas.getJCas();
        } catch (CASException e) {
            throw new CollectionException(e);
        }

        String absoluteResourceId = absoluteResourceIdsIterator.next();
        Integer relativeResourceId = Integer.valueOf(absoluteResourceId.split("_")[0]); //TODO hier (und warsch. im ganzen reader) war die absoluteResourceId noch genau anders herum; dadurch ist der code hier flasch und muss geändert werden (evtl. ist es auch kein problem weil es im ganzen doc so gemacht wird und es nicht mit den filenames zu tun hat). für konsistenz muss man es aber trotzdem ndern.
        String datasetId = absoluteResourceId.split("_")[1];
        SolrDocument dataset = getDatasetFromSolrIndex(datasetId);
        TpResource tpResource = getTpResourceFromDataset(dataset, relativeResourceId);
        if(tpResource == null){
            throw new CollectionException(); //TODO ps 2019-08-21: was für eine sinnvolle exception kann man hier schmeißen
        }

        String docId = Integer.toString(currentTpResource);
        logger.log(Level.INFO, "Processing Transparenzportal resource: " + docId);

        jcas.setDocumentText(tpResource.getFulltext());

        // Set metadata
        Metadata metaCas = new Metadata(jcas);
        metaCas.setDocId(docId);
        metaCas.setTimestamp(tpResource.getDatasetDate());
        metaCas.addToIndexes();


        // write external metadata
        ArrayList<List<String>> metadata = new ArrayList<List<String>>();

        // filename, subject, path
        String fileName = "";
        String field = tpResource.getDatasetTitle(); //Das ist nicht wirklich der Filename
        if (field != null) {
            fileName = field;
            metadata.add(metadataResource.createTextMetadata(docId, "filename", fileName));
        }
        field = tpResource.getName();
        if (field != null) {
            metadata.add(metadataResource.createTextMetadata(docId, "subject", field));
        } else {
            if (!fileName.isEmpty()) {
                metadata.add(metadataResource.createTextMetadata(docId, "subject", fileName));
            }
        }

        field = tpResource.getUrl();
        if(field != null) {
            // Source Id
            metadata.add(metadataResource.createTextMetadata(docId, "Link", field));
        }

        // file-type
        field = tpResource.getFormat();
        if (field != null) {
            metadata.add(metadataResource.createTextMetadata(docId, "filetype", field));
        }

        // Transparenzportal dataset ID
        field = tpResource.getDatasetId();
        if (field != null) {
            metadata.add(metadataResource.createTextMetadata(docId, "dataset ID", field));
        }

        metadataResource.appendMetadata(metadata);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#getProgress()
     */
    public Progress[] getProgress() {
        return new Progress[]{new ProgressImpl(Long.valueOf(currentTpResource).intValue(),
                Long.valueOf(totalNumOfTpResources).intValue(), Progress.ENTITIES)};
    }


    /*
     * (non-Javadoc)
     *
     * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#hasNext()
     */
    public boolean hasNext() throws IOException, CollectionException {
        if (currentTpResource > maxRecords)
            return false;
        return absoluteResourceIdsIterator.hasNext();
    }


    /**
     * Gets all outer document ids from the solr index that match the query.
     * The additional lists are retrieved to verify that the documents are well formed.
     * @return SolrDocumentList
     * @throws ResourceInitializationException
     */
    private SolrDocumentList getDatasetIdsFromSolrIndex() throws ResourceInitializationException {
        QueryResponse response = null;

        SolrQuery documentQuery = new SolrQuery("res_format:\"PDF\"");
        documentQuery.addField("id");
        documentQuery.addField("res_format");
        documentQuery.addField("res_url");
        documentQuery.setRows(Integer.MAX_VALUE);

        try {
            logger.log(Level.INFO, "Getting dataset ids from index " + solrCoreAddress+" with filter '"+documentQuery.getQuery()+"'");
            response = solrClient.query(documentQuery);

            if (response == null) {
                throw new IOException(); //TODO 2019-07-11 ps: ist diese Exception hier korrekt?
            }
        } catch (SolrServerException | IOException e) {
            logger.log(Level.SEVERE, "Failed retrieving dataset ids from index "+solrCoreAddress);
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
    private SolrDocument getDatasetFromSolrIndex(String datasetId) throws IOException {
        QueryResponse response = null;


        SolrQuery documentQuery = new SolrQuery("id:"+datasetId);
        documentQuery.addField("id");
        documentQuery.addField("res_format");
        documentQuery.addField("res_url");
        documentQuery.addField("res_fulltext");
        documentQuery.addField("res_name");
        documentQuery.addField("title");
        documentQuery.addField("publishing_date");
        documentQuery.setRows(Integer.MAX_VALUE); //TODO evtl. weg, da immer nur ein ergebnis kommen sollte


        try {
            logger.log(Level.INFO, "Getting dataset '"+datasetId+"' from index " + solrCoreAddress);
            response = solrClient.query(documentQuery);

            if (response == null) {
                throw new IOException(); //TODO 2019-07-11 ps: ist diese Exception hier korrekt?
            }
        } catch (SolrServerException | IOException e) {
            logger.log(Level.SEVERE, "Failed retrieving dataset '"+datasetId+"' from index "+solrCoreAddress);
            logger.log(Level.SEVERE, "Error message: " + e.getMessage());
            logger.log(Level.SEVERE, "Localized error message: " + e.getLocalizedMessage());

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
     * @param dataset A SolrDocument retrieved from the Transparenz Portal solr index
     * @return List<String> The list of inner ids for the given outer document
     */
    private List<String> getAbsoluteResourceIds(SolrDocument dataset) {
        List<String> absoluteResourceIds = new ArrayList<>();

        List<String> resourceFormats = (List<String>) dataset.getFieldValue("res_format");
        List<String> resourceUrls = (List<String>) dataset.getFieldValue("res_url");
        String datasetId = (String) dataset.getFieldValue("id");

        try {
            logger.log(Level.FINEST, "Getting absolute resource ids for: "+datasetId+"."); //TODO log level korrekt?

            if (resourceFormats == null || resourceFormats.isEmpty() || resourceFormats.size() != resourceUrls.size()) {
                throw new IllegalArgumentException();
            }

            int numOfTpResources = resourceFormats.size();
            for (int i = 0; i < numOfTpResources; i++) {
                String resourceFormat = resourceFormats.get(i);

                if (resourceFormat.equals("PDF")) {
                    String absoluteResourceId = i + "_" + datasetId;
                    absoluteResourceIds.add(absoluteResourceId);
                    numOfPdfTpResources++;
                }
            }
        } catch (IllegalArgumentException e) {
            /** A SolrDocument is malformed if some mandatory information is missing.
             *  E.g. the number of fulltexts does not match the number of inner documents. */
            malformedDatasetCounter++;
            logger.log(Level.INFO, "Malformed document: "+datasetId+". Discarding document."); //TODO evtl. level fine oder finest
            return new ArrayList<>();
        }

        logger.log(Level.FINEST, "Found "+absoluteResourceIds.size()+" Transparenzportal resources for dataset: "+datasetId+".");
        return absoluteResourceIds;
    }


    /**
     * Gets an inner document from the given outer document (SolrDocument).
     * The relativeResourceId is the position in the lists of the outer document, where the information for the desired inner document is located.
     * A SolrDocument in the TransparenzPortal is often actually a group of documents.
     * Each document in this group is called inner document.
     * The SolrDocument itself is called outer document.
     * Whenever a field has the prefix "res" it referrs to a inner document (e.g. res_fulltext refers to the fulltext a an inner document).
     * @param dataset A SolrDocument retrieved from the Transparenz Portal solr index
     * @param relativeResourceId An int specifying which of the inner documents shall be extracted.
     * @return TpDocument The inner document "extracted" from the given outer document.
     */
    private TpResource getTpResourceFromDataset(SolrDocument dataset, int relativeResourceId) {
        TpResource tpResource = new TpResource();

        List<String> resourceFormats = (List<String>) dataset.getFieldValue("res_format");
        List<String> resourceUrls = (List<String>) dataset.getFieldValue("res_url");
        List<String> resourceFulltexts = (List<String>) dataset.getFieldValue("res_fulltext");
        List<String> resourceNames = (List<String>) dataset.getFieldValue("res_name");
        String datasetId = (String) dataset.getFieldValue("id");
        String datasetTitle = (String) dataset.getFieldValue("title");
        String datasetDate = getDateAsString((Date) dataset.getFieldValue("publishing_date"));



        try {
            logger.log(Level.FINEST, "Processing Transparenzportal resource "+relativeResourceId+" from dataset "+datasetId+"."); //TODO log-level korrekt?
            if (!isDatasetWellFormed(resourceFormats, resourceUrls, resourceFulltexts, resourceNames, datasetId)) {
                throw new IllegalArgumentException();
            }

            String resourceFormat = resourceFormats.get(relativeResourceId);
            String resourceUrl = resourceUrls.get(relativeResourceId);
            String resourceFulltext = resourceFulltexts.get(relativeResourceId);
            String resourceName = resourceNames.get(relativeResourceId);
            tpResource.setFormat(resourceFormat);
            tpResource.setRelativeResourceId(String.valueOf(relativeResourceId));
            tpResource.setUrl(resourceUrl);
            tpResource.setFulltext(resourceFulltext);
            tpResource.setName(resourceName);
            tpResource.setDatasetId(datasetId);
            tpResource.setDatasetTitle(datasetTitle);
            tpResource.setDatasetDate(datasetDate);
        } catch (IllegalArgumentException e) {
            /** A SolrDocument is malformed if some mandatory information is missing.
             *  E.g. the number of fulltexts does not match the number of inner documents. */
            malformedDatasetCounter++;
            logger.log(Level.INFO, "Malformed dataset: "+datasetId+". Discarding dataset."); //TODO evtl. level fine oder finest
            return null;
        }

        return tpResource;
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
     * @param resourceFormats The list of file-formats of the inner documents
     * @param resourceUrls The list of URLs to the original files of the inner documents
     * @param resourceFulltexts The list of fulltexts of the inner documents
     * @param datasetId The ID of the outer document (containing the inner documents)
     * @return boolean - True if the outer document is wellformed.
     */
    private boolean isDatasetWellFormed(List<String> resourceFormats, List<String> resourceUrls, List<String> resourceFulltexts, List<String> resourceNames, String datasetId) {
        //TODO ps 2019-08-20 auch abfragen ob die listen empty sind
        return !(resourceFormats == null || resourceUrls == null || resourceFulltexts == null || resourceNames==null || datasetId == null ||
                resourceFormats.isEmpty() || resourceUrls.isEmpty() || resourceFulltexts.isEmpty() || resourceNames.isEmpty() ||
                resourceFormats.size() != resourceUrls.size() || resourceFormats.size() != resourceFulltexts.size() || resourceFormats.size() != resourceNames.size());
    }

    @Override
    public void close() throws IOException{
        super.close();
    }




}