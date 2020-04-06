package uhh_lt.newsleak.services;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import uhh_lt.newsleak.types.TpResource;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TransparenzSolrService {
    /** The solr client */
    HttpSolrClient solrClient;

    /** The address (URL) to the Transparenzportal Solr Index core */
    String solrCoreAddress;

    /** The UIMA logger */
    Logger logger;


    public TransparenzSolrService(String solrCoreAddress, Logger logger){
        this.solrCoreAddress = solrCoreAddress;
        this.logger = logger;
    }

    /**
     * Gets all resources form the Transparenzportal Solr Index and stores them in {@link TpResource} objects.
     *
     * @return a list containing all the retrieved resources
     * @throws ResourceInitializationException if no data could be retrieved from the Transparenzportal Solr Index
     */
    public List<TpResource> getAllResourcesFromSolr() throws ResourceInitializationException {
        List<TpResource> tpResources = new ArrayList<>();

        solrClient = new HttpSolrClient.Builder(solrCoreAddress).build();

        //gets the datasets from solr
        SolrDocumentList datasets = getAllDatasetsFromSolr();

        logger.log(Level.INFO, "Total number of datasets: " + datasets.size());
        logger.log(Level.INFO, "Getting Transparenzportal resources from datasets.");

        //gets the TP resources from the datasets
        for(SolrDocument dataset: datasets) {
            List<TpResource> tempTpResources = getAllTpResourcesFromDataset(dataset);

            tpResources.addAll(tempTpResources);
        }

        return tpResources;
    }


    /**
     * Gets all datasets from the Transparenzportal Solr Index.
     * For more information on datasets and resources see {@link TpResource}
     *
     * @return A list of datasets. Each dataset is represented by a {@link SolrDocument})
     * @throws ResourceInitializationException if no datasets could be retrieved (for any reason)
     */
    public SolrDocumentList getAllDatasetsFromSolr() throws ResourceInitializationException{
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
                throw new ResourceInitializationException();
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed retrieving datasets from Transparenzportal Solr Index "+solrCoreAddress);

            e.printStackTrace();
            throw new ResourceInitializationException();
        }

        return response.getResults();
    }


    /**
     * Stores the resources contained in the given dataset in {@link TpResource} objects.
     *
     * For more information on datasets and resources see {@link TpResource}.
     *
     * @param dataset The dataset that contains the resources
     * @return All {@link TpResource}s contained in the dataset or an empty list if the dataset is not well-formed (see {@link #isDatasetWellFormed(List, List, List, String)}).
     */
    public List<TpResource> getAllTpResourcesFromDataset(SolrDocument dataset){
        List<TpResource> tpResources = new ArrayList<>();
        List<String> resourceFormats = (List<String>) dataset.getFieldValue("res_format");
        List<String> resourceUrls = (List<String>) dataset.getFieldValue("res_url");
        List<String> resourceNames = (List<String>) dataset.getFieldValue("res_name");
        String datasetId = (String) dataset.getFieldValue("id");


        if(isDatasetWellFormed(resourceFormats, resourceUrls, resourceNames, datasetId)){
            for (int i = 0; i < resourceUrls.size(); i++){
                TpResource tpResource = getTpResourceFromDataset(dataset, i);
                tpResources.add(tpResource);
            }
        }else{
            logger.log(Level.INFO, "Ill-formed dataset: "+datasetId+". Discarding dataset.");
        }

        return tpResources;
    }



    /**
     * Extracts a resource from the given dataset and stores it in a {@link TpResource} object.
     *
     * For more information on datasets and resources see {@link TpResource}.
     *
     * @assert dataset is well-formed (see {@link #isDatasetWellFormed(List, List, List, String)})
     * @param dataset The dataset containing the resource
     * @param relativeResourceIdInt The relative ID of the resource that is supposed to be extracted
     * @return The {@link TpResource} extracted from the dataset
     */
    private TpResource getTpResourceFromDataset(SolrDocument dataset, int relativeResourceIdInt) {
        TpResource tpResource = new TpResource();

        //Gets the information of all resources contained in the dataset and of the dataset from the dataset
        List<String> resourceFormats = (List<String>) dataset.getFieldValue("res_format");
        List<String> resourceUrls = (List<String>) dataset.getFieldValue("res_url");
        List<String> resourceNames = (List<String>) dataset.getFieldValue("res_name");
        String datasetId = (String) dataset.getFieldValue("id");
        String relativeResourceId = String.valueOf(relativeResourceIdInt);
        String datasetTitle = (String) dataset.getFieldValue("title");
        String datasetDate = getDateAsString((Date) dataset.getFieldValue("publishing_date"));

        //Gets the information of the resource that is supposed to be extracted
        String resourceFormat = resourceFormats.get(relativeResourceIdInt);
        String resourceUrl = resourceUrls.get(relativeResourceIdInt);
        String resourceName = resourceNames.get(relativeResourceIdInt);

        //Stores the extracted information in the {@link TpResource} object.
        tpResource.setFormat(resourceFormat.toLowerCase());
        tpResource.setRelativeResourceId(relativeResourceId);
        tpResource.setDatasetId(datasetId);
        tpResource.setAbsoluteResourceId(datasetId+"_"+relativeResourceId);
        tpResource.setUrl(resourceUrl);
        tpResource.setName(resourceName);
        tpResource.setDatasetTitle(datasetTitle);
        tpResource.setDatasetDate(datasetDate);

        return tpResource;
    }



    /**
     * Tests if a dataset is well-formed.
     * A dataset is well-formed if all the parameters of this method are Non-Null AND all the lists are Non-Empty AND all the lists have the same length.
     * Explanation:
     * If one of the parameters would be Null or empty (if it is a list), all resources contained in the dataset would miss a mandatory value.
     * If the lists would not have the same length, it is not possible to determine which information belongs to which resource.
     *
     * For more information on datasets, resources and well-formed datasets see {@link TpResource}.
     *
     * @param resourceFormats The list of file-formats of the resources contained in the dataset
     * @param resourceUrls The list of URLs to the actual files corresponding to the resources
     * @param resourceNames The list of names of the resources contained in the dataset
     * @param datasetId The ID of the dataset
     * @return True if the dataset is well-formed.
     */
    private boolean isDatasetWellFormed(List<String> resourceFormats, List<String> resourceUrls, List<String> resourceNames, String datasetId) {

        return !(resourceFormats == null || resourceUrls == null || resourceNames==null || datasetId == null ||
                resourceFormats.isEmpty() || resourceUrls.isEmpty() || resourceNames.isEmpty() ||
                resourceFormats.size() != resourceUrls.size() || resourceFormats.size() != resourceNames.size());
    }


    /**
     * Transforms a {@link Date} into a {@link String}.
     *
     * @param date The date that is supposed to be transformed.
     * @return The {@link String} that was created based on the given {@link Date}.
     */
    private String getDateAsString(Date date){
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


}
