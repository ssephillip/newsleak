package uhh_lt.newsleak.reader;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import de.unihd.dbs.uima.types.heideltime.Dct;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Get;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;
import org.apache.solr.client.solrj.SolrQuery;
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
import uhh_lt.newsleak.resources.HooverResource;
import uhh_lt.newsleak.resources.MetadataResource;
import uhh_lt.newsleak.types.Metadata;
import uhh_lt.newsleak.types.TpResource;
import uhh_lt.newsleak.util.StatsService;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The HooverTransparenzReader connects to a running instance of the Hoover
 * text data extraction system created by the EIC.network (see
 * <a href="https://hoover.github.io">https://hoover.github.io</a>) and to a (non-public) Solr Index of the <a href="http://transparenz.hamburg.de/">Transparenzportal Hamburg</a>.
 * It utilizes the Hoover API to query for all extracted documents in a collection. It retrieves the documents from the Hoover Elasticsearch Index one by one and combines the document text with the metadata that was retrieved from the Transparenzportal Solr Index.
 * TODO vernünftigen kommentar schreiben
 * 
 * Hoover is expected to extract raw fulltext (regardless of any further NLP
 * application or human analysts requirement). Newsleak takes Hoover's output
 * and extracted file metadata (e.g. creation date).
 * 
 * Duplicated storage of fulltexts (with newly generated document IDs) is
 * necessary since we clean and preprocess raw data for further annotation
 * processes. Among others, this includes deletion of multiple blank lines
 * (often extracted from Excel sheets), dehyphenation at line endings (a result
 * from OCR-ed or badly encoded PDFs) , or splitting of long documents into
 * chunks of roughly page length.
 * 
 * This reader sets document IDs to 0. The final document IDs will be generated
 * as an automatically incremented by @see
 * uhh_lt.newsleak.writer.ElasticsearchDocumentWriter
 * 
 * Metadata is written into a temporary file on the disk to be inserted into the
 * newsleak postgres database lateron.
 */
public class HooverTransparenzReader extends NewsleakReader {

	/** The Constant RESOURCE_HOOVER. */
	public static final String RESOURCE_HOOVER = "hooverResource";

	/** The hoover resource. */
	@ExternalResource(key = RESOURCE_HOOVER)
	private HooverResource hooverResource;

	/** The Constant RESOURCE_METADATA. */
	public static final String RESOURCE_METADATA = "metadataResource";

	/** The metadata resource. */
	@ExternalResource(key = RESOURCE_METADATA)
	private MetadataResource metadataResource;

	/** The Constant PARAM_SCROLL_SIZE. */
	private static final String PARAM_SCROLL_SIZE = "10000";

	/** The Constant PARAM_SCROLL_TIME. */
	private static final String PARAM_SCROLL_TIME = "1m";

	/** The Constant TRANSPARENZ_CORE_ADDRESS. */
	public static final String TRANSPARENZ_CORE_ADDRESS = "transparenzcoreaddress";

	/** The URL to the core of the (solr) Transparenz index*/
	@ConfigurationParameter(name = TRANSPARENZ_CORE_ADDRESS, mandatory = true)
	private String solrCoreAddress;

	/** The solr client */
	HttpSolrClient solrClient;

	/** Map containing all data objects (documents) from the Transparenzportal solr index */
	Map<String, TpResource> tpResourcesMap;

	/** JEST client to run JSON API requests. */
	private JestClient client;

	/** The Hoover elasticsearch index. */
	private String esIndex;

	/** The total records. */
	private int totalRecords = 0;

	/** The current record. */
	private int currentRecord = 0;

	/** The list of all Ids. */
	private ArrayList<String> totalIdList;

	/** The date format. */
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

	/** The date created. */
	SimpleDateFormat dateCreated = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

	/** The date json. */
	SimpleDateFormat dateJson = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");

	/** The email regex pattern. */
	Pattern emailPattern = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+");

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.uima.fit.component.CasCollectionReader_ImplBase#initialize(org.
	 * apache.uima.UimaContext)
	 */
	@Override
	public void initialize(UimaContext context) throws ResourceInitializationException {
		super.initialize(context);
		logger = context.getLogger();

		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.INITIALIZE_TRANSPARENZ, Instant.now());
		transparenzInitialize(context);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.INITIALIZE_TRANSPARENZ, Instant.now());

		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.INITIALIZE_HOOVER, Instant.now());
		hooverInitialize(context);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.INITIALIZE_HOOVER, Instant.now());

	}

	/**
	 * Retrieves the metadata of all PDF documents from the Transparenzportal Solr Index and stores it in a map.
	 * During processing, this metadata is combined with the document texts that are being retrieved from Hoover.
	 *
	 * @param context The UIMA context
	 * @throws ResourceInitializationException
	 */
	private void transparenzInitialize(UimaContext context) throws ResourceInitializationException{
		logger.log(Level.INFO, "Getting metadata from Transparenzportal solr index: "+solrCoreAddress);

		int datasetsProcessed = 0;
		tpResourcesMap = new HashMap<>();
		solrClient = new HttpSolrClient.Builder(solrCoreAddress).build();

		SolrDocumentList datasets = getAllDatasetsFromSolr();
		int numOfDatasets = datasets.size();

		logger.log(Level.INFO, "Total number of datasets: " + numOfDatasets);
		logger.log(Level.INFO, "Getting Transparenzportal resources from dataset.");

		//gets the TP resources from all datasets
		for(SolrDocument dataset: datasets) {
			datasetsProcessed++;

			//Logs the progress
			if(datasetsProcessed%1000 == 0) {
				logger.log(Level.INFO, "Getting Transparenzportal resources from dataset number '" + datasetsProcessed + "' of '" + numOfDatasets + "' from index " + solrCoreAddress);
			}

			List<TpResource> tpResources = getAllTpResourcesFromDataset(dataset);

			//Stores the TpResources in a map so that they can be retrieved efficiently during processing.
			for(TpResource tpResource : tpResources){
				tpResourcesMap.put(tpResource.getAbsoluteResourceId(), tpResource);
			}
		}

		logger.log(Level.INFO, "Found " + tpResourcesMap.size() + " Transparenzportal resources in the Transparenzportal solr index.");
		logger.log(Level.INFO, "Finished getting metadata from Transparenzportal solr index");
	}

	private void hooverInitialize(UimaContext context) throws ResourceInitializationException{
		logger.log(Level.INFO, "Getting IDs from Hoover elasticsearch index");
		// init hoover connection
		client = hooverResource.getClient();
		esIndex = hooverResource.getIndex();

		// query hoover's elasticsearch index
		Search search = new Search.Builder(
				"{\"query\": {\"match_all\" : {}}, \"_source\" : false, \"size\" : " + PARAM_SCROLL_SIZE + "}")
				.addIndex(hooverResource.getIndex()).addType(HooverResource.HOOVER_DOCUMENT_TYPE)
				.setParameter(Parameters.SCROLL, PARAM_SCROLL_TIME).build();

		try {
			// run JEST request
			JestResult result = client.execute(search);

			totalIdList = new ArrayList<String>();

			JsonArray hits = result.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits");
			Integer total = result.getJsonObject().getAsJsonObject("hits").get("total").getAsInt();

			int nHits = hits.size();

			logger.log(Level.INFO, "Hits first result: " + nHits);
			logger.log(Level.INFO, "Hits total: " + total);

			totalIdList.addAll(hooverResource.getIds(hits));

			String scrollId = result.getJsonObject().get("_scroll_id").getAsString();

			// run scroll request to collect all Ids
			int i = 0;
			while (nHits > 0) {
				SearchScroll scroll = new SearchScroll.Builder(scrollId, PARAM_SCROLL_TIME).build();

				result = client.execute(scroll);

				hits = result.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits");
				nHits = hits.size();
				logger.log(Level.INFO, "Hits " + ++i + " result: " + nHits);
				totalIdList.addAll(hooverResource.getIds(hits));
				scrollId = result.getJsonObject().getAsJsonPrimitive("_scroll_id").getAsString();
			}

			if (maxRecords > 0 && maxRecords < totalIdList.size()) {
				totalIdList = new ArrayList<String>(totalIdList.subList(0, maxRecords));
			}

			totalRecords = totalIdList.size();


			logger.log(Level.INFO, "Found " + totalRecords + " ids in index " + esIndex);

		} catch (IOException e) {
			throw new ResourceInitializationException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.apache.uima.collection.CollectionReader#getNext(org.apache.uima.cas.CAS)
	 */
	public void getNext(CAS cas) throws IOException, CollectionException {
		JCas jcas;
		try {
			jcas = cas.getJCas();
		} catch (CASException e) {
			throw new CollectionException(e);
		}

		TpResource tpResource = null;
		String docIdNewsleak = null;
		JsonObject source = null;

		//Gets the next document from Hoover and tries to find the corresponding TpResource.
		//If no corresponding TpResource is found, the document is discarded.
		//This is done until a document is retrieved from Hoover for which a TpResource exists or if there are no more documents
		//in the Hoover Elasticsearch index.
		//Generally, it should never happen that no TpResource is found. The Hoover Elasticsearch index should only contain documents form the Transparenzportal.
		//If it happens, it is an indicator that something went wrong somewhere else. This mechanism is solely for the sake of robustness.
		//If no TpResource is found for the last document in the Hoover Elasticsearch index, then an Exception is thrown.
		while(tpResource == null) {
			// temporary document Id (a new id will be generated by the ElasticsearchDocumentWriter)
			docIdNewsleak = Integer.toString(currentRecord);
			String docIdHoover = totalIdList.get(currentRecord - 1);

			logger.log(Level.INFO, "Proceessing document: " + docIdHoover);

			Get get = new Get.Builder(hooverResource.getIndex(), docIdHoover).type(HooverResource.HOOVER_DOCUMENT_TYPE)
					.build();
			JestResult getResult = client.execute(get);
			JsonObject o = getResult.getJsonObject();
			source = o.get("_source").getAsJsonObject();
			String fileName = getField(source, "filename");
			fileName = fileName.split("\\.")[0];
			tpResource = tpResourcesMap.get(fileName);

			if(tpResource == null){
				logger.log(Level.INFO, "No Transparenzportal resource found in Transparenzportal Solr Index for file: "+fileName+". Discarding document: " + docIdHoover);

				if(!hasNext()){
					throw new CollectionException();
				}
			}
		}

		String origDocText = getField(source, "text");
		String newDocText = "";
		String field;
		JsonArray arrayField = null;


		// if document is not empty put email header information and TP ID, Tp Subject/Title and additional description in main text
		if(origDocText != null && !origDocText.trim().isEmpty()) {
			field = tpResource.getAbsoluteResourceId();
			if (field != null) {
				newDocText += "Dataset ID: "+field + "\n";
			}
			field = getField(source, "from");
			if (field != null) {
				String fromText = field.trim();
				newDocText += "From: " + fromText.replaceAll("<", "[").replaceAll(">", "]") + "\n";
			}
			arrayField = getFieldArray(source, "to");
			if (arrayField != null) {
				String toList = "";
				for (JsonElement item : arrayField) {
					String toListItem = item.getAsString().trim();
					toListItem = toListItem.replaceAll("<", "[").replaceAll(">", "]");
					toListItem = toListItem.replaceAll("\\s+", " ") + "\n";
					toList += toList.isEmpty() ? toListItem : "; " + toListItem;
				}
				newDocText += "To: " + toList;
			}
			field = getField(source, "subject");
			if (field != null) {
				newDocText += "Subject: " + field.trim() + "\n";
			}
			field = tpResource.getDatasetTitle();
			if (field != null) {
				newDocText += "Subject/Title: " + field + "\n";
			}
			field = tpResource.getName();
			if (field != null) {
				newDocText += "Additional Description: " + field + "\n";
			}
			if (!newDocText.isEmpty()) {
				newDocText += "\n-- \n\n";
			}
		}

		// add main text
		field = origDocText;
		if (field != null) {
			String completeText = field.trim();
			newDocText += cleanBodyText(completeText);
		}
		jcas.setDocumentText(newDocText);



		// set document metadata
		Metadata metaCas = new Metadata(jcas);
		metaCas.setDocId(docIdNewsleak);

		// date
		String docDate = "1900-01-01";
		field = tpResource.getDatasetDate();
		if(field != null){
			docDate = field;
		}
		metaCas.setTimestamp(docDate);

		// heideltime
		Dct dct = new Dct(jcas);
		dct.setValue(docDate);
		dct.addToIndexes();

		metaCas.addToIndexes();

		// write external metadata
		ArrayList<List<String>> metadata = new ArrayList<List<String>>();

		//Metadata from Transparenzportal
		//filename
		String fileName = "";
		field = tpResource.getDatasetTitle();
		if (field != null) {
			fileName = field;
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "Filename", fileName));
		}

		// Source URL
		field = tpResource.getUrl();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "Resource URL", field));
		}

		//Transparenzportal dataset ID
		field = tpResource.getDatasetId();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "Dataset ID", field));
		}

		//relative Transparenzportal resource ID
		field = tpResource.getRelativeResourceId();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "Relative resource ID", field));
		}


		//Metadata from Hoover
		// attachments
		Boolean booleanField = getFieldBoolean(source, "attachments");
		if (booleanField != null)
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "attachments", booleanField.toString()));
		// content-type
		field = getField(source, "content-type");
		if (field != null)
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "content-type", field));
		// file-type
		field = tpResource.getFormat();
		if (field != null)
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "filetype", field));
		// from
		field = getField(source, "from");
		if (field != null) {
			for (String email : extractEmail(field)) {
				metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "from", email));
			}
		}
		// to
		arrayField = getFieldArray(source, "to");
		if (arrayField != null) {
			for (JsonElement toList : arrayField) {
				for (String email : extractEmail(toList.getAsString())) {
					metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "to", email));
				}
			}
		}


		metadataResource.appendMetadata(metadata);
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
	 * For more information on datasets and resources see {@link TpResource}.
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
	 * Returns a string field value from a JSON object.
	 *
	 * @param o
	 *            the Json object
	 * @param fieldname
	 *            the fieldname
	 * @return the field
	 */
	private String getField(JsonObject o, String fieldname) {
		JsonElement fieldValue = o.get(fieldname);
		if (fieldValue == null) {
			return null;
		} else {
			return fieldValue.isJsonNull() ? null : fieldValue.getAsString();
		}
	}

	/**
	 * Returns a boolean field value from a Json Object.
	 *
	 * @param o
	 *            the Json object
	 * @param fieldname
	 *            the fieldname
	 * @return the field boolean
	 */
	private Boolean getFieldBoolean(JsonObject o, String fieldname) {
		JsonElement fieldValue = o.get(fieldname);
		if (fieldValue == null) {
			return null;
		} else {
			return fieldValue.isJsonNull() ? null : fieldValue.getAsBoolean();
		}
	}

	/**
	 * Returns an array field value from a Json Object.
	 *
	 * @param o
	 *            the Json object
	 * @param fieldname
	 *            the fieldname
	 * @return the field array
	 */
	private JsonArray getFieldArray(JsonObject o, String fieldname) {
		JsonElement fieldValue = o.get(fieldname);
		if (fieldValue == null) {
			return null;
		} else {
			return fieldValue.isJsonNull() ? null : fieldValue.getAsJsonArray();
		}
	}

	/**
	 * Extracts email addresses from a given string.
	 *
	 * @param s
	 *            the string to match email patterns in
	 * @return an array of email addresses
	 */
	private ArrayList<String> extractEmail(String s) {
		ArrayList<String> emails = new ArrayList<String>();
		Matcher m = emailPattern.matcher(s);
		while (m.find()) {
			emails.add(m.group());
		}
		return emails;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#getProgress()
	 */
	public Progress[] getProgress() {
		return new Progress[] { new ProgressImpl(Long.valueOf(currentRecord).intValue() - 1,
				Long.valueOf(totalRecords).intValue(), Progress.ENTITIES) };
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#hasNext()
	 */
	public boolean hasNext() throws IOException, CollectionException {
		if (currentRecord < totalRecords) {
			currentRecord++; //TODO 2019-06-24, ps: Is it correct that "currentrecords" is incremented here in this method? if yes, why??
			return true;
		} else {
			return false;
		}
	}

}
