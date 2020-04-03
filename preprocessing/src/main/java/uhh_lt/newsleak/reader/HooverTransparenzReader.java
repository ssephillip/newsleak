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
	 * Retrieves the metadata of all PDF documents from the Transparenzportal SOlr Index and stores it in a map.
	 * During processing, this metadata is combined with the document texts that are being retrieved from Hoover.
	 *
	 * @param context The UIMA context
	 * @throws ResourceInitializationException
	 */
	private void transparenzInitialize(UimaContext context) throws ResourceInitializationException{
		logger.log(Level.INFO, "Getting metadata from Transparenzportal solr index: "+solrCoreAddress);

		tpResourcesMap = new HashMap<>();
		solrClient = new HttpSolrClient.Builder(solrCoreAddress).build();


		SolrDocumentList solrDocuments = getAllDatasetsFromSolr();
		int datasetsProcessed = 0;

		logger.log(Level.INFO, "Total number of datasets: " + solrDocuments.size());
		logger.log(Level.INFO, "Getting resources from dataset.");

		for(SolrDocument solrDoc: solrDocuments) {
			datasetsProcessed++;

			List<TpResource> tpResources = getAllTpResourcesFromDataset(solrDoc, datasetsProcessed, solrDocuments.size());

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

		while(tpResource == null) {
			// temporary document Id (a new id will be generated by the
			// ElasticsearchDocumentWriter)
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
				logger.log(Level.INFO, "No corresponding document found in Transparenzportal solr index for file: "+fileName+". Discarding document: " + docIdHoover);
				if(!hasNext()){ //TODO relies on the "current" counter being incremented in the hasNext method. When this weird implementation changes the "current" counter needs to be incremented here
					tpResource = new TpResource();
					break;
				}
			}
		}

		String origDocText = getField(source, "text");
		String newDocText = "";
		String field;
		JsonArray arrayField = null;


		// if document is not empty put email header information and TP ID in main text
		if(origDocText != null && !origDocText.trim().isEmpty()) {
			field = tpResource.getAbsoluteResourceId();
			if (field != null) {
				newDocText += "Transparenz-ID: "+field + "\n";
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

		// filename, subject, path
		String fileName = "";
		field = tpResource.getDatasetTitle();
		if (field != null) {
			fileName = field;
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "filename", fileName));
		}
		field = tpResource.getDatasetTitle();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "subject", field));
		} else {
			if (!fileName.isEmpty()) {
				metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "subject", fileName));
			}
		}

		// Source URL
		field = tpResource.getUrl();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "Link", field));
		}

		//Transparenzportal dataset ID
		field = tpResource.getDatasetId();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "dataset ID", field));
		}

		//relative Transparenzportal resource ID
		field = tpResource.getRelativeResourceId();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "relative resource ID", field));
		}

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


	public SolrDocumentList getAllDatasetsFromSolr() throws ResourceInitializationException{
		QueryResponse response = null;

		SolrQuery documentQuery = new SolrQuery("*:*");//"id:f4c2d114-65fe-4e68-a162-ceefed275aa0");
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
				throw new ResourceInitializationException(); //TODO 2020-01-07 ps: ist diese Exception hier korrekt?
			}
		} catch (Exception e) {
			System.out.println("Failed retrieving outer documents from index "+solrCoreAddress);

			e.printStackTrace();
			throw new ResourceInitializationException();
		}

		return response.getResults();
	}

	public List<TpResource> getAllTpResourcesFromDataset(SolrDocument dataset, int datasetsProcessed, int numOfDatasets){
		List<TpResource> tpResources = new ArrayList<>();
		List<String> resourceFormats = (List<String>) dataset.getFieldValue("res_format");
		List<String> resourceUrls = (List<String>) dataset.getFieldValue("res_url");
		List<String> resourceNames = (List<String>) dataset.getFieldValue("res_name");
		String datasetId = (String) dataset.getFieldValue("id");


		if(datasetsProcessed%1000 == 0) {
			logger.log(Level.INFO, "Getting Transparenzportal resources from dataset number '" + datasetsProcessed + "' of '" + numOfDatasets + "' from index " + solrCoreAddress);
		}

		if(isSolrDocWellFormed(resourceFormats, resourceUrls, resourceNames, datasetId)){
			for (int i = 0; i < resourceUrls.size(); i++){
				TpResource tpResource = getTpResourceFromDataset(dataset, i);
				tpResources.add(tpResource);
			}
		}else{
			System.out.println("Malformed dataset: "+datasetId+". Discarding dataset.");
		}

		return tpResources;
	}

	/**
	 * Tests if an outer document is well formed.
	 * An outer document (SolrDocument) is well formed if all the mandatory fields (i.e. the parameters of this method)
	 * are Non-Null AND Non-Empty AND if all lists have the same length.
	 *
	 * @param resourceFormats The list of file-formats of the inner documents
	 * @param resourceUrls The list of URLs to the original files of the inner documents
	 * @param datasetId The ID of the outer document (containing the inner documents)
	 * @return boolean - True if the outer document is wellformed.
	 */
	private boolean isSolrDocWellFormed(List<String> resourceFormats, List<String> resourceUrls, List<String> resourceNames, String datasetId) {
		//TODO ps 2019-08-20 auch abfragen ob die listen empty sind
		return !(resourceFormats == null || resourceUrls == null || resourceNames==null || datasetId == null ||
				resourceFormats.isEmpty() || resourceUrls.isEmpty() || resourceNames.isEmpty() ||
				resourceFormats.size() != resourceUrls.size() || resourceFormats.size() != resourceNames.size());
	}



	/**
	 * Gets an inner document from the given outer document (SolrDocument).
	 * The relativeResourceIdInt is the position in the lists of the outer document, where the information for the desired inner document is located.
	 * A SolrDocument in the TransparenzPortal is often actually a group of documents.
	 * Each document in this group is called inner document.
	 * The SolrDocument itself is called outer document.
	 * Whenever a field has the prefix "res" it referrs to a inner document (e.g. res_fulltext refers to the fulltext a an inner document).
	 * @assert solrDoc is wellformed
	 * @param solrDoc A SolrDocument retrieved from the Transparenz Portal solr index
	 * @param relativeResourceIdInt An int specifying which of the inner documents shall be extracted.
	 * @return TpDocument The inner document "extracted" from the given outer document.
	 */
	private TpResource getTpResourceFromDataset(SolrDocument solrDoc, int relativeResourceIdInt) {
		TpResource tpResource = new TpResource();

		List<String> resourceFormats = (List<String>) solrDoc.getFieldValue("res_format");
		List<String> resourceUrls = (List<String>) solrDoc.getFieldValue("res_url");
		List<String> resourceNames = (List<String>) solrDoc.getFieldValue("res_name");
		String datasetId = (String) solrDoc.getFieldValue("id");
		String relativeResourceId = String.valueOf(relativeResourceIdInt);
		String datasetTitle = (String) solrDoc.getFieldValue("title");
		String datasetDate = getDateAsString((Date) solrDoc.getFieldValue("publishing_date"));

		String resourceFormat = resourceFormats.get(relativeResourceIdInt);
		String resourceUrl = resourceUrls.get(relativeResourceIdInt);
		String resourceName = resourceNames.get(relativeResourceIdInt);
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
			currentRecord++; //TODO 2019-06-24, ps: Stimmt das? (das currentrecords erhöht wird?) wenn ja, warum?
			return true;
		} else {
			return false;
		}
	}

}
