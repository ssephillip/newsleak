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
import org.apache.solr.client.solrj.impl.HttpSolrClient;
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
import uhh_lt.newsleak.services.StatsService;
import uhh_lt.newsleak.services.TransparenzSolrService;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The HooverTransparenzReader connects to a running instance of the Hoover
 * text data extraction system created by the EIC.network (see
 * <a href="https://hoover.github.io">https://hoover.github.io</a>). It utilizes the Hoover API to query for all extracted documents in a collection.
 * Additionally it retrieves the metadata of all datasets stored in the (non-public) Solr Index of the <a href="http://transparenz.hamburg.de/">Transparenzportal Hamburg</a>.
 * The resources are extracted from the datasets and stored in {@link TpResource} objects. The extracted {@link TpResource}s are stored in a {@link Map}.
 * The absolute resource IDs are used as keys.
 * The absolute resource IDs are also the filenames of the documents provided by Hoover.
 * Consequentially, the filenames of the documents retrieved from Hoover can be used directly as keys to retrieve the corresponding {@link TpResource} from the {@link Map}.
 *
 * During processing, the information (e.g. fulltext, metadata) retrieved from the Hoover Elasticsearch Index is combined with the metadata of the corresponding {@link TpResource}.
 * Due to the way the {@link TpResource}s are stored, combining the data is very fast.
 *
 * For more information on Hoover see {@link HooverElasticsearchReader}.
 *
 * For more information on datasets and resources see {@link TpResource}.
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

	/** The URL to the core of the Transparenzportal Solr Index*/
	@ConfigurationParameter(name = TRANSPARENZ_CORE_ADDRESS, mandatory = true)
	private String solrCoreAddress;

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

	/** The solr client */
	HttpSolrClient solrClient;

	/** Map containing all {@link TpResource}s from the Transparenzportal Solr Index */
	Map<String, TpResource> tpResourcesMap;

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

		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.INITIALIZE_TRANSPARENZ);
		transparenzInitialize(context);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.INITIALIZE_TRANSPARENZ);

		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.INITIALIZE_HOOVER);
		hooverInitialize(context);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.INITIALIZE_HOOVER);

	}

	/**
	 * Retrieves the metadata of all PDF resources stored in the Transparenzportal Solr Index and stores it in the {@link #tpResourcesMap}.
	 * During processing, this metadata is combined with the document texts that are being retrieved from Hoover.
	 *
	 * For more information on datasets and resources see {@link TpResource}.
	 *
	 * @param context The UIMA context
	 * @throws ResourceInitializationException if the datasets cannot be retrieved from the Transparenzportal Solr Index.
	 */
	private void transparenzInitialize(UimaContext context) throws ResourceInitializationException {
		logger.log(Level.INFO, "Getting metadata from Transparenzportal solr index: " + solrCoreAddress);

		tpResourcesMap = new HashMap<>();

		//gets all resources from solr
		TransparenzSolrService tpSolrService = new TransparenzSolrService(solrCoreAddress, logger);
		List<TpResource> tpResources = tpSolrService.getAllResourcesFromSolr();

		//Stores the TpResources in a map so that they can be retrieved efficiently during processing.
		for (TpResource tpResource : tpResources) {
			tpResourcesMap.put(tpResource.getAbsoluteResourceId(), tpResource);
		}


		logger.log(Level.INFO, "Found " + tpResourcesMap.size() + " Transparenzportal resources in the Transparenzportal solr index.");
		logger.log(Level.INFO, "Finished getting metadata from Transparenzportal solr index");
	}

	/**
	 * Retrieves the IDs of all documents in the Hoover Elasticsearch index.
	 * Copied from {@link HooverElasticsearchReader#initialize(UimaContext)}.
	 *
	 * @param context The UIMA context.
	 * @throws ResourceInitializationException if an {@link IOException} occurs during this method.
	 */
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

			//This is done to keep consistency with the other readers
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "Subject", fileName));
		}

		// Source URL
		field = tpResource.getUrl();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "Link", field));
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
