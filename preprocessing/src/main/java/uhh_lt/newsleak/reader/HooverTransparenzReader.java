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
import uhh_lt.newsleak.types.TpDocument;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The HooverElasticsearchReader connects to a running instance of the Hoover
 * text data extraction system created by the EIC.network (see
 * <a href="https://hoover.github.io">https://hoover.github.io</a>). It utilizes
 * the Hoover API to query for all extracted documents in a collection.
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

	/** Map containing all inner documents from the Transparenzportal solr index */
	Map<String, TpDocument> tpDocumentsMap;

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

		transparenzInitialize(context);

		hooverInitialize(context);

	}

	private void transparenzInitialize(UimaContext context) throws ResourceInitializationException{
		logger.log(Level.INFO, "Getting metadata from Transparenzportal solr index: "+solrCoreAddress);
		tpDocumentsMap = new HashMap<>();
		solrClient = new HttpSolrClient.Builder(solrCoreAddress).build();


		SolrDocumentList solrDocuments = getAllOuterDocumentsFromSolr();
		int outerDocsProcessed = 0;

		logger.log(Level.INFO, "Total number of outer documents: " + solrDocuments.size());
		logger.log(Level.INFO, "Getting inner documents from outer documents.");

		for(SolrDocument solrDoc: solrDocuments) {
			outerDocsProcessed++;

			List<TpDocument> innerDocuments = getAllInnerDocumentsFromOuterDoc(solrDoc, outerDocsProcessed, solrDocuments.size());

			for(TpDocument tpDocument: innerDocuments){
				tpDocumentsMap.put(tpDocument.getId(), tpDocument);
			}
		}

		logger.log(Level.INFO, "Found " + tpDocumentsMap.size() + " inner documents in the Transparenzportal solr index.");
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

		TpDocument tpDocument = null;
		String docIdNewsleak = null;
		JsonObject source = null;

		while(tpDocument == null) {
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
			tpDocument = tpDocumentsMap.get(fileName);

			if(tpDocument == null){
				logger.log(Level.INFO, "No corresponding document found in Transparenzportal solr index. Discarding document: " + docIdHoover);
				if(!hasNext()){ //TODO relies on the "current" counter being incremented in the hasNext method. When this weird implementation changes the "current" counter needs to be incremented here
					tpDocument = new TpDocument();
					break;
				}
			}else{
				logger.log(Level.INFO, "breakkkpoint");
			}
		}

		String docText = "";
		String field;

		// put email header information and TP ID in main text

		field = tpDocument.getId();
		if (field != null) {
			docText += field+"\n";
		}
		field = getField(source, "from");
		if (field != null) {
			String fromText = field.trim();
			docText += "From: " + fromText.replaceAll("<", "[").replaceAll(">", "]") + "\n";
		}
		JsonArray arrayField = getFieldArray(source, "to");
		if (arrayField != null) {
			String toList = "";
			for (JsonElement item : arrayField) {
				String toListItem = item.getAsString().trim();
				toListItem = toListItem.replaceAll("<", "[").replaceAll(">", "]");
				toListItem = toListItem.replaceAll("\\s+", " ") + "\n";
				toList += toList.isEmpty() ? toListItem : "; " + toListItem;
			}
			docText += "To: " + toList;
		}
		field = getField(source, "subject");
		if (field != null) {
			docText += "Subject: " + field.trim() + "\n";
		}
		field = tpDocument.getTitle();
		if (field != null) {
			docText += "Subject/Title: " + field + "\n";
		}
		field = tpDocument.getResName();
		if (field != null) {
			docText += "Additional Description: " + field + "\n";
		}
		if (!docText.isEmpty()) {
			docText += "\n-- \n\n";
		}

		// add main text
		field = getField(source, "text");
		if (field != null) {
			String completeText = field.trim();
			docText += cleanBodyText(completeText);
		}
		jcas.setDocumentText(docText);



		// set document metadata
		Metadata metaCas = new Metadata(jcas);
		metaCas.setDocId(docIdNewsleak);

		// date
		String docDate = "1900-01-01";
		field = tpDocument.getDate();
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
		field = tpDocument.getTitle();
		if (field != null) {
			fileName = field;
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "filename", fileName));
		}
		field = tpDocument.getTitle();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "subject", field));
		} else {
			if (!fileName.isEmpty()) {
				metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "subject", fileName));
			}
		}

		// Source URL
		field = tpDocument.getResUrl();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "Link", field));
		}

		//Transparenzportal ID
		field = tpDocument.getOuterId();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "transparenz-id", field));
		}

		//inner Transparenzportal ID
		field = tpDocument.getInnerId();
		if (field != null) {
			metadata.add(metadataResource.createTextMetadata(docIdNewsleak, "inner transparenz-id", field));
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
		field = tpDocument.getResFormat();
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


	public SolrDocumentList getAllOuterDocumentsFromSolr() throws ResourceInitializationException{
		QueryResponse response = null;

		SolrQuery documentQuery = new SolrQuery("*:*");
		documentQuery.addField("id");
		documentQuery.addField("res_format");
		documentQuery.addField("res_url");
		documentQuery.addField("res_name");
		documentQuery.addField("title");
		documentQuery.addField("publishing_date");
		documentQuery.setRows(Integer.MAX_VALUE); //TODO evtl. weg, da immer nur ein ergebnis kommen sollte


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

	public List<TpDocument> getAllInnerDocumentsFromOuterDoc(SolrDocument outerDocument, int outerDocsProcessed, int numOfOuterDocs){
		List<TpDocument> innerDocuments = new ArrayList<>();
		List<String> docResFormats = (List<String>) outerDocument.getFieldValue("res_format");
		List<String> docResUrls = (List<String>) outerDocument.getFieldValue("res_url");
		List<String> docResNames = (List<String>) outerDocument.getFieldValue("res_name");
		String outerId = (String) outerDocument.getFieldValue("id");


		if(outerDocsProcessed%1000 == 0) {
			logger.log(Level.INFO, "Getting inner documents from outer document number '" + outerDocsProcessed + "' of '" + numOfOuterDocs + "' from index " + solrCoreAddress);
		}

		if(isSolrDocWellFormed(docResFormats, docResUrls, docResNames, outerId)){
			for (int i = 0; i < docResUrls.size(); i++){
				TpDocument innerDocument = getInnerDocFromOuterDoc(outerDocument, i);
				innerDocuments.add(innerDocument);
			}
		}else{
			System.out.println("Malformed outer document: "+outerId+". Discarding document.");
		}

		return innerDocuments;
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
		String innerId = String.valueOf(relativeInnerId);
		String outerName = (String) solrDoc.getFieldValue("title");
		String date = getDateAsString((Date) solrDoc.getFieldValue("publishing_date"));

		String innerDocFormat = docResFormats.get(relativeInnerId);
		String url = docResUrls.get(relativeInnerId);
		String name = docResNames.get(relativeInnerId);
		tpDocument.setResFormat(innerDocFormat.toLowerCase());
		tpDocument.setInnerId(innerId);
		tpDocument.setOuterId(outerId);
		tpDocument.setId(outerId+"_"+innerId);
		tpDocument.setResUrl(url);
		tpDocument.setResName(name);
		tpDocument.setTitle(outerName);
		tpDocument.setDate(date);

		return tpDocument;
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