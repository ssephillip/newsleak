package uhh_lt.newsleak.preprocessing;

/**
 * Main class to create a new newsleak collection. It runs the information
 * extraction pipeline which writes to a relational database. The second step
 * then indexes retrieved entities and metadata from the database for fast
 * restrieval and aggregation to the system's fulltext index (elasticsearch).
 * 
 * For configuration it expects a config file as parsed in @see
 * uhh_lt.newsleak.preprocessing.NewsleakPreprocessor
 */
public class CreateCollection {

	/**
	 * The main method to start the creation process of a new collection.
	 *
	 * @param args
	 *             expects a paramter -c to point to the config file
	 * @throws Exception
	 *             Any exception which may occur...
	 */
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		Class.forName("org.postgresql.Driver");

		// extract fulltext, entities and metdadata and write to DB
		InformationExtraction2Postgres.main(args);
		long timeUIMAPipeline = System.currentTimeMillis() - startTime;


		long startEmbedding = System.currentTimeMillis();
		DocEmbeddingManager.main(args);
		long timeEmbeddingManager = System.currentTimeMillis()-startEmbedding;


		// read from DB and write to fullext index
		long startPost2Elastic = System.currentTimeMillis();
		Postgres2ElasticsearchIndexer.main(args);
		long timePost2Elastic = System.currentTimeMillis()-startPost2Elastic;

		long estimatedTime = System.currentTimeMillis() - startTime;

		System.out.println("Processing time passed (seconds): " + estimatedTime / 1000);
		System.out.println("Time spent for UIMA pipeline (seconds): " + timeUIMAPipeline / 1000);
		System.out.println("Time spent for document embedding (seconds): " + timeEmbeddingManager / 1000);
		System.out.println("Time spent for Postgres to Elasticsearch index (seconds): " + timePost2Elastic / 1000);
	}



}
