package uhh_lt.newsleak.preprocessing;

import uhh_lt.newsleak.util.StatsService;

import java.time.Instant;

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
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.TOTAL, Instant.now());
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.UIMA_PIPELINE, Instant.now());
		InformationExtraction2Postgres.main(args);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.UIMA_PIPELINE, Instant.now());
		long timeUIMAPipeline = System.currentTimeMillis() - startTime;

		long startEmbedding = System.currentTimeMillis();
		DocEmbeddingManager.main(args);
		long timeEmbeddingManager = System.currentTimeMillis()-startEmbedding;


		// read from DB and write to fullext index
		long startPost2Elastic = System.currentTimeMillis();
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.POSTGRES_TO_ELASTIC, Instant.now());
		Postgres2ElasticsearchIndexer.main(args);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.POSTGRES_TO_ELASTIC, Instant.now());
		long timePost2Elastic = System.currentTimeMillis()-startPost2Elastic;

		long estimatedTime = System.currentTimeMillis() - startTime;
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.TOTAL, Instant.now());
		StatsService.getInstance().writeStatsForStartedRun();

		System.out.println("Processing time passed (seconds): " + estimatedTime / 1000);
		System.out.println("Time spent for UIMA pipeline (seconds): " + timeUIMAPipeline / 1000);
		System.out.println("Time spent for document embedding (seconds): " + timeEmbeddingManager / 1000);
		System.out.println("Time spent for Postgres to Elasticsearch index (seconds): " + timePost2Elastic / 1000);
	}



}
