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
		Class.forName("org.postgresql.Driver");

		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.TOTAL, Instant.now());

		// extract fulltext, entities and metdadata and write to DB
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.UIMA_PIPELINE, Instant.now());
		InformationExtraction2Postgres.main(args);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.UIMA_PIPELINE, Instant.now());

		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.EMBEDDING_MANAGER, Instant.now());
		DocEmbeddingManager.main(args);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.EMBEDDING_MANAGER, Instant.now());



		// read from DB and write to fullext index
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.POSTGRES_TO_ELASTIC, Instant.now());
		Postgres2ElasticsearchIndexer.main(args);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.POSTGRES_TO_ELASTIC, Instant.now());


		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.TOTAL, Instant.now());
		StatsService.getInstance().writeStatsForStartedRun();
	}



}
