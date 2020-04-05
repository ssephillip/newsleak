package uhh_lt.newsleak.preprocessing;

import uhh_lt.newsleak.util.StatsService;

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

		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.TOTAL);

		// extract fulltext, entities and metdadata and write to DB
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.UIMA_PIPELINE);
		InformationExtraction2Postgres.main(args);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.UIMA_PIPELINE);

		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.EMBEDDING_MANAGER);
		DocEmbeddingManager.main(args);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.EMBEDDING_MANAGER);



		// read from DB and write to fullext index
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.POSTGRES_TO_ELASTIC);
		Postgres2ElasticsearchIndexer.main(args);
		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.POSTGRES_TO_ELASTIC);


		StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.TOTAL);
		StatsService.getInstance().writeStatsForStartedRun();
	}



}
