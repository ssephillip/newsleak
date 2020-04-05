package uhh_lt.newsleak.util;

import org.apache.uima.cas.AbstractCas;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Provides functionality to measure the time spent on different parts of the application (measurment targets).
 * Additionally it provides functionality to write these measurements (and other useful information) to a text file.
 *
 * The StatsService works as follows:
 * To begin a time measurement a start-event needs to be registered at the StatsService.
 * To stop a time measurement an end-event needs to be registered.
 *
 * To register an event the method {@link #addStatsEvent(String, String)} is called.
 * As parameters, the event type constant ({@link #EVENT_TYPE_START} or {@link #EVENT_TYPE_STOP}), the constant of that corresponds to the measurement target and the current {@link Instant} is passed.
 *
 * To add a new measurement target (i.e. measure a part of the software that is currently not measured), a new measurement target CONSTANT should be defined in this class.
 *
 *
 *
 * Additional information on how this Service works:
 * Before the time measurements can be written to a file, a new run needs to be started.
 * Measurement events can be registered <b>BEFORE</b> a run was started.
 * Starting a new run simply sets the values that are needed to write the stats to a file.
 * This information is not passed as parameter of the method that writes the stats to the file, because the required information might not be available when that method is called.
 *
 *
 * When a start-event is registered multiple times for the same measurement target, all but the first start-event are ignored.
 * When an end-event is registered multiple times only the last end-event that was registered is used to determine the time spent on the measurement target.
 * This is done to enable measurement of Analysis Components (e.g. {@link uhh_lt.newsleak.annotator.KeytermExtractor}, {@link uhh_lt.newsleak.writer.PostgresDbWriter}).
 *
 * The reason behind this is the following:
 * There is no method that provides (reliable) information if an Analysis Component processed all documents.
 * There are some methods that seem as they could be used for this purpose, but it has been shown that they do not work reliably.
 * However, when processing a document, an Analysis Component always calls the method "process" ({@link org.apache.uima.analysis_component.AnalysisComponent#process(AbstractCas)}).
 * Consequentially, when an Analysis Component processes the first document and the last document it calls the method "process".
 * Thus, we can register the start-event at the beginning of the method "process" and the end-event at the end of the method "process".
 * The problem with this is that the method "process" is called many times in between the first and the last time it is called.
 * This is why the start-event and end-event are handled as described above.
 * This works very efficiently, because the events are stored in hash maps.
 *
 */
public class StatsService {
    private static StatsService statsService;

    /** Measurement target CONSTANTS */
    public static final String TOTAL = "Total";
    public static final String UIMA_PIPELINE = "UIMA Pipeline";
    public static final String LANGUAGE_DETECTOR = "Language Detector";
    public static final String ELASTICSEARCH_WRITER = "Elasticsearch writer";
    public static final String SEGMENTER_ICU = "Segmenter ICU";
    public static final String SENTENCE_CLEANER = "Sentence Cleaner";
    public static final String HEIDELTIME = "Heideltime";
    public static final String NER = "NER";
    public static final String KEYTERMS = "Keyterms";
    public static final String DICTIONARY = "Dictionary extractor";
    public static final String DOC2VEC_WRITER = "Doc2Vec writer";
    public static final String POSTGRES_DB_WRITER = "PostgresDB writer";
    public static final String POSTGRES_TO_ELASTIC = "Postgres to Elastic indexing";
    public static final String EMBEDDING_MANAGER = "Doc embedding manager";
    public static final String EMBEDDING = "Doc embedding";
    public static final String EMBEDDING_INDEXING = "Doc embedding indexing";
    public static final String INITIALIZE_TRANSPARENZ = "Transparenz(reader) initialization";
    public static final String INITIALIZE_HOOVER = "Hoover(reader) initialization";

    /** Event type CONSTANTS */
    public static final String EVENT_TYPE_START = "start";
    public static final String EVENT_TYPE_STOP = "stop";

    /** Path to the file to which the stats will be written */
    String statsFilePath;

    /** Number of documents that were processed */
    int numOfDocsProcessed;

    /** NUmber of threads used */
    int numOfThreads;

    /** The Map containing the start times (i.e. start events). A linked map is used to preserve to order at which the start times were added. A hash map is used so to ensure efficiency.*/
    LinkedHashMap<String, Instant> startTimes;

    /** The Map containing the end times (i.e. end events). A hash map is used so to ensure efficiency. */
    HashMap<String, Instant> endTimes;

    /** True if a run has been started */
    private boolean isRunStarted;


    private StatsService(){
        super();
        startTimes = new LinkedHashMap<>();
        endTimes = new HashMap<>();
        isRunStarted = false;
    }

    public static StatsService getInstance(){
        if(statsService == null){
            statsService = new StatsService();
        }

        return statsService;
    }

    /**
     * Adds a stats event.
     *
     * For more information on stats events see the class description.
     *  @param eventType The event type CONSTANT
     * @param measurementTarget The measurement target CONSTANT
     */
    public void addStatsEvent(String eventType, String measurementTarget){
        Instant instant = Instant.now();

        if(eventType.equals(EVENT_TYPE_START)){
            startTimes.putIfAbsent(measurementTarget, instant);
        }else if (eventType.equals(EVENT_TYPE_STOP)){
            endTimes.put(measurementTarget, instant);
        }

    }

    /**
     * Starts a new run.
     * This method needs to be called before the stats can be written to a file.
     * Calling this method sets the path to the file to which the stats will be written and the number of threads used during processing.
     *
     * @param filePath the path to the file to which the stats will be written
     * @param numOfThreads the number of threads used during processing
     */
    public void startNewRun(String filePath, int numOfThreads) { //TODO zu setRunInformation oder so umbenennen
        statsFilePath = filePath+"/processing-stats.txt";
        this.numOfThreads = numOfThreads;

        isRunStarted = true;
    }

    /**
     * Writes the stats to the file that was specified when the run was started.
     *
     */
    public void writeStatsForStartedRun(){
        if(isRunStarted) {//TODO warsch. diese kpie wegmachen da nicht mehr nötig
            Set<Map.Entry<String, Instant>> startTimesEntrySet = startTimes.entrySet();

            //Writes general information to the stats file
            writeGeneralInformation();

            //writes the stats for each measurement target
            for (Map.Entry startEntry : startTimesEntrySet) {
                String componentName = (String) startEntry.getKey();
                Instant endTime = endTimes.get(componentName);

                if (endTime != null) {
                    Duration duration = Duration.between((Instant) startEntry.getValue(), endTime);
                    writeStatsForComponent(componentName, duration);
                }
            }

        }else{
            System.out.println("Stats could not be written because no run was started!"); //TODO maybe better to throw exception or log the error. Problem: logger is not available in the calling method.
        }
    }


    private void writeGeneralInformation(){
        try {
            FileWriter fileWriter = new FileWriter(statsFilePath, true);
            fileWriter.write("-------------------------------------------------- \n");
            fileWriter.write("-------------------------------------------------- \n");
            fileWriter.write("-------------------------------------------------- \n");
            fileWriter.write("-------------------------------------------------- \n");
            fileWriter.write("Run: "+ Instant.now() +"\n");
            fileWriter.write("Threads: "+numOfThreads +"\n");
            fileWriter.write("Number of docs processed: "+ numOfDocsProcessed +"\n");

            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Writes the stats of one measurement target to the file specified when the run was started.
     * @param measurementTargetName the name of the measurement target
     * @param timeSpent the time spent on the measurement target
     */
    private void writeStatsForComponent(String measurementTargetName, Duration timeSpent) {

        try {
            FileWriter fileWriter = new FileWriter(statsFilePath, true);
            fileWriter.write("----------------- \n");
            fileWriter.write("Target: "+measurementTargetName +"\n");
            fileWriter.write("Time spent (sec): "+timeSpent.getSeconds() +"\n");

            if(numOfDocsProcessed != 0) {
                double numOfDocsDouble = (double) numOfDocsProcessed;
                fileWriter.write("Time spent per doc (sec): " + timeSpent.getSeconds() / numOfDocsDouble + "\n");
            }

            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void setNumOfDocsProcessed(int numOfDocsProcessed) {
        this.numOfDocsProcessed = numOfDocsProcessed;
    }

}
