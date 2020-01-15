package uhh_lt.newsleak.util;

import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class StatsService {
    private static StatsService statsService;
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
    public static final String EMBEDDING = "Doc embedding";
    public static final String EMBEDDING_INDEXING = "Doc embedding indexing";
    public static final String INITIALIZE_TRANSPARENZ = "Transparenz(reader) initialization";
    public static final String INITIALIZE_HOOVER = "Hoover(reader) initialization";
    public static final String EVENT_TYPE_START = "start";
    public static final String EVENT_TYPE_STOP = "stop";
    String statsFilePath;
    int numOfDocsProcessed;
    int numOfThreads;
    Map<String, Instant> startTimes;
    Map<String, Instant> endTimes;
    private boolean isRunStarted;
    private boolean isSetNumOfDocsProcessed;


    private StatsService(){
        super();
        startTimes = new LinkedHashMap<>();
        endTimes = new HashMap<>();
        isRunStarted = false;
        isSetNumOfDocsProcessed = false;
    }

    public static StatsService getInstance(){
        if(statsService == null){
            statsService = new StatsService();
        }

        return statsService;
    }

    public void startNewRun(String filePath, int numOfThreads) {
        statsFilePath = filePath+"/processing-stats.txt";
        this.numOfThreads = numOfThreads;

        try {
            FileWriter fileWriter = new FileWriter(statsFilePath, true);
            fileWriter.write("-------------------------------------------------- \n");
            fileWriter.write("-------------------------------------------------- \n");
            fileWriter.write("-------------------------------------------------- \n");
            fileWriter.write("-------------------------------------------------- \n");
            fileWriter.write("Run: "+ Instant.now() +"\n");
            fileWriter.write("Threads: "+numOfThreads +"\n");

            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        isRunStarted = true;

    }


    public void writeStatsForStartedRun(){
        if(isRunStarted) {
            Map<String, Instant> startTimesCopy = new LinkedHashMap<>(startTimes);
            Set<Map.Entry<String, Instant>> startTimesEntrySet = startTimesCopy.entrySet();
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


    private void writeStatsForComponent(String componentName, Duration timeSpent) {

        try {
            FileWriter fileWriter = new FileWriter(statsFilePath, true);
            fileWriter.write("----------------- \n");
            fileWriter.write("Target: "+componentName +"\n");
            fileWriter.write("Number of docs processed: "+ numOfDocsProcessed +"\n");
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

    //TODO stats so printen das es eine tabelle ist
    public void writeFinalStatsTable(){
        if(isRunStarted) {
            Map<String, Instant> startTimesCopy = new LinkedHashMap<>(startTimes);
            Set<Map.Entry<String, Instant>> startTimesEntrySet = startTimesCopy.entrySet();
            for (Map.Entry startEntry : startTimesEntrySet) {
                String componentName = (String) startEntry.getKey();
                Instant endTime = endTimes.get(componentName);
                if (endTime != null) {
                    Duration duration = Duration.between((Instant) startEntry.getValue(), endTime);
                    writeStatsForComponent(componentName, duration);

                    startTimes.remove(componentName);
                    endTimes.remove(componentName);
                }
            }

        }else{
            System.out.println("Stats could not be written because no run was started!"); //TODO maybe better to throw exception or log the error. Problem: logger is not available in the calling method.
        }
    }



//    public static void writeFinalStats(String filePath, int numOfDocsToProcess, int numOfThreads, Duration totalDuration, Duration uimaDuration, Duration embeddingDuration, Duration elasticIndexingDuration) {
//        double numOfDocsDouble = numOfDocsToProcess;
//
//        try {
//            FileWriter fileWriter = new FileWriter(filePath, true);
//            fileWriter.write("-------------------------------------------------- \n");
//            fileWriter.write("Final statistics (in seconds):");
//            fileWriter.write("Docs to process: "+numOfDocsToProcess +"\n");
//            fileWriter.write("Threads: "+numOfThreads +"\n");
//            fileWriter.write("Time spent total: "+ totalDuration.getSeconds() +"\n");
//            fileWriter.write("Time spent total (per doc): "+ totalDuration.getSeconds()/numOfDocsDouble +"\n");
//            fileWriter.write("Time spent for UIMA pipeline: "+ totalDuration +"\n");
//            fileWriter.write("Time spent for UIMA pipeline (per doc): "+ totalDuration +"\n");
//            fileWriter.write("Time spent document embedding: "+ totalDuration +"\n");
//            fileWriter.write("Time spent document embedding (per doc): "+ totalDuration +"\n"); //TODO vector indexing auch mit messen
//            fileWriter.write("Time spent for Postgres to Elastic: "+ totalDuration +"\n");
//            fileWriter.write("Time spent for Postgres to Elastic (per doc): "+ totalDuration +"\n");
//
//
//            fileWriter.flush();
//            fileWriter.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }

    public void addStatsEvent(String eventType, String serviceName, Instant instant){
        if(eventType.equals(EVENT_TYPE_START)){
            startTimes.putIfAbsent(serviceName, instant);
        }else if (eventType.equals(EVENT_TYPE_STOP)){
            endTimes.put(serviceName, instant);
        }

    }


    public void setNumOfDocsProcessed(int numOfDocsProcessed) {
        this.numOfDocsProcessed = numOfDocsProcessed;
        isSetNumOfDocsProcessed = true;
    }

    public boolean isSetNumOfDocsProcessed(){
        return isSetNumOfDocsProcessed;
    }
}