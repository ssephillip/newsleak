import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args){
        //example parameters: "http://localhost:8983/solr/simfin" "/home/phillip/BA/data/transparenz3/" "/home/phillip/BA/data/statsT3.txt" "pdf,csv,html" 150000 10000 11000 3
        String solrAddress = args[0];
        String path = args[1];
        String pathToStats = args[2];
        List<String> formatsToDownload = Arrays.asList(args[3].split(","));
        int numOfDocs = Integer.valueOf(args[4]);
        int startFrom = Integer.valueOf(args[5]);
        int downloadUntil = Integer.valueOf(args[6]);
        int numOfThreads =Integer.valueOf(args[7]);


        TransparenzResourceDownloader transparenzResourceDownloader = new TransparenzResourceDownloader(solrAddress);
        Instant start = Instant.now();
        try {
            transparenzResourceDownloader.download(path, pathToStats, formatsToDownload, numOfDocs, startFrom, downloadUntil, numOfThreads);
        }catch(InstantiationException e){
            System.out.println("Couldn't retrieve IDs!");
            e.printStackTrace();
        }
        Instant end = Instant.now();
        System.out.println("Downloaded all documents in "+ Duration.between(start,end).getSeconds()+" seconds");
    }



}