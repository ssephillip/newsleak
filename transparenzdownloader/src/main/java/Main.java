import java.time.Duration;
import java.time.Instant;

public class Main {

    public static void main(String[] args){
        //example parameters: "http://localhost:8983/solr/simfin" "/home/phillip/BA/data/transparenz3/" 150000 10000 11000 3
        String solrAddress = args[0];
        String path = args[1];
        int numOfDocs = Integer.valueOf(args[2]);
        int startFrom = Integer.valueOf(args[3]);
        int downloadUntil = Integer.valueOf(args[4]);
        int numOfThreads =Integer.valueOf(args[5]);


        TransparenzResourceDownloader transparenzResourceDownloader = new TransparenzResourceDownloader(solrAddress);
        Instant start = Instant.now();
        try {
            transparenzResourceDownloader.download(path, numOfDocs, startFrom, downloadUntil, numOfThreads);
        }catch(InstantiationException e){
            System.out.println("Couldn't retrieve IDs!");
            e.printStackTrace();
        }
        Instant end = Instant.now();
        System.out.println("Downloaded all documents in "+ Duration.between(start,end).getSeconds()+" seconds");
    }



}