import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args){
        //command line parameters:
        //1. solr address
        //2. path to the location where the documents shall be stored
        //3. path to the location where the stats shall be written to
        //4. file formats that shall be downloaded
        //5. number of documents to download
        //...TODO start from und until entfernen, da es nicht richtig funktioniert
        //6. number of threads
        //example parameters: "http://localhost:8983/solr/simfin" "/home/phillip/BA/data/transparenz3/" "/home/phillip/BA/data/statsT3.txt" "pdf,csv,html" 150000 10000 11000 3




        String solrAddress = args[0];
        String path = args[1];
        String pathToStats = args[2];
        List<String> formatsToDownload = Arrays.asList(args[3].split(","));
        int numOfDocs = Integer.valueOf(args[4]);
        int numOfThreads =Integer.valueOf(args[5]);


        TransparenzResourceDownloader transparenzResourceDownloader = new TransparenzResourceDownloader(solrAddress);
        Instant start = Instant.now();
        try {
            transparenzResourceDownloader.download(path, pathToStats, formatsToDownload, numOfDocs, numOfThreads);
        }catch(InstantiationException e){
            System.out.println("Couldn't retrieve IDs!");
            e.printStackTrace();
        }
        Instant end = Instant.now();
        System.out.println("Downloaded all documents in "+ Duration.between(start,end).getSeconds()+" seconds");
    }





}