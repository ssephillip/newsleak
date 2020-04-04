import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class TransparenzResourceDownloader {


    /** The solr client */
    HttpSolrClient solrClient;

    String solrCoreAddress;

    TransparenzSolrService transparenzSolrService;





    public TransparenzResourceDownloader(String solrCoreAddress){
        super();
        this.solrCoreAddress = solrCoreAddress;

        solrClient = new HttpSolrClient.Builder(solrCoreAddress).build();
    }



    public void download(String path, String pathToStats, List<String> formatsToDownload, int numOfDocsToDownload, int numOfThreads) throws InstantiationException{
        Instant startTime = Instant.now();

        List<TpResource> tpResources = new ArrayList<>();
        transparenzSolrService = new TransparenzSolrService(solrClient, solrCoreAddress);

        //gets all resources stored in the Transparenzportal
        tpResources = transparenzSolrService.getAllResourcesFromSolr();

        //removes all resources that have a file format that is not supposed to be downloaded
        tpResources.removeIf(tp -> !formatsToDownload.contains(tp.getFormat()));

        transparenzSolrService.setNumOfRelevantResources(tpResources.size());
        System.out.println("Time for getting TpResources from Transparenzportal Solr Index: "+Duration.between(startTime, Instant.now()).getSeconds() + " sec"); //TODO evtl. weg da sehr schnell

        //downloads the actual files corresponding to the resources retrieved from the Transparenzportal Solr Index
        downloadAllFiles(tpResources, path, numOfDocsToDownload, numOfThreads);

        //writes the statistics to the file specified in the command line arguments
        writeStatsWhenFinished(startTime, pathToStats, formatsToDownload, numOfThreads);
    }













    private void downloadAllFiles(List<TpResource> tpResources, String path, int numOfDocsToDownload, int numOfThreads){ //TODO umbenennen zu startDownloadThreads
        final TpResourceProvider tpResourceProvider = TpResourceProvider.getInstance(tpResources, numOfDocsToDownload);
        final int totalNumOfResources = transparenzSolrService.getNumOfRelevantResources();

        for(int i= 0; i < numOfThreads; i++){
            Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    downloadFiles(tpResourceProvider, path, numOfDocsToDownload, totalNumOfResources);
                }
            });
            thread.start();
        }
    }


    private void downloadFiles(TpResourceProvider tpResourceProvider, String path, int numOfDocsToDownload, int totalNumOfResources) {
        int threadNumber = tpResourceProvider.getCurrentThreadCount();
        System.out.println("Thread '"+threadNumber+"' is starting to download files.");

        while(true) {
            TpResource tpResource = tpResourceProvider.getNextTpResource();
            int current = tpResourceProvider.getCurrent();

            if(tpResource != null && current < numOfDocsToDownload) {
                    System.out.println("Thread '"+threadNumber+"'Downloading file '"+(current+1)+"' of '"+totalNumOfResources+"' with the absolute resource ID: '"+tpResource.getAbsoluteResourceId());
                    downloadFile(tpResource, tpResourceProvider, path);
            }else{
                System.out.println("Thread '"+threadNumber+"' finished");
                break;
            }
        }

    }



    private void downloadFile(TpResource tpResource, TpResourceProvider tpResourceProvider, String path){
            String urlString = tpResource.getUrl();
            String docFormat = tpResource.getFormat().toLowerCase();
            String docId = tpResource.getAbsoluteResourceId();
            String docName = tpResource.getName();
            String pathToFile = path+docId+"."+docFormat;

            try {
                URL url = new URL(urlString);
                URLConnection urlConnection = url.openConnection();
                urlConnection.setReadTimeout(300000);//1000000);
                urlConnection.connect();
                InputStream in = new BufferedInputStream(urlConnection.getInputStream());
                OutputStream out = new BufferedOutputStream(new FileOutputStream(pathToFile));

                for (int i; (i = in.read()) != -1; ) {
                    out.write(i);
                }
                in.close();
                out.close();
                tpResourceProvider.incrementNumOfFilesDownloaded();
            }catch(Exception e){
                System.out.println("Couldn't download file.");
                e.printStackTrace();
                tpResourceProvider.incrementNumOfFilesFailedToDownload();
            }

    }













    private void writeStatsWhenFinished(Instant startTime, String pathToStats, List<String> formatsToDownload, int numOfThreads) {  //TODO wenn temp stats hier bleiben dann umbenennen
        String pathToTempStats = pathToStats+"--temp.txt";

        while(true){
            TpResourceProvider tpResourceProvider = TpResourceProvider.getInstance();
            if(tpResourceProvider != null && tpResourceProvider.isFinished()){
                Instant endTime = Instant.now();
                long secondsElapsed = Duration.between(startTime, endTime).toMillis()/1000;
                writeDownloadStatsToFile(secondsElapsed, tpResourceProvider.getNumOfFilesDownloaded(), tpResourceProvider.getNumOfFilesFailedToDownload(), formatsToDownload, pathToStats, numOfThreads);
                break;
            }else if(tpResourceProvider != null &&  tpResourceProvider.getCurrent()%500==0){ //TODO determine time for temp-stat print better (e.g. with a callback)
                //TODO zwischen stats ausbauen wenn fertig, da zwischen stats funktion zu unstabil.
                Instant endTime = Instant.now();
                long secondsElapsed = Duration.between(startTime, endTime).toMillis()/1000;
                writeDownloadStatsToFile(secondsElapsed, tpResourceProvider.getNumOfFilesDownloaded(), tpResourceProvider.getNumOfFilesFailedToDownload(), formatsToDownload, pathToTempStats, numOfThreads);

            }

            try {
                Thread.sleep(100);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }


    private void writeDownloadStatsToFile(long secondsElapsed, int numOfFilesDownloaded, int numOfFilesFailedToDownload, List<String> downloadedFormats, String filePath, int numOfThreads){
        int numOfDatasets = transparenzSolrService.getTotalNumOfDatasets();
        int totalNumOfResources = transparenzSolrService.getTotalNumOfResources();
        int filteredNumOfResources = transparenzSolrService.getNumOfRelevantResources(); //TODO umbenennen zu numOfRelevantInnerDocs
        int illformedDatasetsCounter = transparenzSolrService.getIllformedDatasetsCounter();

        try {
            FileWriter fileWriter = new FileWriter(filePath, true);
            fileWriter.write("\n--------------------------------------------------");
            fileWriter.write("--------------------------------------------------\n");
            fileWriter.write("Run: "+Instant.now().toString()+"\n");
            fileWriter.write("Number of threads: "+numOfThreads+"\n");
            fileWriter.write("Total number of datasets: "+numOfDatasets+"\n");
            fileWriter.write("Number of illformed datasets: "+illformedDatasetsCounter+"\n");
            fileWriter.write("Total number of resources: "+totalNumOfResources+"\n");
            fileWriter.write("Number of resources of desired formats: "+filteredNumOfResources+"\n");
            fileWriter.write("Number of resources downloaded: "+numOfFilesDownloaded+"\n");
            fileWriter.write("Number of resources failed to download: "+numOfFilesFailedToDownload+"\n");
            fileWriter.write("Time elapsed: "+secondsElapsed+"\n");
            fileWriter.write("Resources per second (average): "+((double) numOfFilesDownloaded)/secondsElapsed+"\n");
            fileWriter.write("Formats downloaded: "+downloadedFormats.toString()+"\n");
            fileWriter.write("--------------------------------------------------\n");
            fileWriter.write("--------------------------------------------------");


            fileWriter.flush();
            fileWriter.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }


}