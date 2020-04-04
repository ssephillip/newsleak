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



    public void download(String path, String pathToStats, List<String> formatsToDownload, int numOfDocs, int numOfThreads) throws InstantiationException{
        Instant startTime = Instant.now();

        List<TpResource> tpResources = new ArrayList<>();
        transparenzSolrService = new TransparenzSolrService(solrClient, solrCoreAddress);

        //gets all resources stored in the Transparenzportal
        tpResources = transparenzSolrService.getAllResourcesFromSolr();

        //removes all resources that have a file format that is not supposed to be downloaded
        tpResources.removeIf(tp -> !formatsToDownload.contains(tp.getFormat()));

        transparenzSolrService.setNumOfRelevantResources(tpResources.size());
        System.out.println("Time for getting and extracting documents: "+Duration.between(startTime, Instant.now()).getSeconds() + " sec"); //TODO evtl. weg da sehr schnell

        //downloads the actual files corresponding to the resources retrieved from the Transparenzportal Solr Index
        downloadAllDocuments(tpResources, path, numOfDocs, numOfThreads);

        //writes the statistics to the file specified in the command line arguments
        writeStatsWhenFinished(startTime, pathToStats, formatsToDownload, numOfThreads);
    }













    private void downloadAllDocuments(List<TpResource> tpResources, String path, int numOfDocsToDownload, int numOfThreads){
        final TpDocumentProvider tpDocumentProvider = TpDocumentProvider.getInstance(tpResources, numOfDocsToDownload);
        final int totalNumOfInnerDocs = transparenzSolrService.getNumOfRelevantResources();

        for(int i= 0; i < numOfThreads; i++){
            Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    downloadDocuments(tpDocumentProvider, path, numOfDocsToDownload, totalNumOfInnerDocs);
                }
            });
            thread.start();
        }
    }


    private void downloadDocuments(TpDocumentProvider tpDocumentProvider, String path, int numOfDocsToDownload, int totalNumOfInnerDocs) {
        System.out.println("Starting to download files.");
        int threadNumber = tpDocumentProvider.getCurrentThreadCount();

        while(true) {
            TpResource tpResource = tpDocumentProvider.getNextTpDocument();
            int current = tpDocumentProvider.getCurrent();

            if(tpResource != null && current < numOfDocsToDownload) {
                    System.out.println("Thread '"+threadNumber+"'Downloading document '"+(current+1)+"' of '"+totalNumOfInnerDocs+"' with the ID: '"+tpResource.getAbsoluteResourceId());
                    downloadDocument(tpResource, tpDocumentProvider, path);
            }else{
                System.out.println("Thread '"+threadNumber+"' finished");
                break;
            }
        }

    }



    private void downloadDocument(TpResource tpResource,  TpDocumentProvider tpDocumentProvider, String path){
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
                tpDocumentProvider.incrementNumOfDocsDownloaded();
            }catch(Exception e){
                System.out.println("Couldn't download dcoument.");
                e.printStackTrace();
                tpDocumentProvider.incrementNumOfDocsFailed();
            }

    }













    private void writeStatsWhenFinished(Instant startTime, String pathToStats, List<String> formatsToDownload, int numOfThreads) {  //TODO wenn temp stats hier bleiben dann umbenennen
        String pathToTempStats = pathToStats+"--temp.txt";

        while(true){
            TpDocumentProvider tpDocumentProvider = TpDocumentProvider.getInstance();
            if(tpDocumentProvider != null && tpDocumentProvider.isFinished()){
                Instant endTime = Instant.now();
                long secondsElapsed = Duration.between(startTime, endTime).toMillis()/1000;
                writeDownloadStatsToFile(secondsElapsed, tpDocumentProvider.getNumOfInnerDocsDownloaded(), tpDocumentProvider.getNumOfInnerDocsFailed(), formatsToDownload, pathToStats, numOfThreads);
                break;
            }else if(tpDocumentProvider != null &&  tpDocumentProvider.getCurrent()%500==0){ //TODO determine time for temp-stat print better (e.g. with a callback)
                //TODO zwischen stats ausbauen wenn fertig, da zwischen stats funktion zu unstabil.
                Instant endTime = Instant.now();
                long secondsElapsed = Duration.between(startTime, endTime).toMillis()/1000;
                writeDownloadStatsToFile(secondsElapsed, tpDocumentProvider.getNumOfInnerDocsDownloaded(), tpDocumentProvider.getNumOfInnerDocsFailed(), formatsToDownload, pathToTempStats, numOfThreads);

            }

            try {
                Thread.sleep(100);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }


    private void writeDownloadStatsToFile(long secondsElapsed, int numOfDocsDownloaded, int numOfDocsFailedToDownload, List<String> downloadedFormats, String filePath, int numOfThreads){
        int numOfOuterDocs = transparenzSolrService.getTotalNumOfDatasets();
        int totalNumOfInnerDocs = transparenzSolrService.getTotalNumOfResources();
        int filteredNumOfInnerDocs = transparenzSolrService.getNumOfRelevantResources(); //TODO umbenennen zu numOfRelevantInnerDocs
        int malformedSolrDocCounter = transparenzSolrService.getIllformedDatasetsCounter();

        try {
            FileWriter fileWriter = new FileWriter(filePath, true);
            fileWriter.write("\n--------------------------------------------------");
            fileWriter.write("--------------------------------------------------\n");
            fileWriter.write("Run: "+Instant.now().toString()+"\n");
            fileWriter.write("Number of threads: "+numOfThreads+"\n");
            fileWriter.write("Total number of outer documents: "+numOfOuterDocs+"\n");
            fileWriter.write("Number of malformed outer documents: "+malformedSolrDocCounter+"\n");
            fileWriter.write("Total number of inner documents: "+totalNumOfInnerDocs+"\n");
            fileWriter.write("Number of inner documents of desired formats: "+filteredNumOfInnerDocs+"\n");
            fileWriter.write("Number of inner documents downloaded: "+numOfDocsDownloaded+"\n");
            fileWriter.write("Number of inner documents failed to download: "+numOfDocsFailedToDownload+"\n");
            fileWriter.write("Time elapsed: "+secondsElapsed+"\n");
            fileWriter.write("Documents per second: "+((double) numOfDocsDownloaded)/secondsElapsed+"\n");
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