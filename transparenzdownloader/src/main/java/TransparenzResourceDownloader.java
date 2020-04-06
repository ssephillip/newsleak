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

    /** The address (URL) to the Transparenzportal Solr Index */
    String solrCoreAddress;

    /** The TransparenzportalSolrService */
    TransparenzSolrService transparenzSolrService;



    public TransparenzResourceDownloader(String solrCoreAddress){
        super();
        this.solrCoreAddress = solrCoreAddress;

        solrClient = new HttpSolrClient.Builder(solrCoreAddress).build();
    }

    /**
     * Starts the downloading process
     * @param path the path to the folder where the files will be stored
     * @param pathToStats the path to the file to which the stats will be written
     * @param formatsToDownload files of the specified formats will be downloaded
     * @param numOfFilesToDownload the number of files that shall be downloaded
     * @param numOfThreads the number of threads used to download the files
     * @throws InstantiationException if the download process could not be started because the resources could not be retrieved from the Transparenzportal Solr Index
     */
    public void download(String path, String pathToStats, List<String> formatsToDownload, int numOfFilesToDownload, int numOfThreads) throws InstantiationException{
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
        startDownloadingThreads(tpResources, path, numOfFilesToDownload, numOfThreads);

        //writes the statistics to the file specified in the command line arguments
        writeStatsWhenFinished(startTime, pathToStats, formatsToDownload, numOfThreads);
    }


    /**
     * Starts the threads that download the documents.
     * The number of threads is specified by the corresponding command line parameter (see {@link Main}).
     *
     * @param tpResources the list with the {@link TpResource} retrieved from the Transparenzportal Solr Index
     * @param path the path to the folder where the files will be stored
     * @param numOfDocsToDownload the number of files that shall be downloaded
     * @param numOfThreads the number of threads used to download the files
     */
    private void startDownloadingThreads(List<TpResource> tpResources, String path, int numOfDocsToDownload, int numOfThreads){
        TpResourceProvider.initializeResourceProvider(tpResources, numOfDocsToDownload);
        final TpResourceProvider tpResourceProvider = TpResourceProvider.getInstance();
        final int numOfRelevantResources = transparenzSolrService.getNumOfRelevantResources();

        //starts the threads
        for(int i= 0; i < numOfThreads; i++){
            Thread thread = new Thread(new Runnable() {

                @Override
                public void run() {
                    downloadFiles(tpResourceProvider, path, numOfDocsToDownload, numOfRelevantResources);
                }
            });
            thread.start();
        }
    }

    /**
     * Each thread calls this method ones.
     * Because of the endless loop this method runs until there are no more files to download or if the number of files to download has been reached.
     *
     * @param tpResourceProvider The {@link TpResourceProvider}
     * @param path the path to the folder where the files will be stored
     * @param numOfDocsToDownload the number of files that shall be downloaded
     * @param numOfRelevantResources the number of resources where the resource format is a format that shall be downloaded
     */
    private void downloadFiles(TpResourceProvider tpResourceProvider, String path, int numOfDocsToDownload, int numOfRelevantResources) {
        int threadNumber = tpResourceProvider.getCurrentThreadCount();
        System.out.println("Thread '"+threadNumber+"' is starting to download files.");

        while(true) {
            TpResource tpResource = tpResourceProvider.getNextTpResource();
            int current = tpResourceProvider.getCurrent();

            if(tpResource != null && current < numOfDocsToDownload) {
                    System.out.println("Thread '"+threadNumber+"'Downloading file '"+(current+1)+"' of '"+numOfRelevantResources+"' with the absolute resource ID: '"+tpResource.getAbsoluteResourceId());
                    downloadFile(tpResource, tpResourceProvider, path);
            }else{
                System.out.println("Thread '"+threadNumber+"' finished");
                break;
            }
        }

    }

    /**
     * Downloads the actual file that corresponds to the provided {@link TpResource}.
     *
     * @param tpResource the resource that holds the information of the file that shall be downloaded
     * @param tpResourceProvider the {@link TpResourceProvider}
     * @param path the path to the folder where the file will be stored
     */
    private void downloadFile(TpResource tpResource, TpResourceProvider tpResourceProvider, String path){
            String urlString = tpResource.getUrl();
            String docFormat = tpResource.getFormat().toLowerCase();
            String docId = tpResource.getAbsoluteResourceId();

            String pathToFile = path + docId + "." + docFormat;

            try {
                //opens a connection
                URL url = new URL(urlString);
                URLConnection urlConnection = url.openConnection();
                urlConnection.setReadTimeout(1000000);
                urlConnection.connect();

                //reads the file
                InputStream in = new BufferedInputStream(urlConnection.getInputStream());
                OutputStream out = new BufferedOutputStream(new FileOutputStream(pathToFile));

                //writes the file
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

    /**
     * Coordinates if/when the stats should be written to the stats file.
     *
     * TODO wenn ich entschieden habe ob temp stats raus kommen oder nicht unter noch ein bisschen kommentieren (und evtl. hier auch noch was schreiben).
     *
     * @param startTime the time at which the downloading process started
     * @param pathToStats the path to the file to which the stats will be written
     * @param formatsToDownload files of the specified formats will be downloaded
     * @param numOfThreads the number of threads used to download the files
     */
    private void writeStatsWhenFinished(Instant startTime, String pathToStats, List<String> formatsToDownload, int numOfThreads) {  //TODO wenn temp stats hier bleiben dann umbenennen
        String pathToTempStats = pathToStats+"--temp.txt";

        while(true){
            TpResourceProvider tpResourceProvider = TpResourceProvider.getInstance();
            if(tpResourceProvider != null && tpResourceProvider.isFinished()){
                Instant endTime = Instant.now();
                long secondsElapsed = Duration.between(startTime, endTime).toMillis()/1000;
                writeDownloadStatsToFile(secondsElapsed, tpResourceProvider.getNumOfFilesDownloaded(), tpResourceProvider.getNumOfFilesFailedToDownload(), formatsToDownload, pathToStats, numOfThreads);
                break;
            }else if(tpResourceProvider != null &&  tpResourceProvider.getNumOfDownloadsSinceLastTempStats() >= 1000){
                Instant endTime = Instant.now();
                long secondsElapsed = Duration.between(startTime, endTime).toMillis()/1000;
                writeDownloadStatsToFile(secondsElapsed, tpResourceProvider.getNumOfFilesDownloaded(), tpResourceProvider.getNumOfFilesFailedToDownload(), formatsToDownload, pathToTempStats, numOfThreads);
                tpResourceProvider.setNumOfDownloadsSinceLastTempStats(0);
            }

            try {
                Thread.sleep(1000);
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * Writes the different stats that were collected throughout the downloading process to the specified file
     * .
     * @param secondsElapsed Time spent for downloading the files 8sin seconds)
     * @param numOfFilesDownloaded The number of files that have been downloaded successfully
     * @param numOfFilesFailedToDownload The number of files that could not be downloaded
     * @param downloadedFormats The file formats that were should have been downloaded (includes file formats for which no file was found)
     * @param filePath The path to the file to which the stats will be written
     * @param numOfThreads The number of threads that were used for downloading the documents.
     */
    private void writeDownloadStatsToFile(long secondsElapsed, int numOfFilesDownloaded, int numOfFilesFailedToDownload, List<String> downloadedFormats, String filePath, int numOfThreads){
        int numOfDatasets = transparenzSolrService.getTotalNumOfDatasets();
        int totalNumOfResources = transparenzSolrService.getTotalNumOfResources();
        int filteredNumOfResources = transparenzSolrService.getNumOfRelevantResources();
        int illformedDatasetsCounter = transparenzSolrService.getIllformedDatasetsCounter();

        try {
            FileWriter fileWriter = new FileWriter(filePath, true);
            fileWriter.write("--------------------------------------------------\n");
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
            fileWriter.write("--------------------------------------------------\n");


            fileWriter.flush();
            fileWriter.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

}