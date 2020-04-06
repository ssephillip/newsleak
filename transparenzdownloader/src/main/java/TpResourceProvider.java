import java.util.Iterator;
import java.util.List;

/**
 * This class "coordinates" the threads that download the files.
 * This class is a singleton and provides the information that is needed by all threads.
 * It must be initialized before it can be used (by calling {@link #initializeResourceProvider(List, int)}).
 *
 * It holds the information that is needed to download the actual files and provides means to retrieve this information.
 * Where needed, the access to the information is synchronized to ensure the consistency of the application.
 * The most important information provided by this class are the {@link TpResource} objects.
 */
public class TpResourceProvider {
    /** The iterator containing the {@link TpResource}s */
    Iterator<TpResource> tpResourceIterator;

    /** The number of documents for which a download has started */
    int current;

    /** The number of threads that have started */
    int threadCounter;

    /** The number of files that were downloaded successfully */
    int numOfFilesDownloaded;

    /** The number of files that could not be downloaded */
    int numOfFilesFailedToDownload;

    /** The number of resources that are available to be retrieved. */
    int numOfRelevantResources;

    /** The number of files that shall be downloaded */
    int numOfFilesToDownload;

    /** The number of files that have been downloaded since the last temp stats was printed */
    int numOfDownloadsSinceLastTempStats;

    static TpResourceProvider tpResourceProvider;


    private TpResourceProvider(List<TpResource> tpResources, int numOfFilesToDownload){
        tpResourceIterator = tpResources.iterator();
        numOfRelevantResources = tpResources.size();
        this.numOfFilesToDownload = numOfFilesToDownload;
        current = -1;
    }

    /**
     * Initializes the {@link TpResourceProvider}.
     * Before the {@link TpResourceProvider} can be used, it must be initialized by this method.
     * This must be done to provide the necessary information to the {@link TpResourceProvider}.
     *
     * @param tpResources the list of TpResources that are used to download the actual files
     * @param numofDocsToDownload the number of files that shall be downloaded
     */
    public static void initializeResourceProvider(List<TpResource> tpResources, int numofDocsToDownload){
        if(tpResourceProvider == null){
            tpResourceProvider = new TpResourceProvider(tpResources, numofDocsToDownload);
        }
    }

    /**
     * Provides the {@link TpResourceProvider}.
     * This method only provides the {@link TpResourceProvider} if it was initialized (by calling {@link #initializeResourceProvider(List, int)}).
     *
     * @return the {@link TpResourceProvider} or null if {@link #initializeResourceProvider(List, int)} has not yet been called.
     */
    public static TpResourceProvider getInstance(){
        //This is done to make the behaviour of the method explicit and thereby the code more readable. We are aware that it is redundant.
        if(tpResourceProvider == null){
            return null;
        }

        return tpResourceProvider;
    }

    /**
     * Provides the next available {@link TpResource}.
     *
     * @return the next available {@link TpResource} or null of no more {@link TpResource}s are available
     */
    public synchronized TpResource getNextTpResource(){
        TpResource tpResource = null;
        if(tpResourceIterator.hasNext()){
            current++;
            numOfDownloadsSinceLastTempStats++;
            tpResource = tpResourceIterator.next();
        }

        return tpResource;
    }

    public synchronized int getCurrentThreadCount(){
        threadCounter++;
        return threadCounter;
    }

    public synchronized int getCurrent(){
        return current;
    }

    /**
     * Checks if the download process is finished.
     *
     * @return True, if the number of files to download is reached or if all available files have been downloaded.
     */
    public synchronized boolean isFinished(){
        return (numOfFilesDownloaded + numOfFilesFailedToDownload == numOfRelevantResources) || (numOfFilesDownloaded + numOfFilesFailedToDownload == numOfFilesToDownload);
    }

    public synchronized void incrementNumOfFilesDownloaded(){
        numOfFilesDownloaded++;
    }

    public synchronized void incrementNumOfFilesFailedToDownload(){
        numOfFilesFailedToDownload++;
    }

    public int getNumOfFilesDownloaded(){
        return numOfFilesDownloaded;
    }

    public int getNumOfFilesFailedToDownload(){
        return numOfFilesFailedToDownload;
    }



    public int getNumOfDownloadsSinceLastTempStats() {
        return numOfDownloadsSinceLastTempStats;
    }

    public void setNumOfDownloadsSinceLastTempStats(int numOfDownloadsSinceLastTempStats) {
        this.numOfDownloadsSinceLastTempStats = numOfDownloadsSinceLastTempStats;
    }
}