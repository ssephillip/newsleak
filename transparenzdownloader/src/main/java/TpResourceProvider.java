import java.util.Iterator;
import java.util.List;

//TODO javadoc comment
public class TpResourceProvider {
    Iterator<TpResource> tpResourceIterator;
    int current;
    int threadCounter;
    int numOfFilesDownloaded;
    int numOfFilesFailedToDownload;
    int totalNumOfResources;
    int numOfFilesToDownload;


    static TpResourceProvider tpResourceProvider;

    private TpResourceProvider(List<TpResource> tpResources, int numOfFilesToDownload){
        tpResourceIterator = tpResources.iterator();
        totalNumOfResources = tpResources.size();
        this.numOfFilesToDownload = numOfFilesToDownload;
        current = -1;
    }

    public static TpResourceProvider getInstance(){
        if(tpResourceProvider == null){
            return null;
        }

        return tpResourceProvider;
    }

    public static TpResourceProvider getInstance(List<TpResource> tpResources, int numofDocsToDownload){
        if(tpResourceProvider == null){
            tpResourceProvider = new TpResourceProvider(tpResources, numofDocsToDownload);
        }
        return tpResourceProvider;
    }

    public synchronized TpResource getNextTpResource(){
        TpResource tpResource = null;
        if(tpResourceIterator.hasNext()){
            current++;
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

    public synchronized boolean isFinished(){
        return (numOfFilesDownloaded + numOfFilesFailedToDownload == totalNumOfResources) || (numOfFilesDownloaded + numOfFilesFailedToDownload == numOfFilesToDownload);
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
}