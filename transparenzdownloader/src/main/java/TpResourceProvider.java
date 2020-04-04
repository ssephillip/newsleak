import java.util.Iterator;
import java.util.List;

//TODO javadoc comment
public class TpResourceProvider {
    Iterator<TpResource> tpResourceIterator;
    int current;
    int threadCounter;
    int numOfInnerDocsDownloaded;
    int numOfInnerDocsFailed;
    int numOfDocsTotal;
    int numOfDocsToDownload;


    static TpResourceProvider tpResourceProvider;

    private TpResourceProvider(List<TpResource> tpResources, int numofDocsToDownload){
        tpResourceIterator = tpResources.iterator();
        numOfDocsTotal = tpResources.size();
        this.numOfDocsToDownload = numofDocsToDownload;
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

    public synchronized TpResource getNextTpDocument(){
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
        return (numOfInnerDocsDownloaded + numOfInnerDocsFailed == numOfDocsTotal) || (numOfInnerDocsDownloaded + numOfInnerDocsFailed == numOfDocsToDownload);
    }

    public synchronized void incrementNumOfFilesDownloaded(){
        numOfInnerDocsDownloaded++;
    }

    public synchronized void incrementNumOfFilesFailedToDownload(){
        numOfInnerDocsFailed++;
    }

    public int getNumOfInnerDocsDownloaded(){
        return numOfInnerDocsDownloaded;
    }

    public int getNumOfInnerDocsFailed(){
        return numOfInnerDocsFailed;
    }
}