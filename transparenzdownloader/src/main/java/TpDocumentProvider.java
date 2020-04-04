import java.util.Iterator;
import java.util.List;

public class TpDocumentProvider{
    Iterator<TpResource> tpResourceIterator;
    int current;
    int threadCounter;
    int numOfInnerDocsDownloaded;
    int numOfInnerDocsFailed;
    int numOfDocsTotal;
    int numOfDocsToDownload;


    static TpDocumentProvider tpDocumentProvider;

    private TpDocumentProvider(List<TpResource> tpResources, int numofDocsToDownload){
        tpResourceIterator = tpResources.iterator();
        numOfDocsTotal = tpResources.size();
        this.numOfDocsToDownload = numofDocsToDownload;
        current = -1;
    }

    public static TpDocumentProvider getInstance(){
        if(tpDocumentProvider == null){
            return null;
        }

        return tpDocumentProvider;
    }

    public static TpDocumentProvider getInstance(List<TpResource> tpResources, int numofDocsToDownload){
        if(tpDocumentProvider == null){
            tpDocumentProvider = new TpDocumentProvider(tpResources, numofDocsToDownload);
        }
        return tpDocumentProvider;
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

    public synchronized void incrementNumOfDocsDownloaded(){
        numOfInnerDocsDownloaded++;
    }

    public synchronized void incrementNumOfDocsFailed(){
        numOfInnerDocsFailed++;
    }

    public int getNumOfInnerDocsDownloaded(){
        return numOfInnerDocsDownloaded;
    }

    public int getNumOfInnerDocsFailed(){
        return numOfInnerDocsFailed;
    }
}