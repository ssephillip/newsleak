import java.util.Iterator;
import java.util.List;

public class TpDocumentProvider{
    Iterator<TpDocument> tpDocumentIterator;
    int current;
    int threadCounter;
    int numOfInnerDocsDownloaded;
    int numOfInnerDocsFailed;
    int numOfDocsTotal;
    int numOfDocsToDownload;


    static TpDocumentProvider tpDocumentProvider;

    private TpDocumentProvider(List<TpDocument> tpDocuments, int numofDocsToDownload){
        tpDocumentIterator = tpDocuments.iterator();
        numOfDocsTotal = tpDocuments.size();
        this.numOfDocsToDownload = numofDocsToDownload;
        current = -1;
    }

    public static TpDocumentProvider getInstance(){
        if(tpDocumentProvider == null){
            return null;
        }

        return tpDocumentProvider;
    }

    public static TpDocumentProvider getInstance(List<TpDocument> tpDocuments, int numofDocsToDownload){
        if(tpDocumentProvider == null){
            tpDocumentProvider = new TpDocumentProvider(tpDocuments, numofDocsToDownload);
        }
        return tpDocumentProvider;
    }

    public synchronized TpDocument getNextTpDocument(){
        TpDocument tpDocument = null;
        if(tpDocumentIterator.hasNext()){
            current++;
            tpDocument = tpDocumentIterator.next();
        }

        return tpDocument;
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