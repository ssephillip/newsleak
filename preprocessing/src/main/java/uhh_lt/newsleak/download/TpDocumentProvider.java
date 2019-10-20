package uhh_lt.newsleak.download;

import uhh_lt.newsleak.types.TpDocument;

import java.util.Iterator;
import java.util.List;

public class TpDocumentProvider{
    Iterator<TpDocument> tpDocumentIterator;
    int current;
    int threadCounter;
    static TpDocumentProvider tpDocumentProvider;

    private TpDocumentProvider(List<TpDocument> tpDocuments){
        tpDocumentIterator = tpDocuments.iterator();
        current = 0;
    }

    public static TpDocumentProvider getInstance(List<TpDocument> tpDocuments){
        if(tpDocumentProvider == null){
            tpDocumentProvider = new TpDocumentProvider(tpDocuments);
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
}