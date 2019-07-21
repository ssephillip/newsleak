package uhh_lt.newsleak.writer;

import opennlp.uima.Sentence;
import opennlp.uima.Token;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ExternalResource;
import org.apache.uima.fit.descriptor.OperationalProperties;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import uhh_lt.newsleak.resources.Doc2VecWriterResource;
import uhh_lt.newsleak.resources.TextLineWriterResource;
import uhh_lt.newsleak.types.Metadata;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

@OperationalProperties(multipleDeploymentAllowed = true, modifiesCas = false)
public class Doc2VecWriter extends JCasAnnotator_ImplBase {

    /** The Constant RESOURCE_LINEWRITER. */
    public static final String RESOURCE_DOC2VECWRITER = "doc2vecwriter";

    /** The linewriter. */
    @ExternalResource(key = RESOURCE_DOC2VECWRITER)
    private Doc2VecWriterResource doc2vecWriter;

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.uima.fit.component.JCasAnnotator_ImplBase#initialize(org.apache.
     * uima.UimaContext)
     */
    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        super.initialize(context);

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.uima.analysis_component.JCasAnnotator_ImplBase#process(org.apache.
     * uima.jcas.JCas)
     */
    @Override
    public void process(JCas jcas) throws AnalysisEngineProcessException {

        Metadata metadata = (Metadata) jcas.getAnnotationIndex(Metadata.type).iterator().next();
        String docId = metadata.getDocId();
        String docText = jcas.getDocumentText();

        //docText = docText.replaceAll("[\\s\\v]+", " ");
        docText = docText.trim();

        String outputText = docId+"\t"+docText; //TODO 2019-07-12 ps: hier muss noch die ID davor geschrieben werden

        doc2vecWriter.append(outputText);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.uima.analysis_component.AnalysisComponent_ImplBase#
     * collectionProcessComplete()
     */
    @Override
    public void collectionProcessComplete() throws AnalysisEngineProcessException {

    }

}
