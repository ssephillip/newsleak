package uhh_lt.newsleak.writer;


import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ExternalResource;
import org.apache.uima.fit.descriptor.OperationalProperties;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import uhh_lt.newsleak.resources.Doc2VecWriterResource;
import uhh_lt.newsleak.types.Metadata;
import uhh_lt.newsleak.util.StatsService;

/**
 * A writer to write the document fulltexts toa textfile.
 * This textfile is later used as training data to create the Doc2VecC document embedding vectors.
 *
 * The fulltexts are written to the textfile in the form: *
 * [newsleak-document-ID]<TAB>[document-fulltext]
 */
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
        StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.DOC2VEC_WRITER);
        Metadata metadata = (Metadata) jcas.getAnnotationIndex(Metadata.type).iterator().next();
        String docId = metadata.getDocId();
        String docText = jcas.getDocumentText();

        //removes linebreaks and multiple spaces so that each document text is one line
        docText = docText.replaceAll("[\\s\\v]+", " ");
        docText = docText.trim();

        String outputText = docId+"\t"+docText;

        doc2vecWriter.append(outputText);
        StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.DOC2VEC_WRITER);
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
