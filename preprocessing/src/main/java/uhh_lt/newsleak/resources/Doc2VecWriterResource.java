package uhh_lt.newsleak.resources;

import org.apache.uima.fit.component.Resource_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;


/**
 * Provides shared functionality and data for the {@link uhh_lt.newsleak.writer.Doc2VecWriter}.
 * It allows for synchronzied writing of text to the given output file.
 */
public class Doc2VecWriterResource extends Resource_ImplBase {

    /** The logger. */
    private Logger logger;

    /** The Constant PARAM_TRAINING_FILE. */
    public static final String PARAM_TRAINING_FILE = "trainingFile";

    /** The trainingFile. */
    @ConfigurationParameter(name = PARAM_TRAINING_FILE, mandatory = true, description = "File to write the training data to.")
    private File trainingFile;



    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.uima.fit.component.Resource_ImplBase#initialize(org.apache.uima.
     * resource.ResourceSpecifier, java.util.Map)
     */
    @Override
    public boolean initialize(ResourceSpecifier aSpecifier, Map<String, Object> aAdditionalParams)
            throws ResourceInitializationException {
        super.initialize(aSpecifier, aAdditionalParams);
        this.logger = this.getLogger();

        return true;
    }

    /**
     * Append training data lines to the training file.
     * A training data line consists of the newsleak-document-id and the fulltext of the corresponding document.
     * For more information see {@link uhh_lt.newsleak.writer.Doc2VecWriter}.
     *
     * @param text the text
     */
    public synchronized void append(String text) {
        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(trainingFile, true);
            fileWriter.write(text + "\n");
            fileWriter.close();
            logger.log(Level.FINEST, "Added row to training file.");
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error adding row to training file.");
            e.printStackTrace();
        }

    }
}
