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

        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(trainingFile, false);
            fileWriter.write("");
            fileWriter.close();
            logger.log(Level.INFO, "Deleted old training data.");
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error deleting old training data.");
            e.printStackTrace();
            System.exit(1);
        }

        return true;
    }

    /**
     * Append text to the training file.
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
