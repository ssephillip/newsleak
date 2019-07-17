package uhh_lt.newsleak.resources;

import org.apache.uima.fit.component.Resource_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class Doc2VecWriterResource extends Resource_ImplBase {

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

        FileWriter fileWriter;
        try {
            fileWriter = new FileWriter(trainingFile, false);
            fileWriter.write("");
            fileWriter.close();
        } catch (IOException e) {
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
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
