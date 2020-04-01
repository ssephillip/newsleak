package uhh_lt.newsleak.util;

import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Doc2VecUtil {

    /**
     * Deletes the training data of the previous run (if there is any).
     * If the old training data is not delted, the new training data is appended to the old training data.
     *
     * @param trainingFileString Path to the training file
     * @param logger UIMA logger
     */
    public static void deleteOldTrainingData(String trainingFileString, Logger logger){
        File trainingFile = new File(trainingFileString);
        if(trainingFile.isFile()) {
            try {
                FileWriter fileWriter = new FileWriter(trainingFile, false);
                fileWriter.write("");
                fileWriter.close();
                logger.log(Level.INFO, "Deleted old training data.");
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error deleting old training data.");
                e.printStackTrace();
                System.exit(1);
            }
        }

    }

}
