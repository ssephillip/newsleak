package uhh_lt.newsleak.preprocessing;

import org.apache.http.HttpEntity;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.uima.util.Level;
import uhh_lt.newsleak.util.StatsService;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Provides functionality to create document embedding vectors and store these vectors in a remote vector index (via Http Request)
 */
public class DocEmbeddingManager extends NewsleakPreprocessor {

    //Creates Doc2VecC document embedding vectors and stores them in a remote vector index

    /**
     * Starts the Doc2VecC document embedding process and stores them in a remote vector index
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        //TODO remove the outprints of the file names
        DocEmbeddingManager docEmbeddingManager = new DocEmbeddingManager();
        docEmbeddingManager.getConfiguration(args);

        StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.EMBEDDING);
        docEmbeddingManager.createEmbeddings();
        StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.EMBEDDING);


        StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.EMBEDDING_INDEXING);
        docEmbeddingManager.indexVectors();
        StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.EMBEDDING_INDEXING);
    }



    /**
     * Triggers a Shell script which prepares the training data and starts the C-Program that exectutes the embedding process.
     * The C-Program is the implementation of Doc2VecC provided by the author of the paper that introduces Doc2VecC.
     * For more information on the C-Program an Doc2VecC go to the <a href="https://github.com/mchen24/iclr2017">Github repository</a>.
     *
     * The resulting vectors are written to a textfile.
     */
    public void createEmbeddings() {

        //Intializes the process builder
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.directory(new File(this.doc2vecTrainingDir));

        //specifies the file to execute and the parameters
        //TODO 2020-04-01 ps: define parameters is separate strings
        //TODO 2020-04-01 ps: evtl. prepare training data within java
        processBuilder.command(this.doc2vecTrainingDir+File.separator+"produce_vectors.sh", this.trainingFileName+".txt", this.trainingFileNameDocOnly+".txt", this.trainingFileNameIdOnly+".txt", this.doc2vecResultFileName+".txt");


        try {
            logger.log(Level.INFO, "Starting document embedding process with "+this.trainingFileName+" as training data.");
            Process process = processBuilder.start();

            StringBuilder output = readOutput(process); //For some reason the script does not finish/return if the output isn't read. For this reason we read the output here. The output is of no relevance. It is just a bug fix since I was not able to identify the problem.

            int exitVal = process.waitFor();
            if (exitVal != 0) {
                throw new IllegalArgumentException();
            }

            logger.log(Level.INFO, "Finished document embedding process successfully.");

        }catch(Exception e){
            logger.log(Level.SEVERE, "Failed producing document embeddings with "+this.trainingFileName+" as training data.");
            e.printStackTrace();
        }

    }

    /**
     * Reads vectors from a textfile and stores them in a remote vector index.
     * The data is sent to the vector index via Http request.
     */
    public void indexVectors(){
        logger.log(Level.INFO, "Preparing document vector indexing.");
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(this.doc2vecIndexUrl+":"+this.doc2vecIndexPort+"/index_vectors");

        //Constructs the Multipart and adds it to the http request
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        //TODO 2020-04-01 ps: introduce separate string variables for parameters
        builder.addBinaryBody("vectors", new File(this.doc2vecResultDir+File.separator+this.doc2vecResultFileName +".txt"), ContentType.APPLICATION_OCTET_STREAM, doc2vecResultFileName +".ext"); //TODO 2019-09-18 ps: introduce own attribute in config file for result dir
        builder.addBinaryBody("ids", new File(this.doc2vecTrainingDir+File.separator+this.trainingFileNameIdOnly+".txt"), ContentType.APPLICATION_OCTET_STREAM, trainingFileNameIdOnly+".ext");//TODO 2019-09-18 ps: maybe copy id file also to result dir for consistency
        HttpEntity multipart = builder.build();
        httpPost.setEntity(multipart);

        try {
            //executes http request
            logger.log(Level.INFO, "Starting document vector indexing. Index URL: "+httpPost.getURI());
            CloseableHttpResponse response = client.execute(httpPost);
            client.close();

            //logs if the vectors were indexed successfully based on the http response code
            int statusCode = response.getStatusLine().getStatusCode();
            if(statusCode == 200) {
                logger.log(Level.INFO, "Finished document vector indexing successfully.");
            }else{
                logger.log(Level.SEVERE, "Http request failed with status code: "+statusCode);
                throw new HttpResponseException(statusCode,"Vector indexing failed.");
            }
        }catch(Exception e){
            logger.log(Level.SEVERE, "Failed indexing document vectors. Index URL: "+httpPost.getURI());
            e.printStackTrace();
        }

    }


    /**
     * Method to read the output of a process
     * @param process A process
     * @return The output of the process
     * @throws IOException
     */
    private StringBuilder readOutput(Process process) throws IOException {
        StringBuilder output = new StringBuilder();
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            output.append(line + "\n");
        }

        return output;
    }


}
