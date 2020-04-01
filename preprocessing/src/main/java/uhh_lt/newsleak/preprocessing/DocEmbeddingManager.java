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
import java.time.Instant;

/**
 * Provides functionality to create document embedding vectors and store these vectors to a remote vector index (via WebService)
 */
public class DocEmbeddingManager extends NewsleakPreprocessor {


    public static void main(String[] args) {
        DocEmbeddingManager docEmbeddingManager = new DocEmbeddingManager();
        docEmbeddingManager.getConfiguration(args);
        StatsService.getInstance().startNewRun(docEmbeddingManager.dataDirectory, docEmbeddingManager.threads); //TODO wieder entfernen für finale JAR (gehört eigentlich nur in InformationExtraction2Postgres)
        long startCreateEmbedding = System.currentTimeMillis();
        StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.EMBEDDING, Instant.now());
//        docEmbeddingManager.createEmbeddings();
        StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.EMBEDDING, Instant.now());
        long timeCreateEmbedding = System.currentTimeMillis()-startCreateEmbedding;

        long startVectorIndexing = System.currentTimeMillis();
        StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_START, StatsService.EMBEDDING_INDEXING, Instant.now());
        docEmbeddingManager.indexVectors();
        StatsService.getInstance().addStatsEvent(StatsService.EVENT_TYPE_STOP, StatsService.EMBEDDING_INDEXING, Instant.now());
        long timeVectorIndexing = System.currentTimeMillis()-startVectorIndexing;

        System.out.println("Time spent for creating embeddings (seconds): " + timeCreateEmbedding / 1000);
        System.out.println("Time spent for indexing the (vector) embeddings (seconds): " + timeVectorIndexing / 1000);
    }


    /**
     * This method triggers a Shell script which prepares the training data and starts a C-Script which starts the embedding process.
     * The resulting vectors are written to a textfile in the result directory.
     */
    public void createEmbeddings() {

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.inheritIO();
        processBuilder.directory(new File(this.doc2vecTrainingDir));
        //processBuilder.command("bash", "produce_vectors.sh "+this.trainingFileName+".txt "+this.trainingFileNameDocOnly+".txt "+this.trainingFileNameIdOnly+".txt "+this.doc2vecResultFileName +".txt");

        processBuilder.command(this.doc2vecTrainingDir+File.separator+"produce_vectors.sh", this.trainingFileName+".txt", this.trainingFileNameDocOnly+".txt", this.trainingFileNameIdOnly+".txt", this.doc2vecResultFileName+".txt");
        //processBuilder.command("bash produce_vectors.sh",this.trainingFileName+".txt",this.trainingFileNameDocOnly+".txt",this.trainingFileNameIdOnly+".txt",this.doc2vecResultFileName +".txt");



        try {
            logger.log(Level.INFO, "Starting document embedding process with "+this.trainingFileName+" as training data.");
            Process process = processBuilder.start();

            //For some reason the script does not finish/return if the output isn't read
            //For this reason we read the output here. The output is of no relevance, it is just a bug fix since I was not able to identify the problem.
            StringBuilder output = readOutput(process);


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
     * The data is sent via http request.
     */
    public void indexVectors(){
        logger.log(Level.INFO, "Preparing document vector indexing.");
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(this.doc2vecIndexUrl+":"+this.doc2vecIndexPort+"/index_vectors");


        //String pathToResultVectorFile =
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addBinaryBody("vectors", new File(this.doc2vecResultDir+File.separator+this.doc2vecResultFileName +".txt"), ContentType.APPLICATION_OCTET_STREAM, doc2vecResultFileName +".ext"); //TODO 2019-09-18 ps: introduce own attribute in config file for result dir
        builder.addBinaryBody("ids", new File(this.doc2vecTrainingDir+File.separator+this.trainingFileNameIdOnly+".txt"), ContentType.APPLICATION_OCTET_STREAM, trainingFileNameIdOnly+".ext");//TODO 2019-09-18 ps: maybe copy id file also to result dir for consistency
        HttpEntity multipart = builder.build();
        httpPost.setEntity(multipart);

        try {
            logger.log(Level.INFO, "Starting document vector indexing. Index URL: "+httpPost.getURI());
            CloseableHttpResponse response = client.execute(httpPost);
            client.close();

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
