package uhh_lt.newsleak.preprocessing;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class DocEmbeddingManager extends NewsleakPreprocessor {



    public static void main(String[] args) {
        DocEmbeddingManager docEmbeddingManager = new DocEmbeddingManager();
        docEmbeddingManager.getConfiguration(args);

        docEmbeddingManager.createEmbeddings();
        docEmbeddingManager.indexVectors();
    }




    public void createEmbeddings() {

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.directory(new File(this.doc2vecTrainingDir));
        processBuilder.command("bash", "-c","bash produce_vectors.sh "+this.trainingFileName+".txt "+this.trainingFileNameDocOnly+".txt "+this.trainingFileNameIdOnly+".txt "+this.doc2vecResultVector+".txt");

        try {
            Process process = processBuilder.start();

            //For some reason the script does not finish/return if the output isn't read
            //For this reason we read the output here. The output is of no relevance, it is just a bug fix since I was not able to identify the problem.
            StringBuilder output = readOutput(process);


            int exitVal = process.waitFor();
            if (exitVal != 0) {
                throw new IllegalArgumentException();
            }
            //TODO 2019-21-07 ps: Log if it worked or not (and maybe minimalistic stats)
        }catch(Exception e){
            e.printStackTrace();
            //TODO 2019-07-21 ps: log the error
        }

    }


    public void indexVectors(){
        CloseableHttpClient client = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(this.doc2vecIndexUrl+":"+this.doc2vecIndexPort+"/index_vectors");

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addBinaryBody(this.doc2vecResultVector, new File(this.doc2vecResultDir+File.separator+this.doc2vecResultVector+".txt"), ContentType.APPLICATION_OCTET_STREAM, doc2vecResultVector+".ext");
        builder.addBinaryBody(this.trainingFileNameIdOnly, new File(this.doc2vecTrainingDir+File.separator+this.trainingFileNameIdOnly+".txt"), ContentType.APPLICATION_OCTET_STREAM, trainingFileNameIdOnly+".ext");
        HttpEntity multipart = builder.build();
        httpPost.setEntity(multipart);

        try {
            CloseableHttpResponse response = client.execute(httpPost);
            System.out.println(response.getStatusLine().getStatusCode());
            client.close();
        }catch(Exception e){
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
