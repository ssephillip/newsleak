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

public class DocEmbeddingCreator extends NewsleakPreprocessor {

    public static void main(String[] args) {
        DocEmbeddingCreator docEmbeddingCreator = new DocEmbeddingCreator();
        //docEmbeddingCreator.getConfiguration(args);

        docEmbeddingCreator.hooverHost = "http://localhost";
        docEmbeddingCreator.dataDirectory = "./data";
        docEmbeddingCreator.createEmbeddings();
        docEmbeddingCreator.indexVectors();

    }

    public void createEmbeddings() {

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.directory(new File(dataDirectory+File.separator+"doc2vec"));
        processBuilder.command("bash", "-c","bash produce_vectors.sh");

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
        //TODO 2019-07-21 ps: introduce new configuration variable for the python server url and port (similar to hooverurl)
        HttpPost httpPost = new HttpPost(hooverHost+":5002"+"/index_vectors");

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addBinaryBody("docvectors", new File(dataDirectory+File.separator+"doc2vec/docvectors.txt"), ContentType.APPLICATION_OCTET_STREAM, "docvectors.ext");
        builder.addBinaryBody("doc2vec_id", new File(dataDirectory+File.separator+"doc2vec/doc2vec_id.txt"), ContentType.APPLICATION_OCTET_STREAM, "doc2vec_id.ext");
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
