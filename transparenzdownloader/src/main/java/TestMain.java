import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

public class TestMain {

    public static void main(String[] args){
        String path ="/home/phillip/BA/data/versuch-1/" ;
        String urlString = "http://www.buergerschaft-hh.de/parldok/tcl/PDDocView.tcl?mode=get&lp=18&doknum=2332";
        String docId = "3e497131-c169-41f1-8338-44c0ff8c9561_0";
        String pathToFile = path+docId+".pdf";

        try {
            URL url = new URL(urlString);
            URLConnection httpUrlConnection = url.openConnection();
            httpUrlConnection.setReadTimeout(1000000);
            httpUrlConnection.connect();
            String headerGeneral = httpUrlConnection.getHeaderField("General");
            String headerResponse = httpUrlConnection.getHeaderField("Location");
            String headerStatusCode = httpUrlConnection.getHeaderField("Status Code");
            System.out.println("Header general: "+headerGeneral);
            System.out.println("Header location: "+headerResponse);
            System.out.println("Header status: "+headerStatusCode);
            InputStream in = new BufferedInputStream(httpUrlConnection.getInputStream());
            OutputStream out = new BufferedOutputStream(new FileOutputStream(pathToFile));

            for (int i; (i = in.read()) != -1; ) {
                out.write(i);
            }
            in.close();
            out.close();
        }catch(Exception e){
            System.out.println("Couldn't download dcoument.");
            e.printStackTrace();
        }
    }
}
