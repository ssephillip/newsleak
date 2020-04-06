import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class Main {

    /**
     * Starts the downloading tool "TransparenzDownloader".
     * The TransparenzDownloader downloads the actual files that belong to the resources in the Transparenzportal Solr Index.
     * When specified, multiple threads are used to download the files in parallel.
     *
     * For more information on datasets and resources see {@link TpResource}.
     *
     * The TransparenzDownloader can be run as a standalone jar or from within an IDE.
     * To create the jar, go into the project directory ([the path to newsleak]/newsleak/transparenzdownloader/) and execute the following command from the command line:
     * <code>mvn clean package assembly:single"</code>
     * The jar is found at:
     * [the path to newsleak]/newsleak/transparenzdownloader/target/transparenzdownloader-jar-with-dependencies.jar
     *
     *
     * <h3>The following parameters must be provided:</h3>
     * 1. Solr address - The address (URL) to the Transparenzportal Solr Index <b>core</b> (see the example)
     * 2. Path to the folder where the documents will be stored.
     * 3. Path to the file to which the stats will be written to (the file does not need to exist). Must be a .txt file. See below for more information.
     * 4. File formats that will be downloaded (separated with a comma). Only files of the specified formats will be downloaded. See below for a list of the available formats.
     * 5. Number of documents to download. If all documents shall be downloaded, simply choose a very large number (e.g. 1000000).
     * 6. Number of threads used for downloading the files.
     *
     *
     *
     * <h3>Examples:</h3>
     * Conceptual example:
     * "solrAddress" "pathToFileStorage" "pathToStatsFile/stats-file.txt" "format-1,format-2,format-3" numberOfFilesToDownload numberOfThreads
     *
     * Concrete example command line parameters:
     * "http://localhost:8983/solr/simfin" "/home/user/data/file-folder/" "/home/user/data/stats-file.txt" "pdf,csv,html" 150000  3
     *
     *
     *
     *
     * <h3>Additional information:</h3>
     * <h4>Stats:</h4>
     * The Stats are appended to the existing Stats in a file.
     * If the file does't exist, a new file with the specified name is created.
     * The folder containing the files that are downloaded can get very large (and thus slow).
     * Therefore, it can be useful to chose a different location for the stats file than for the files that are downloaded.
     *
     * <h4>File formats:</h4>
     * All file formats that occur in the Transparenzportal can be downloaded.
     * As of December 2019 these are (in alphabetical order):
     * ascii, citygml, csv, data, doc, docx, dxf, ert, gml, html, jpeg, jpg, json, ov2, pdf, png, rar, shapefiles, tiff, txt, web, wfs, wms, xls, xlsx, xml, xsd, zip
     *
     * @param args the command line parameters
     */
    public static void main(String[] args){
        //gets the command line parameters
        String solrAddress = args[0];
        String path = args[1];
        String pathToStats = args[2];
        List<String> formatsToDownload = Arrays.asList(args[3].split(","));
        int numOfDocs = Integer.valueOf(args[4]);
        int numOfThreads =Integer.valueOf(args[5]);

        //starts the download process
        TransparenzResourceDownloader transparenzResourceDownloader = new TransparenzResourceDownloader(solrAddress);
        Instant start = Instant.now();
        try {
            transparenzResourceDownloader.download(path, pathToStats, formatsToDownload, numOfDocs, numOfThreads);
        }catch(InstantiationException e){
            System.out.println("Couldn't retrieve IDs!");
            e.printStackTrace();
        }
        Instant end = Instant.now();
        System.out.println("Downloaded all documents in "+ Duration.between(start,end).getSeconds()+" seconds");
    }





}