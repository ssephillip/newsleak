import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import java.util.List;

public class TransparenzUtils {






    public static SolrDocumentList getAllOuterDocumentsFromSolr(HttpSolrClient solrClient, String solrCoreAddress) throws InstantiationException{
        QueryResponse response = null;



        SolrQuery documentQuery = new SolrQuery("*:*");
        documentQuery.addField("id");
        documentQuery.addField("res_format");
        documentQuery.addField("res_url");
        documentQuery.addField("res_name");
        documentQuery.addField("title");
        documentQuery.addField("publishing_date");
        documentQuery.setRows(Integer.MAX_VALUE); //TODO evtl. weg, da immer nur ein ergebnis kommen sollte


        try {
            response = solrClient.query(documentQuery);

            if (response == null) {
                throw new InstantiationException(); //TODO 2019-07-11 ps: ist diese Exception hier korrekt?
            }
        } catch (Exception e) {
            System.out.println("Failed retrieving outer documents from index "+solrCoreAddress);

            e.printStackTrace();
            throw new InstantiationException(); //TODO 2019-08-20 ps: sinnvolle exception schmeißen
        }

        return response.getResults();
    }




}
