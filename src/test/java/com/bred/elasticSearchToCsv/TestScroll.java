package com.bred.elasticSearchToCsv;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ET80860
 * Date: 15/09/15
 * Time: 17:45
 * To change this template use File | Settings | File Templates.
 */
public class TestScroll {
   String  queryDocuments = "{\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must\":[{\"range\":{\"dateOperation\":{\"gte\":\"2015-09-08\",\"lt\":\"2015-09-09\"}}}]}}}}}";
    int scrollSize = 1;
    String scrollTimeOut = "1s";
    String indexName = "operations",
            host = "172.20.71.14",
    port = "9200";
    ;


    @Test
    public void testScroll() throws Exception {
       ElasticSearchToCsv e = new ElasticSearchToCsv(new Parameters(host,
               port,
               indexName,
               "id devise codeLibelle dateOperation referenceOperation entiteGestion compte poste sousPoste categorie sousCategorie montant libelle details pointage.id dateValeur",
               queryDocuments,
               "E:/tmp/testScrollEsToCsv.csv",
               "3",
               "20"
               ));
        e.extract();
    }

    @Test
    public void testSmallResult() throws Exception {
        smallResult("111");
    }

    private void smallResult(String part) throws IOException {
        String  query = "{\"query\":{\"filtered\":{\"filter\":{\"bool\":{\"must\":[{\"range\":{\"dateOperation\":{\"gte\":\"2015-09-08\",\"lt\":\"2015-09-09\"}}}," +
                "{\"range\":{\"compte\":{\"gte\":\"00" + part + "1\",\"lt\":\"00" + part + "2\"}}}]}}}}}";
        ElasticSearchToCsv e = new ElasticSearchToCsv(new Parameters(host,
                port,
                indexName,
                "id devise codeLibelle dateOperation referenceOperation entiteGestion compte poste sousPoste categorie sousCategorie montant libelle details pointage.id dateValeur",
                query,
                "E:/tmp/testScrollEsToCsv.csv",
                "10000",
                "-1"
        ));
        e.extract();
    }

    @Test
    public void testMultipleSmallResult() throws Exception {
        for (int i = 0; i<100;i++)
            smallResult("1"+i);
    }


    @Ignore
   @Test
    public void testPerf() throws Exception {
        ElasticSearchToCsv e = new ElasticSearchToCsv(new Parameters(host,
                port,
                indexName,
                "id devise codeLibelle dateOperation referenceOperation entiteGestion compte poste sousPoste categorie sousCategorie montant libelle details pointage.id dateValeur",
                queryDocuments,
                "E:/tmp/testScrollEsToCsv.csv",
                "5000",
                "20000"
        ));
        e.extract();
    }

}
