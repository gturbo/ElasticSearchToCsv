package com.bred.elasticSearchToCsv;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: ET80860
 * Date: 15/09/15
 * Time: 12:23
 * To change this template use File | Settings | File Templates.
 */
public class ElasticSearchToCsv {

    private Parameters params;

    public ElasticSearchToCsv(Parameters params) {
        this.params = params;
    }

    private Writer out;
    private Field[] fields;

    void extract() throws IOException {
        getFields();
        openFile();
        JestClient client = params.getClient();
        Search search = (Search) new Search.Builder(params.query)
                // multiple index or types can be added.
                .addIndex(params.indices)
                .setParameter(io.searchbox.params.Parameters.SIZE, params.scrollSize)
                .setParameter(io.searchbox.params.Parameters.SCROLL, params.timeout)
                .build();

        JestResult result = client.execute(search);
        boolean doContinue = true;
        final JsonObject allHits = result.getJsonObject().getAsJsonObject("hits");
        final long allCount = allHits.getAsJsonPrimitive("total").getAsLong();
        final long limit = params.limit >= 0 ? Math.min(allCount, params.limit) : allCount;
        System.out.println("record count: " + allHits.getAsJsonPrimitive("total").getAsBigInteger().toString());
        System.out.println("limiting records to : " + limit);
        JsonArray hits = allHits.getAsJsonArray("hits");
        long found = 0;
        System.out.print("Avancement:");
        while (result.isSucceeded() && found < limit && hits.size() > 0) {
            found += hits.size();
            System.out.print(" " + found * 100l / limit + "%");
            toCsv(hits);
            final String scrollId = result.getJsonObject().getAsJsonPrimitive("_scroll_id").getAsString();
            SearchScroll scroll = new SearchScroll.Builder(scrollId, params.timeout).build();
        }
        out.close();
    }

    private Field[] getFields() {
        if (fields == null) {
            String[] fieldNames = params.fieldsString.split(" ");
            final int numFields = fieldNames.length;
            fields = new Field[numFields];
            for (int i = 0; i < numFields; i++) {
                fields[i] = new Field(fieldNames[i], params);
            }
        }
        return fields;
    }

    private void openFile() throws IOException {
         out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(params.outputFilePath), params.charsetName)));
    }

    private void toCsv(JsonArray hits) throws IOException {
        final int length = hits.size();
        if (length == 0) {
            System.out.println("WARNING empty result for scroll");
            return;
        }
        final Field[] fields = getFields();
        final int lastField = fields.length - 1;
        int row = 0;
        while (row < length) {
            final JsonObject doc = hits.get(row).getAsJsonObject().getAsJsonObject("_source");
            int f = 0;
            while (f < lastField) {
                fields[f].writeValue(doc, out);
                out.write(params.delimiter);
                f++;
            }
            fields[f].writeValue(doc, out); // last field
            out.write('\n');
            row++;
        }
    }
}
