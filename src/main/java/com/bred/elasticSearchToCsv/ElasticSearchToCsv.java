package com.bred.elasticSearchToCsv;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;

import java.io.*;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.SynchronousQueue;

/**
 * Created with IntelliJ IDEA.
 * User: ET80860
 * Date: 15/09/15
 * Time: 12:23
 * To change this template use File | Settings | File Templates.
 */
public class ElasticSearchToCsv {

    private Parameters params;
    private long generationTime;
    private long generationNumber;
    volatile boolean  extractionDone = false;

    public ElasticSearchToCsv(Parameters params) {
        this.params = params;
    }

    private Writer out;
    private Field[] fields;

    public void extract() throws IOException {
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
        JsonObject allHits = result.getJsonObject().getAsJsonObject("hits");
        final long allCount = allHits.getAsJsonPrimitive("total").getAsLong();
        final long limit = params.limit >= 0 ? Math.min(allCount, params.limit) : allCount;
        System.out.println("record count: " + allHits.getAsJsonPrimitive("total").getAsBigInteger().toString());
        System.out.println("limiting records to : " + limit);
        JsonArray hits = allHits.getAsJsonArray("hits");
        long found = 0;
        System.out.print("Avancement:");

/**
 *  We create a caped queue of tasks and a consuming worker thread
 *
 *  that way we can do in parallel: waiting for elastic search next response and processing data for writing file
 *
 *  could be enhanced with multiple workers (1 per core) but beware of upsizing the queue from 2 to 2*nbThread;
 *
 *  WARNING need to buffer writing of a single document to prevent record mixings
 *  and to ensure writer is multithreaded;
 *  mind that multithreading won't honour ordering in query
 *
 */

        final ArrayBlockingQueue<JsonArray> tasks = new ArrayBlockingQueue<JsonArray>(2);

        Runnable workerCode = new Runnable() {
            @Override
            public void run() {
                try {
                    while (!(extractionDone && tasks.isEmpty())) {
                        long start = System.currentTimeMillis();
                        toCsv(tasks.take());
                        generationTime+=System.currentTimeMillis()-start;
                        generationNumber++;
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException("worker interrupted", e);
                } catch (IOException ioe) {
                    throw new RuntimeException("worker IOException", ioe);
                }
            }
        };

        Thread worker = new Thread(workerCode);
        worker.start();


        while (result.isSucceeded() && found < limit && hits.size() > 0)
        {
            try {
                tasks.put(hits);
            } catch (InterruptedException e) {
                out.close();
                throw new RuntimeException("interrupted while waiting putting hittings in the queue", e);
            }
            found += hits.size();
            if (found < limit)
                System.out.print(" " + found * 100l / limit + "%");
            else
                System.out.println("\nlast record fetched extracted " + found + " records");
            ;
            final String scrollId = result.getJsonObject().getAsJsonPrimitive("_scroll_id").getAsString();
            SearchScroll scroll = new SearchScroll.Builder(scrollId, params.timeout).build();
            result = client.execute(scroll);
            allHits = result.getJsonObject().getAsJsonObject("hits");
            hits = allHits.getAsJsonArray("hits");
            System.out.flush();
        }

        extractionDone = true;
        long waitTime =  (generationNumber > 0) ? (tasks.size()+1)*generationTime/generationNumber*12/10
                : 100l * new Long(params.scrollSize);
        try {
            worker.join(waitTime);
        } catch (InterruptedException e) {
            out.close();
            throw new RuntimeException("interrupted while waiting for worker thread", e);
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

    @Override
    public String toString() {
        return "ElasticSearchToCsv extraction with " + params;
    }
}
