package com.bred.elasticSearchToCsv;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

/**
 * Created with IntelliJ IDEA.
 * User: ET80860
 * Date: 15/09/15
 * Time: 13:09
 * To change this template use File | Settings | File Templates.
 */
public class Parameters {
    public final String host,
            port,
            indices,
            fieldsString,
            timeout,
            scrollSize,
            nullValue,
            multiSeparator,
            query,
            zeroAsNull,
            outputFilePath,
            charsetName;
    public final char quoteChar,
            delimiter,
            escapeChar;
    public final long limit;

    public final boolean noEscape;

    public JestClient getClient() {
        String connectionUrl = "http://" + host + ":" + port + "/";
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(connectionUrl)
                .multiThreaded(false)
                .build());
        return factory.getObject();
    }

    public Parameters(String host, String port, String indices, String fieldsString, String query, String outputFilePath, String scrollSize, String limit) {
        this(host, port, indices, fieldsString, query, outputFilePath, scrollSize, limit, null, "60s", "", " ", ";", null, "\\", "UTF-8");
    }

    @Override
    public String toString() {
        return "Parameters:" +
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                "\nindices='" + indices + '\'' +
                "\nfields='" + fieldsString + '\'' +
                "\nquery='" + query + '\'' +
                "\ntimeout='" + timeout + '\'' +
                ", scrollSize='" + scrollSize + '\'' +
                ", limit=" + limit +
                ", nullValue='" + nullValue + '\'' +
                ", multiSeparator='" + multiSeparator + '\'' +
                ", zeroAsNull='" + zeroAsNull + '\'' +
                ", outputFilePath='" + outputFilePath + '\'' +
                ", charsetName='" + charsetName + '\'' +
                ", quoteChar=" + quoteChar +
                ", delimiter=" + delimiter +
                ", escapeChar=" + escapeChar +
                ", noEscape=" + noEscape +
                '}';
    }

    public Parameters(String host, String port, String indices, String fieldsString, String query, String outputFilePath, String scrollSize, String limit,
                      String zeroAsNull, String timeout, String nullValue, String multiSeparator, String delimiter, String quoteChar, String escapeChar, String charsetName) {
        this.host = host == null || host.isEmpty() ? "localhost" : host;
        this.port = port == null || port.isEmpty() ? "9200" : port;
        this.indices = indices;

        if (fieldsString == null || fieldsString.isEmpty()) throw new RuntimeException("field List cannot be empty");

        this.fieldsString = fieldsString;
        this.timeout = timeout;
        this.scrollSize = scrollSize;
        this.nullValue =  nullValue == null ? "" : nullValue;
        this.multiSeparator = multiSeparator;
        if (delimiter == null || delimiter.isEmpty()) {
            delimiter = ";";
        } else if (delimiter.length() > 1) {
            throw new RuntimeException("delimiter must be one character value: " + delimiter + " forbidden");
        }
        this.delimiter = delimiter.charAt(0);
        if (escapeChar == null || escapeChar.isEmpty()) {
            escapeChar = "\u0000";
        } else if (escapeChar.length() > 1) {
            throw new RuntimeException("escapeChar must be one character value: " + escapeChar + " forbidden");
        }
        this.escapeChar = escapeChar.charAt(0);
        if (quoteChar == null || quoteChar.isEmpty()) {
            quoteChar = "\u0000";
        } else if (quoteChar.length() > 1) {
            throw new RuntimeException("quoteChar must be one character value: " + quoteChar + " forbidden");
        }
        this.quoteChar = quoteChar.charAt(0);
        this.noEscape = this.quoteChar == '\u0000';
        if (limit == null || limit.isEmpty()) {
            this.limit = -1l;
        } else {
            this.limit = Long.parseLong(limit);
        }
        this.query = query;
        this.zeroAsNull = zeroAsNull == null ? "" : zeroAsNull;
        this.outputFilePath = outputFilePath;
        this.charsetName = charsetName;
    }
}
