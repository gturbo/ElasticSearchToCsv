package com.bred.elasticSearchToCsv;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.io.IOException;
import java.io.Writer;
import java.text.DecimalFormat;

/**
 * Created with IntelliJ IDEA.
 * User: ET80860
 * Date: 15/09/15
 * Time: 12:55
 * To change this template use File | Settings | File Templates.
 */
public class Field {
    final private String name;
    final Parameters params;
    final DecimalFormat decimalFormat;

    public Field(String name, Parameters params) {
        this.name = name;
        this.params = params;
        this.decimalFormat = new DecimalFormat("0.##");
    }


    private void writeString(String value, Writer out) throws IOException {
        final char fieldDelimiter = params.delimiter;
        final char quoteChar = params.quoteChar;
        final int length = value.length();
        final char escapeChar = params.escapeChar;
        if (quoteChar == '\u0000') {  // no quotes
            if (params.noEscape) {    // no escape char we suppress some characters from string values
                for (int i = 0; i < length; i++) {
                    final char c = value.charAt(i);
                    if (c == '\u0000' || c == fieldDelimiter || c == '\n' || c == '\r') {
                    } else {
                        out.write(c);
                    }
                }
            } else {   // escape special characters
                for (int i = 0; i < length; i++) {
                    final char c = value.charAt(i);
                    if (c == '\u0000') {
                    } else if (c == escapeChar || c == fieldDelimiter || c == '\n' || c == '\r') {
                        out.write(escapeChar);
                        out.write(c);
                    } else {
                        out.write(c);
                    }
                }
            }
        } else {
            out.write(quoteChar);
            if (params.noEscape) {    // no escape char we suppress some characters from string values
                for (int i = 0; i < length; i++) {
                    final char c = value.charAt(i);
                    if (c == '\u0000' || c == quoteChar || c == '\n' || c == '\r') {
                    } else {
                        out.write(c);
                    }
                }
            } else {
                for (int i = 0; i < length; i++) {
                    final char c = value.charAt(i);
                    if (c == '\u0000') {
                    } else if (c == escapeChar || c == quoteChar || c == '\n' || c == '\r') {
                        out.write(escapeChar);
                        out.write(c);
                    } else {
                        out.write(c);
                    }
                }
            }
            out.write(params.quoteChar);
        }
    }

    private String cleanString(final String src) {
        return (src.length() >1 && src.charAt(0) == '"') ?
                src.substring(1, src.length() - 2)
                : src;
    }

    void writeValue(JsonObject doc, Writer out) throws IOException {
        JsonElement o = doc.get(name);
        if (o == null || o.isJsonNull()) {
            out.write(params.nullValue);
        } else if (o.isJsonPrimitive()) {
            final JsonPrimitive p = o.getAsJsonPrimitive();
            if (p.isString()) {
                writeString(p.getAsString(), out);
            } else if (p.isNumber()) {
                out.write(p.toString());
            }
        } else if (o.isJsonArray()) {
            JsonArray a = o.getAsJsonArray();
            int max = a.size() - 1;
            if (max < 0) {
                out.write(params.nullValue);
                return;
            }
            String s = "";
            int i = 0;
            for (; i < a.size() - 1; i++) {
                final String s1 = a.get(i).toString();
                s += cleanString(s1) + params.multiSeparator;
            }
            final String s1 = a.get(i).toString();
            s += cleanString(s1);
            writeString(s, out);
        } else if (o.isJsonObject()) {
            writeString(o.toString(), out);
        } else {
            // shouldn't happen
            throw new RuntimeException("unknown Json Type for element: " + o.toString());
        }
    }
}
