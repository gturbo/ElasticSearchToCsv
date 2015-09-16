package com.bred.elasticSearchToCsv;

import junit.framework.Assert;
import org.junit.Test;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

/**
 * Created with IntelliJ IDEA.
 * User: ET80860
 * Date: 15/09/15
 * Time: 17:39
 * To change this template use File | Settings | File Templates.
 */
public class TestNumberPattern {
    final DecimalFormat f = new DecimalFormat("0.##", new DecimalFormatSymbols(new Locale("US")));
    @Test
    public void testInteger() throws Exception {
        Assert.assertEquals("1", f.format(1));
        Assert.assertEquals("1000000000000",  f.format(1000000000000l));
    }

    @Test
    public void testDouble() throws Exception {
      //  f.setMaximumIntegerDigits(309);
        Assert.assertEquals("1000.24", f.format(1000.2401));
        Assert.assertEquals("10000050000000000000", f.format(1.000005000000000000024E19));
        Assert.assertEquals("1000000000000000000000000000000", f.format(1E30) );
    }
}
