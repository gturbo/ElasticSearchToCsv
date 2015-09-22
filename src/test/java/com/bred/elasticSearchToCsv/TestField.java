package com.bred.elasticSearchToCsv;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: ET80860
 * Date: 21/09/15
 * Time: 15:03
 * To change this template use File | Settings | File Templates.
 */
public class TestField {
    @Test
    public void testClean() throws Exception {
        Field f = new Field("test", new Parameters(null,null,null,"a",null,null,null,null));
        Assert.assertEquals("a", f.cleanString("\"a\""));
        Assert.assertEquals("", f.cleanString("\"\""));
        Assert.assertEquals("", f.cleanString(null));
    }
}
