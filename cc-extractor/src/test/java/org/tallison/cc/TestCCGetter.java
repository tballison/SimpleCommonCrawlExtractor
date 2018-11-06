package org.tallison.cc;

import org.junit.Test;
import org.tallison.cc.CCGetter;

import static org.junit.Assert.assertEquals;

public class TestCCGetter {

    @Test
    public void testClean() throws Exception {
        assertEquals("test", CCGetter.clean("\"test"));
        assertEquals("test", CCGetter.clean("test\""));
        assertEquals("\"te\"\"st\"", CCGetter.clean("te\"st"));
    }
}
