package org.canova.api.records.reader.impl;

import org.canova.api.io.data.Text;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.reader.impl.regex.RegexLineRecordReader;
import org.canova.api.split.FileSplit;
import org.canova.api.util.ClassPathResource;
import org.canova.api.writable.Writable;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by Alex on 12/04/2016.
 */
public class RegexRecordReaderTest {

    @Test
    public void testRegexLineRecordReader() throws Exception {
        String regex = "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}) (\\d+) ([A-Z]+) (.*)";

        System.out.println("2016-01-01 23:59:59.001 1 DEBUG First entry message!".matches(regex));

        RecordReader rr = new RegexLineRecordReader(regex, 1);
        rr.initialize(new FileSplit(new ClassPathResource("/logtestdata/logtestfile0.txt").getFile()));

        List<Writable> exp0 = Arrays.asList((Writable) new Text("2016-01-01 23:59:59.001"), new Text("1"), new Text("DEBUG"), new Text("First entry message!"));
        List<Writable> exp1 = Arrays.asList((Writable) new Text("2016-01-01 23:59:59.002"), new Text("2"), new Text("INFO"), new Text("Second entry message!"));
        List<Writable> exp2 = Arrays.asList((Writable) new Text("2016-01-01 23:59:59.003"), new Text("3"), new Text("WARN"), new Text("Third entry message!"));
        assertEquals(exp0, rr.next());
        assertEquals(exp1, rr.next());
        assertEquals(exp2, rr.next());
        assertFalse(rr.hasNext());

        //Test reset:
        rr.reset();
        assertEquals(exp0, rr.next());
        assertEquals(exp1, rr.next());
        assertEquals(exp2, rr.next());
        assertFalse(rr.hasNext());
    }

}
