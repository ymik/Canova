package org.canova.api.records.reader.impl;

import org.canova.api.io.data.Text;
import org.canova.api.split.FileSplit;
import org.canova.api.split.StringSplit;
import org.canova.api.util.ClassPathResource;
import org.canova.api.writable.Writable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class CSVRecordReaderTest {
    @Test
    public void testNext() throws Exception {
        CSVRecordReader reader = new CSVRecordReader();
        reader.initialize(new StringSplit("1,1,8.0,,,,14.0,,,,15.0,,,,,,,,,,,,1"));
        while (reader.hasNext()) {
            Collection<Writable> vals = reader.next();
            List<Writable> arr = new ArrayList<>(vals);

            assertEquals("Entry count", 23, vals.size());
            Text lastEntry = (Text)arr.get(arr.size()-1);
            assertEquals("Last entry garbage", 1, lastEntry.getLength());
        }
    }

    @Test
    public void testEmptyEntries() throws Exception {
        CSVRecordReader reader = new CSVRecordReader();
        reader.initialize(new StringSplit("1,1,8.0,,,,14.0,,,,15.0,,,,,,,,,,,,"));
        while (reader.hasNext()) {
            Collection<Writable> vals = reader.next();
            assertEquals("Entry count", 23, vals.size());
        }
    }

    @Test
    public void testReset() throws Exception {
        CSVRecordReader rr = new CSVRecordReader(0,",");
        rr.initialize(new FileSplit(new ClassPathResource("iris.dat").getFile()));
        int nResets = 5;
        for( int i=0; i<nResets; i++ ){
            rr.reset();

            int lineCount = 0;
            while(rr.hasNext()){
                Collection<Writable> line = rr.next();
                assertEquals(5,line.size());
                lineCount++;
            }

            assertEquals(150,lineCount);
        }
    }
}
