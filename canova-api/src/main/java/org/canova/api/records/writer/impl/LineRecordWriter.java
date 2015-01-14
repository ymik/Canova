package org.nd4j.api.records.writer.impl;

import org.nd4j.api.io.data.Text;
import org.nd4j.api.writable.Writable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

/**
 * Line record writer
 * @author Adam Gibson
 */
public class LineRecordWriter extends FileRecordWriter {


    public LineRecordWriter(File path) throws FileNotFoundException {
        super(path);
    }

    @Override
    public void write(Collection<Writable> record) throws IOException {
         if(!record.isEmpty()) {
             Text t = (Text) record.iterator().next();
             t.write(out);
             out.write(NEW_LINE.getBytes());
         }


    }
}
