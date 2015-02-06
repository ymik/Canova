package org.canova.api.records.writer.impl;


import org.canova.api.writable.Writable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

/**
 * Write matlab records
 */
public class MatlabRecordWriter extends FileRecordWriter {

  public MatlabRecordWriter(File path) throws FileNotFoundException {
        super(path);
    }

    @Override
    public void write(Collection<Writable> record) throws IOException {
        StringBuilder result = new StringBuilder();

        int count = 0;
        for(Writable w : record) {
            // attributes
            if (count > 0) {
              boolean tabs = false;
              result.append((tabs ? "\t" : " "));
            }
            result.append(w.toString());
            count++;

        }

        out.write(result.toString().getBytes());
        out.write(NEW_LINE.getBytes());



    }
}
