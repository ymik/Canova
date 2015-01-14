package org.nd4j.api.records.reader.impl;

import org.nd4j.api.records.reader.RecordReader;
import org.nd4j.api.split.InputSplit;
import org.nd4j.api.writable.Writable;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Collection record reader.
 * Mainly used for testing.
 *
 * @author Adam Gibson
 */
public class CollectionRecordReader implements RecordReader {
    private Iterator<? extends Collection<Writable>> records;

    public CollectionRecordReader(Collection<? extends Collection<Writable>> records) {
        this.records = records.iterator();
    }

    @Override
    public void initialize(InputSplit split) throws IOException, InterruptedException {

    }

    @Override
    public Collection<Writable> next() {
        return records.next();
    }

    @Override
    public boolean hasNext() {
        return records.hasNext();
    }

    @Override
    public void close() throws IOException {

    }
}
