package org.canova.api.records.reader.impl;

import com.sun.prism.impl.Disposer;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.InputSplit;
import org.canova.api.writable.Writable;

import java.io.IOException;
import java.util.*;

/**
 * @author sonali
 */
/**
RecordReader for each pipeline. Individual record is a concatenation of the two collections.
        Create a recordreader that takes recordreaders and iterates over them and concatenates them
        hasNext would be the & of all the recordreaders
        concatenation would be next & addAll on the collection
        return one record
 */
public class ComposableRecordReader implements RecordReader {

    private RecordReader[] readers;

    public ComposableRecordReader(RecordReader...readers) {
        this.readers = readers;
    }

    @Override
    public void initialize(InputSplit split) throws IOException, InterruptedException {

    }

    @Override
    public Collection<Writable> next() {
        List<Writable> ret = new ArrayList<>();
        if (this.hasNext()) {
            for (RecordReader reader: readers) {
                ret.addAll(reader.next());
            }
        }
        return ret;
    }

    @Override
    public boolean hasNext() {
        Boolean readersHasNext = true;
        for (RecordReader reader: readers) {
            readersHasNext = readersHasNext && reader.hasNext();
        }
        return readersHasNext;
    }

    @Override
    public void close() throws IOException {
       for(RecordReader reader : readers)
           reader.close();
    }
}
