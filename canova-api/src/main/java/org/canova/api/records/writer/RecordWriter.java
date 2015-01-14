package org.nd4j.api.records.writer;


import org.nd4j.api.writable.Writable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/**
 *  Record writer
 *  @author Adam Gibson
 */
public interface RecordWriter extends Closeable {


    /**
     * Write a record
     * @param record the record to write
     */
    void write(Collection<Writable> record) throws IOException;



    void close();

}
