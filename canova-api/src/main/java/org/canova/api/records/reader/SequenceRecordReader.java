package org.canova.api.records.reader;

import org.canova.api.writable.Writable;

import java.util.Collection;

/**
 * A sequence of records.
 *
 * @author Adam Gibson
 */
public interface SequenceRecordReader extends RecordReader {
    /**
     * Returns a sequence record`
     * @return a sequence of records
     */
    Collection<Collection<Writable>> sequenceRecord();
}
