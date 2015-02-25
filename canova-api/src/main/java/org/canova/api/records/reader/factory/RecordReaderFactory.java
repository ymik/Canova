package org.canova.api.records.reader.factory;

import org.canova.api.exceptions.UnknownFormatException;
import org.canova.api.records.reader.RecordReader;

import java.net.URI;

/**
 * Factory for creating RecordReader instance
 *
 * @author sonali
 */
public interface RecordReaderFactory {
    /**
     * Creates instance of RecordReader
     *
     * @param uri
     * @return record reader instance
     * @throws UnknownFormatException
     */
    RecordReader create(URI uri) throws UnknownFormatException;
}
