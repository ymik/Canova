package org.canova.api.records.reader.factory;

import org.canova.api.records.writer.RecordWriter;

import java.net.URI;

/**
 * Factory for creating RecordWriter instance
 *
 * @author sonali
 */
public interface RecordWriterFactory {

    /**
     *
     * @param uri destination for saving model
     * @return record writer instance
     * @throws Exception
     */

    RecordWriter create(URI uri) throws Exception;
}
