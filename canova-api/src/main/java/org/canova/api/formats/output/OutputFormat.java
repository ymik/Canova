package org.nd4j.api.formats.output;

import org.nd4j.api.conf.Configuration;
import org.nd4j.api.exceptions.CanovaException;
import org.nd4j.api.records.writer.RecordWriter;

/**
 * Create a record writer
 * @author Adam Gibson
 */
public interface OutputFormat {

    public static final String OUTPUT_PATH = "org.nd4j.outputpath";

    /**
     * Create a record writer
     * @return the created writer
     */
    RecordWriter createWriter(Configuration conf) throws CanovaException;

}
