package org.canova.api.formats.output.impl;

import org.nd4j.api.conf.Configuration;
import org.nd4j.api.exceptions.CanovaException;
import org.nd4j.api.formats.output.OutputFormat;
import org.nd4j.api.records.writer.RecordWriter;
import org.nd4j.api.records.writer.impl.CSVRecordWriter;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Creates an @link{CSVRecordWriter}
 *
 * @author Adam Gibson
 */
public class CSVOutputFormat implements OutputFormat {
    @Override
    public RecordWriter createWriter(Configuration conf) throws CanovaException {
        String outputPath = conf.get(OutputFormat.OUTPUT_PATH,".");
        try {
            return new CSVRecordWriter(new File(outputPath));
        } catch (FileNotFoundException e) {
            throw new CanovaException(e);
        }
    }
}
