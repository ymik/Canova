package org.canova.codec.reader;

import static org.junit.Assert.*;

import org.canova.api.conf.Configuration;
import org.canova.api.records.reader.SequenceRecordReader;
import org.canova.api.split.FileSplit;
import org.canova.api.writable.Writable;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.util.Collection;

/**
 * @author Adam Gibson
 */
public class CodecReaderTest {
    @Test
    public void testCodecReader() throws Exception {
       File file = new ClassPathResource("fire.mp4").getFile();
        SequenceRecordReader reader = new CodecRecordReader();
        Configuration conf = new Configuration();
        conf.set(CodecRecordReader.RAVEL,"true");
        conf.set(CodecRecordReader.START_FRAME,"160");
        conf.set(CodecRecordReader.TOTAL_FRAMES,"500");
        reader.initialize(new FileSplit(file));
        reader.setConf(conf);
        assertTrue(reader.hasNext());
        Collection<Collection<Writable>> record = reader.sequenceRecord();
        System.out.println(record.size());
    }

}
