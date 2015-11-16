/*
 *
 *  *
 *  *  * Copyright 2015 Skymind,Inc.
 *  *  *
 *  *  *    Licensed under the Apache License, Version 2.0 (the "License");
 *  *  *    you may not use this file except in compliance with the License.
 *  *  *    You may obtain a copy of the License at
 *  *  *
 *  *  *        http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  *    Unless required by applicable law or agreed to in writing, software
 *  *  *    distributed under the License is distributed on an "AS IS" BASIS,
 *  *  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  *    See the License for the specific language governing permissions and
 *  *  *    limitations under the License.
 *  *
 *
 */

package org.canova.api.records.reader.impl;

import org.apache.commons.io.IOUtils;
import org.canova.api.conf.Configuration;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.writer.RecordWriter;
import org.canova.api.records.writer.impl.FileRecordWriter;
import org.canova.api.records.writer.impl.LibSvmRecordWriter;
import org.canova.api.split.FileSplit;
import org.canova.api.writable.Writable;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Adam Gibson
 */
public class LibSvmTest {
    @Test
    public void testReadWrite() throws Exception {
        Configuration conf = new Configuration();
        conf.set(FileRecordReader.APPEND_LABEL,"true");
        File out = new File("iris.libsvm.out");
        conf.set(FileRecordWriter.PATH,out.getAbsolutePath());
        RecordReader libSvmRecordReader = new LibSvmRecordReader();
        libSvmRecordReader.initialize(conf,new FileSplit(new ClassPathResource("iris.libsvm").getFile()));

        RecordWriter writer = new LibSvmRecordWriter();
        writer.setConf(conf);
        Collection<Collection<Writable>> data = new ArrayList<>();
        while(libSvmRecordReader.hasNext()) {
            Collection<Writable> record = libSvmRecordReader.next();
            writer.write(record);
            data.add(record);
        }

        out.deleteOnExit();
        Collection<Collection<Writable>> test = new ArrayList<>();
        RecordReader testLibSvmRecordReader = new LibSvmRecordReader();
        testLibSvmRecordReader.initialize(conf,new FileSplit(out));
        while(testLibSvmRecordReader.hasNext())
            test.add(testLibSvmRecordReader.next());
        assertEquals(data,test);



    }


}
