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

package org.canova.image.recordreader;

import static org.junit.Assert.*;

import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.InputStreamInputSplit;
import org.canova.api.writable.Writable;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import java.util.Collection;


/**
 * @author Adam Gibson
 */
public class TestImageRecordReader {

    private static Logger log = LoggerFactory.getLogger(TestImageRecordReader.class);

    @Test
    public void testInputStream() throws Exception {
        RecordReader reader = new ImageRecordReader(28,28,false);
        ClassPathResource res = new ClassPathResource("/test.jpg");
        reader.initialize(new InputStreamInputSplit(res.getInputStream(), res.getURI()));
        assertTrue(reader.hasNext());
        Collection<Writable> record = reader.next();
        assertEquals(784,record.size());
    }




}
