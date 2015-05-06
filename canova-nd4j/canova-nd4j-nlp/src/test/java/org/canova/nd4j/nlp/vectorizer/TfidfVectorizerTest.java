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

package org.canova.nd4j.nlp.vectorizer;

import static org.junit.Assert.*;

import org.canova.api.conf.Configuration;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.reader.impl.CollectionRecordReader;
import org.canova.api.writable.Writables;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;

import java.util.Arrays;

/**
 * Created by agibsonccc on 11/23/14.
 */
public class TfidfVectorizerTest {

    private static Logger log = LoggerFactory.getLogger(TfidfVectorizerTest.class);

    @Test
    public void testTfidfVectorizer() {
        TfidfVectorizer vectorizer = new TfidfVectorizer();
        vectorizer.initialize(new Configuration());
        RecordReader reader = new CollectionRecordReader(Writables.writables(Arrays.asList("Testing one.", "Testing 2.")));
        INDArray n = vectorizer.fitTransform(reader);
        //number of vocab words is 3
        assertEquals(3,n.columns());
        //number of records is 2
        assertEquals(2,n.rows());
    }

}
