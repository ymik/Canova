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

package org.canova.nd4j.nlp.reader;

import org.canova.api.conf.Configuration;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.InputSplit;
import org.canova.api.vector.Vectorizer;
import org.canova.api.writable.Writable;
import org.canova.nd4j.nlp.vectorizer.TfidfVectorizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Adam Gibson
 */
public class TfidfRecordReader implements RecordReader  {
    private TfidfVectorizer tfidfVectorizer;
    private Collection<Collection<Writable>> records = new ArrayList<>();
    private Iterator<Collection<Writable>> recordIter;
    private Configuration conf;

    @Override
    public void initialize(InputSplit split) throws IOException, InterruptedException {
        initialize(new Configuration(),split);
    }

    @Override
    public void initialize(Configuration conf, InputSplit split) throws IOException, InterruptedException {
        tfidfVectorizer = new TfidfVectorizer();
        tfidfVectorizer.initialize(conf);
        tfidfVectorizer.fit(this, new Vectorizer.RecordCallBack() {
            @Override
            public void onRecord(Collection<Writable> record) {
                records.add(record);
            }
        });

        recordIter = records.iterator();

    }

    @Override
    public Collection<Writable> next() {
        return recordIter.next();
    }

    @Override
    public boolean hasNext() {
        return recordIter.hasNext();
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
