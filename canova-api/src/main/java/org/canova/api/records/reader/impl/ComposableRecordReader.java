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

import org.canova.api.conf.Configuration;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.InputSplit;
import org.canova.api.writable.Writable;

import java.io.IOException;
import java.util.*;

/**
 * @author sonali
 */
/**
RecordReader for each pipeline. Individual record is a concatenation of the two collections.
        Create a recordreader that takes recordreaders and iterates over them and concatenates them
        hasNext would be the & of all the recordreaders
        concatenation would be next & addAll on the collection
        return one record
 */
public class ComposableRecordReader implements RecordReader {

    private RecordReader[] readers;

    public ComposableRecordReader(RecordReader...readers) {
        this.readers = readers;
    }

    @Override
    public void initialize(InputSplit split) throws IOException, InterruptedException {

    }

    @Override
    public void initialize(Configuration conf, InputSplit split) throws IOException, InterruptedException {

    }

    @Override
    public void initialize(String basePath, int seed, int numExamples) throws IOException {

    }

    @Override
    public Collection<Writable> next() {
        List<Writable> ret = new ArrayList<>();
        if (this.hasNext()) {
            for (RecordReader reader: readers) {
                ret.addAll(reader.next());
            }
        }
        return ret;
    }

    @Override
    public boolean hasNext() {
        Boolean readersHasNext = true;
        for (RecordReader reader: readers) {
            readersHasNext = readersHasNext && reader.hasNext();
        }
        return readersHasNext;
    }

    @Override
    public void close() throws IOException {
       for(RecordReader reader : readers)
           reader.close();
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public List<String> getLabels(){
        return null; }



}
