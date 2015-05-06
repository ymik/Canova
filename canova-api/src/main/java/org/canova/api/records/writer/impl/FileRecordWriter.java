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

package org.canova.api.records.writer.impl;


import org.canova.api.io.data.Text;
import org.canova.api.records.writer.RecordWriter;
import org.canova.api.writable.Writable;

import java.io.*;
import java.util.Collection;

/**
 * Write to files
 * @author Adam Gibson
 */
public  class FileRecordWriter implements RecordWriter {

    protected File writeTo;
    protected DataOutputStream out;
    public final static String NEW_LINE = "\n";
    private boolean append;
    public FileRecordWriter(File path) throws FileNotFoundException {
        this.writeTo = path;
        out = new DataOutputStream(new FileOutputStream(writeTo,append));
    }
    public FileRecordWriter(File path,boolean append) throws FileNotFoundException {
        this.writeTo = path;
        this.append = append;
        out = new DataOutputStream(new FileOutputStream(writeTo,append));
    }

    @Override
    public void write(Collection<Writable> record) throws IOException {
        if(!record.isEmpty()) {
            Text t = (Text) record.iterator().next();
            t.write(out);
        }
    }

    @Override
    public void close() {
        if(out != null) {
            try {
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}
