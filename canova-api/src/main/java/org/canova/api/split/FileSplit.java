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

package org.canova.api.split;

import org.apache.commons.io.FileUtils;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;

/**
 * File input split. Splits up a root directory in to files.
 * @author Adam Gibson
 */
public class FileSplit extends BaseInputSplit {

    private File rootDir;
    public FileSplit(File rootDir) {
        if(rootDir == null && rootDir.exists())
            throw new IllegalArgumentException("File must not be null");

        this.rootDir = rootDir;
        if(rootDir.isDirectory()) {
            Collection<File> subFiles = FileUtils.listFiles(rootDir, null, true);
            locations = new URI[subFiles.size()];
            int count = 0;
            for(File f : subFiles) {
                if(f.getPath().startsWith("file:"))
                    locations[count++] = URI.create(f.getPath());
                else
                    locations[count++] = f.toURI();
                length += f.length();
            }
        }
        else {
            String path = rootDir.getPath();
            locations = new URI[1];
            if(path.startsWith("file:"))
                 locations[0] = URI.create(path);
            else
                locations[0] = rootDir.toURI();
            length += rootDir.length();

        }

    }


    @Override
    public long length() {
        return length;
    }


    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    public File getRootDir() {
        return rootDir;
    }
}


