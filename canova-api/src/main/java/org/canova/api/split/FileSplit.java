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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;


import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.regex.Pattern;

/**
 * File input split. Splits up a root directory in to files.
 * @author Adam Gibson
 */
public class FileSplit extends BaseInputSplit {

    private File rootDir;
    private String[] allowFormat = null;
    private boolean recursive = true;
    private int numExamples = 0;
    private int i = 0;
    private String currentName;
    private String pattern;
    private int patternPosition = 0;
    private long seed = System.currentTimeMillis();
    private boolean shuffle = false;


    public FileSplit(File rootDir) {
        this.rootDir = rootDir;
        this.initialize();
    }

    public FileSplit(File rootDir, boolean recursive) {
        this.recursive = recursive;
        this.rootDir = rootDir;
        this.initialize();
    }

    public FileSplit(File rootDir, String[] allowFormat) {
        this.allowFormat = allowFormat;
        this.rootDir = rootDir;
        this.initialize();
    }

    public FileSplit(File rootDir, long seed) {
        this.rootDir = rootDir;
        this.seed = seed;
        this.shuffle = true;
        this.initialize();
    }

    public FileSplit(File rootDir, boolean recursive, long seed) {
        this.recursive = recursive;
        this.rootDir = rootDir;
        this.seed = seed;
        this.shuffle = true;
        this.initialize();
    }

    public FileSplit(File rootDir, String[] allowFormat, long seed) {
        this.allowFormat = allowFormat;
        this.rootDir = rootDir;
        this.seed = seed;
        this.shuffle = true;
        this.initialize();
    }


    public FileSplit(File rootDir, int numExamples) {
        this.rootDir = rootDir;
        this.numExamples = numExamples;
        this.initialize();
    }

    public FileSplit(File rootDir, String[] allowFormat, boolean recursive) {
        this.allowFormat = allowFormat;
        this.recursive = recursive;
        this.rootDir = rootDir;
        this.initialize();
    }

    public FileSplit(File rootDir, String[] allowFormat, boolean recursive, int numExamples) {
        this.allowFormat = allowFormat;
        this.recursive = recursive;
        this.rootDir = rootDir;
        this.numExamples = numExamples;
        this.initialize();
    }

    public FileSplit(File rootDir, int numExamples, String pattern) {
        this.rootDir = rootDir;
        this.numExamples = numExamples;
        this.pattern = pattern;
        this.initialize();
    }

    public FileSplit(File rootDir, String[] allowFormat, boolean recursive, int numExamples, String pattern) {
        this.allowFormat = allowFormat;
        this.recursive = recursive;
        this.rootDir = rootDir;
        this.numExamples = numExamples;
        this.pattern = pattern;
        this.initialize();
    }

    public FileSplit(File rootDir, String[] allowFormat, boolean recursive, int numExamples, String pattern, int patternPosition) {
        this.allowFormat = allowFormat;
        this.recursive = recursive;
        this.rootDir = rootDir;
        this.numExamples = numExamples;
        this.pattern = pattern;
        this.patternPosition = patternPosition;
        this.initialize();
    }


    public FileSplit(File rootDir, int numExamples, String pattern, long seed) {
        this.rootDir = rootDir;
        this.numExamples = numExamples;
        this.pattern = pattern;
        this.seed = seed;
        this.shuffle = true;
        this.initialize();
    }

    public FileSplit(File rootDir, String[] allowFormat, boolean recursive, int numExamples, String pattern, long seed) {
        this.allowFormat = allowFormat;
        this.recursive = recursive;
        this.rootDir = rootDir;
        this.numExamples = numExamples;
        this.pattern = pattern;
        this.seed = seed;
        this.shuffle = true;
        this.initialize();
    }

    public FileSplit(File rootDir, String[] allowFormat, boolean recursive, int numExamples, String pattern, int patternPosition, long seed) {
        this.allowFormat = allowFormat;
        this.recursive = recursive;
        this.rootDir = rootDir;
        this.numExamples = numExamples;
        this.pattern = pattern;
        this.patternPosition = patternPosition;
        this.seed = seed;
        this.shuffle = true;
        this.initialize();
    }

    private void initialize() {
        Collection<File> subFiles;

        if(rootDir == null && rootDir.exists())
            throw new IllegalArgumentException("File must not be null");

        if(rootDir.isDirectory()) {
            // Limits number files listed will pull set number from each directory
            if(numExamples > 0){
                Iterator iter = FileUtils.iterateFiles(rootDir, allowFormat, recursive);
                subFiles = new ArrayList<>();

                File root = new File(rootDir.toString());
                File[] numDirs = root.listFiles(new FileFilter() {
                    @Override
                    public boolean accept(File f) {
                        return f.isDirectory();
                    }
                });

                int numExamplesPerDir =  (numDirs.length > 0) ? numExamples/numDirs.length : numExamples;

                File f;
                String name = null;
                while(iter.hasNext()){
                    f = (File) iter.next();
                    if(!pattern.isEmpty()) {
                        name = FilenameUtils.getBaseName(f.getName()).split(pattern)[patternPosition];
                        if (i == 0) currentName = name;
                    }
                    if(f.isFile() && currentName.equals(name) && i < numExamplesPerDir) {
                        subFiles.add(f);
                        i++;
                    }
                    // Will reset for multiple directories otherwise limit size to one
                    else if(!currentName.equals(name))
                        i = 0;
                    else
                        System.out.print(f.toString() + "  only has " + i + " examples when " + numExamplesPerDir + " are expected.");
                }
            } else
                // Includes all files in the root path including subdirectories
                subFiles = FileUtils.listFiles(rootDir, allowFormat, recursive);
            locations = new URI[subFiles.size()];

            if (shuffle) Collections.shuffle((List<File>) subFiles, new Random(seed));
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
            // Lists one file
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




