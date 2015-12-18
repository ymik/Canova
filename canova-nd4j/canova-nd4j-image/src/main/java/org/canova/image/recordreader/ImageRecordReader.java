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


import java.io.File;
import java.util.*;

/**
 * Image record reader.
 * Reads a local file system and parses images of a given
 * width and height.
 *
 * Also appends the label if specified
 * (one of k encoding based on the directory structure where each subdir of the root is an indexed label)
 * @author Adam Gibson
 */
public class ImageRecordReader extends BaseImageRecordReader {


    public ImageRecordReader() {
        super();
    }

    public ImageRecordReader(int width, int height, int channels, List<String> labels) {
        super(width, height, channels, labels);
    }

    public ImageRecordReader(int width, int height, int channels, boolean appendLabel, List<String> labels) {
        super(width, height, channels, appendLabel, labels);
    }

    public ImageRecordReader(int width, int height, int channels) {
        super(width, height, channels, false);
    }

    public ImageRecordReader(int width, int height, int channels, boolean appendLabel) {
        super(width, height, channels, appendLabel);
    }

    public ImageRecordReader(int width, int height, List<String> labels) {
        super(width, height, 1, labels);
    }

    public ImageRecordReader(int width, int height, boolean appendLabel, List<String> labels) {
        super(width, height, 1, appendLabel, labels);
    }

    public ImageRecordReader(int width, int height) {
        super(width, height, 1, false);
    }

    public ImageRecordReader(int width, int height, boolean appendLabel) {
        super(width, height, 1, appendLabel);
    }


    public ImageRecordReader(int width, int height, int channels, boolean appendLabel, String pattern) {
        super(width, height, channels, appendLabel, pattern, 0);
    }


    @Override
    public String getLabel(String path) {
        return new File(path).getParentFile().getName();
    }


}
