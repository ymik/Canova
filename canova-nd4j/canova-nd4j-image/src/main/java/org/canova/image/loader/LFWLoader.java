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

package org.canova.image.loader;


import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.LimitFileSplit;
import org.canova.image.recordreader.ImageRecordReader;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Loads LFW faces data transform. You can customize the size of the images as well
 * @author Adam Gibson
 *
 */
public class LFWLoader extends BaseImageLoader{

    public final static int NUM_IMAGES = 13233;
    public final static int NUM_LABELS = 1680;
    public final static int SUB_NUM_IMAGES = 1054;
    public final static int SUB_NUM_LABELS = 432;
    public final static int WIDTH = 250;
    public final static int HEIGHT = 250;
    public final static int CHANNELS = 3;
    public final static String DATA_URL = "http://vis-www.cs.umass.edu/lfw/lfw.tgz";
    public final static String LABEL_URL =  "http://vis-www.cs.umass.edu/lfw/lfw-names.txt";
    public final static String SUBSET_URL = "http://vis-www.cs.umass.edu/lfw/lfw-a.tgz";

    public String dataFile = "lfw";
    public String labelFile = "lfw-names.txt";
    public String subsetFile = "lfw-a";

    public String localDir = "lfw";
    public String localSubDir = "lfw-a/lfw";
    protected File fullDir = new File(BASE_DIR, localDir);
    protected String regexPattern = ".[0-9]+";
    protected boolean useSubset = false;
    protected int numExamples = NUM_IMAGES;
    protected int numLabels = NUM_LABELS;

    public static Map<String, String> lfwData = new HashMap<>();
    public static Map<String, String> lfwLabel = new HashMap<>();
    public static Map<String, String> lfwSubsetData = new HashMap<>();


    public LFWLoader(String localDir, boolean useSubset){
        this.localDir = localDir;
        this.fullDir = new File(localDir);
        this.useSubset = useSubset;
        if (useSubset) {
            this.numExamples = SUB_NUM_IMAGES;
            this.numLabels = SUB_NUM_LABELS;
        }
        generateLfwMaps();
        if (!imageFilesExist()) load();
    }

    public LFWLoader(boolean useSubset){
        this.useSubset = useSubset;
        if (useSubset) {
            this.fullDir = new File(BASE_DIR, localSubDir);
            this.numExamples = SUB_NUM_IMAGES;
            this.numLabels = SUB_NUM_LABELS;
        }
        generateLfwMaps();
        if (!imageFilesExist()) load();
    }

    public LFWLoader(String path){
        this(path, false);
    }

    public LFWLoader(){this(false);}


    public void generateLfwMaps() {
        if(useSubset) {
            // Subset of just faces with a name starting with A
            lfwSubsetData.put("filesFilename", new File(SUBSET_URL).getName());
            lfwSubsetData.put("filesURL", SUBSET_URL);
            lfwSubsetData.put("filesFilenameUnzipped", subsetFile);

        } else {
            lfwData.put("filesFilename", new File(DATA_URL).getName());
            lfwData.put("filesURL", DATA_URL);
            lfwData.put("filesFilenameUnzipped", dataFile);

            lfwLabel.put("filesFilename", labelFile);
            lfwLabel.put("filesURL",LABEL_URL);
            lfwLabel.put("filesFilenameUnzipped", labelFile);
        }

    }

    public void load()  {
        if (!fullDir.exists()) {
            fullDir.mkdir();

            if (useSubset) {
                log.info("Downloading {} subset...", localDir);
                downloadAndUntar(lfwSubsetData, fullDir);
            }
            else {
                log.info("Downloading {}...", localDir);
                downloadAndUntar(lfwData, fullDir);
                downloadAndUntar(lfwLabel, fullDir);
            }
        }
    }

    public boolean imageFilesExist(){
        //Check 4 files:
        if(useSubset){
            File f = new File(BASE_DIR, lfwSubsetData.get("filesFilenameUnzipped"));
            if (!f.exists()) return false;
        } else {
            File f = new File(BASE_DIR, lfwData.get("filesFilenameUnzipped"));
            if (!f.exists()) return false;
            f = new File(BASE_DIR, lfwLabel.get("filesFilenameUnzipped"));
            if (!f.exists()) return false;
        }
        return true;
    }


    public RecordReader getRecordReader() {
        return getRecordReader(WIDTH, HEIGHT, CHANNELS, true, regexPattern);
    }

    public RecordReader getRecordReader(int width, int height, int channels) {
        return getRecordReader(width, height, channels, true, regexPattern);
    }

    public RecordReader getRecordReader(int numExamples) {
        this.numExamples = numExamples;
        return getRecordReader(WIDTH, HEIGHT, CHANNELS, true, regexPattern);
    }

    public RecordReader getRecordReader(int numExamples, int numCategories) {
        this.numExamples = numExamples;
        this.numLabels = numCategories;
        return getRecordReader(WIDTH, HEIGHT, CHANNELS, true, regexPattern);
    }

    public RecordReader getRecordReader(int width, int height, int channels, int numExamples) {
        this.numExamples = numExamples;
        return getRecordReader(width, height, channels, true, regexPattern);
    }

    public RecordReader getRecordReader(int width, int height, int channels, int numExamples, Random rng) {
        this.numExamples = numExamples;
        this.rng = rng;
        return getRecordReader(width, height, channels, true, regexPattern);
    }

    public RecordReader getRecordReader(int width, int height, int channels, boolean appendLabel, String regexPattern) {
        if (!imageFilesExist()) load();
        RecordReader recordReader = new ImageRecordReader(width, height, channels, appendLabel, regexPattern);
        try {
            recordReader.initialize(new LimitFileSplit(fullDir, ALLOWED_FORMATS, numExamples, numLabels, regexPattern, rng));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return recordReader;
    }

}
