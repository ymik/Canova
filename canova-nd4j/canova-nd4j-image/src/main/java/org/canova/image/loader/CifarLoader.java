package org.canova.image.loader;

import org.apache.commons.lang3.NotImplementedException;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.LimitFileSplit;
import org.canova.image.recordreader.ImageRecordReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Reference: Learning Multiple Layers of Features from Tiny Images, Alex Krizhevsky, 2009.
 * Created by nyghtowl on 12/17/15.
 */
public class CifarLoader extends BaseImageLoader {

    public final static int NUM_TRAIN_IMAGES = 50000;
    public final static int NUM_TEST_IMAGES = 10000;
    public final static int NUM_LABELS = 10; // 6000 imgs per class
    public final static int WIDTH = 32;
    public final static int HEIGHT = 32;
    public final static int CHANNELS = 3;
//    public String dataUrl = "https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz"; // TODO use cPickle to unload
//    public String dataFile = "cifar-10-python";
    public String dataBinUrl = "https://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz";
    public String dataBinFile = "cifar-10-binary";
    protected String labelFileName = "batches.meta.txt ";
    protected List<String> labels = new ArrayList<>();
    protected Map<String,String> labelIdMap = new LinkedHashMap<>();

    String[] fileNames = {"data_batch_1.bin", "data_batch_2.bin", "data_batch_3.bin", "data_batch_4.bin", "data_batch5.bin", "test_batch.bin"};

    public String localDir = "cifar";
    protected File fullDir = new File(BASE_DIR, localDir);
    protected String regexPattern;
    protected int numExamples = NUM_TRAIN_IMAGES;
    protected int numLabels = NUM_LABELS;

    public static Map<String, String> cifarTrainData = new HashMap<>();

    public CifarLoader(String localDir){
        this.localDir = localDir;
        this.fullDir = new File(localDir);
        load();
    }

    public CifarLoader(){
        load();
    }

    public void generateMaps() {
        cifarTrainData.put("filesFilename", new File(dataBinUrl).getName());
        cifarTrainData.put("filesURL", dataBinUrl);
        cifarTrainData.put("filesFilenameUnzipped", dataBinFile);
    }

    private void defineLabels() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(labelFileName));
            String line;

            while ((line = br.readLine()) != null) {
                String row[] = line.split(",");
                labelIdMap.put(row[0], row[1]);
                labels.add(row[1]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void load()  {
        generateMaps();
        defineLabels();
        if (!fullDir.exists()) {
            fullDir.mkdir();

            log.info("Downloading {}...", localDir);
            downloadAndUntar(cifarTrainData, fullDir);
        }
    }

    public boolean imageFilesExist(){
        //Check 4 files:
        File f = new File(BASE_DIR, cifarTrainData.get("filesFilenameUnzipped"));
        if (!f.exists()) return false;
        return true;
    }

    public RecordReader getRecordReader() {
        return getRecordReader(WIDTH, HEIGHT, CHANNELS, false, regexPattern);
    }

    public RecordReader getRecordReader(int width, int height, int channels) {
        return getRecordReader(width, height, channels, false, regexPattern);
    }

    public RecordReader getRecordReader(int numExamples) {
        this.numExamples = numExamples;
        return getRecordReader(WIDTH, HEIGHT, CHANNELS, false, regexPattern);
    }

    public RecordReader getRecordReader(int numExamples, int numCategories) {
        this.numExamples = numExamples;
        this.numLabels = numCategories;
        return getRecordReader(WIDTH, HEIGHT, CHANNELS, false, regexPattern);
    }

    public RecordReader getRecordReader(int width, int height, int channels, int numExamples) {
        this.numExamples = numExamples;
        return getRecordReader(width, height, channels, false, regexPattern);
    }


    // TODO setup to pull data from binary record reader
    public RecordReader getRecordReader(int width, int height, int channels, boolean appendLabel, String regexPattern) {
        throw new NotImplementedException("Cifar loader is still development. Pull content directly from https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz for now.");

//        RecordReader recordReader = new ImageRecordReader(width, height, channels, appendLabel, labels);
//        try {
//            recordReader.initialize(new LimitFileSplit(fullDir, ALLOWED_FORMATS, numExamples, numLabels, null, rng));
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        return recordReader;
    }
}
