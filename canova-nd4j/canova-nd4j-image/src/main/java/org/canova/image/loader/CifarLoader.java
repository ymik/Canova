package org.canova.image.loader;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
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
    public String dataUrl = "https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz"; // used for python version - similar structure to datBin structure
    public String dataFile = "cifar-10-python";
    public String dataBinUrl = "https://www.cs.toronto.edu/~kriz/cifar-10-binary.tar.gz";
    public String dataBinFile = "cifar-10-batches-bin";
    protected String labelFileName = "batches.meta.txt";
    protected List<String> labels = new ArrayList<>();

    protected String[] trainFileNames = {"data_batch_1.bin", "data_batch_2.bin", "data_batch_3.bin", "data_batch_4.bin", "data_batch5.bin"};
    protected String testFileName = "test_batch.bin";

    public String localDir = "cifar";
    protected File fullDir = new File(BASE_DIR, localDir);
    protected String regexPattern;
    protected int numExamples = NUM_TRAIN_IMAGES;
    protected int numLabels = NUM_LABELS;
    protected String version = "TRAIN"; // make either TRAIN or TEST to different types of files loaded

    public static Map<String, String> cifarTrainData = new HashMap<>();

    public CifarLoader(String version){
        this.version = version;
    }

    public CifarLoader(String version, String localDir){
        this.version = version;
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
            File path = new File(fullDir, FilenameUtils.concat(dataBinFile, labelFileName));
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line;

            while ((line = br.readLine()) != null) {
                labels.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void load()  {
        defineLabels();
        if (!imageFilesExist() && !fullDir.exists()) {
            generateMaps();
            fullDir.mkdir();

            log.info("Downloading {}...", localDir);
            downloadAndUntar(cifarTrainData, fullDir);
        }
    }

    public boolean imageFilesExist(){
        File f = new File(fullDir, FilenameUtils.concat(dataBinFile, testFileName));
        if (!f.exists()) return false;

        for(String name: trainFileNames) {
            f = new File(fullDir, FilenameUtils.concat(dataBinFile, name));
            if (!f.exists()) return false;
        }
        return true;
    }

    public InputStream getInputStream() {
//        throw new NotImplementedException("Cifar loader is still development. Pull content directly from https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz for now.");
        load();
        InputStream in = null;

        try {
            // Create inputStream
            switch (version) {
                case "TRAIN":
                    Collection<File> subFiles = FileUtils.listFiles(new File(fullDir, dataBinFile), new String[] {"bin"}, true);
                    Iterator trainIter = subFiles.iterator();
                    in = new SequenceInputStream(new FileInputStream((File) trainIter.next()), new FileInputStream((File) trainIter.next()));
                    while (trainIter.hasNext()) {
                        File nextFile = (File) trainIter.next();
                        if(!testFileName.equals(nextFile.getName()))
                            in = new SequenceInputStream(in, new FileInputStream(nextFile));
                    }
                    break;
                case "TEST":
                    in = new FileInputStream(new File(fullDir, FilenameUtils.concat(dataBinFile, testFileName)));

            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return in;
    }

    public List<String> getLabels(){
        return labels;
    }
}
