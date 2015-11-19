package org.canova.image.recordreader;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.Text;
import org.canova.api.split.FileSplit;
import org.canova.api.split.InputSplit;
import org.canova.api.split.InputStreamInputSplit;
import org.canova.api.writable.Writable;
import org.canova.common.RecordConverter;
import org.canova.image.loader.ImageLoader;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Record reader to handle ImageNet dataset
 **
 * Built to avoid changing api at this time. Api should change to track labels that are only referenced by id in filename
 * Creates a hashmap for label name to id and references that with filename to generate matching lables.
 */
public class ImageNetRecordReader extends BaseImageRecordReader {

    protected static Logger log = LoggerFactory.getLogger(ImageNetRecordReader.class);
    protected ListIterator<File> iter;
    protected Map<String,String> labelFileIdMap = new LinkedHashMap<>();
    protected Map<String,String> fileNameMap = new LinkedHashMap<>();
    protected final List<String> allowedFormats = Arrays.asList("jpg", "jpeg", "JPG", "JPEG");
    protected String labelPath; // "cls-loc-labels.csv"
    protected String fileNameMapPath = null;
    protected boolean eval = false;

    public ImageNetRecordReader(int width, int height, int channels, String labelPath) {
        imageLoader = new ImageLoader(width, height, channels);
        this.labelPath = labelPath;
    }

    public ImageNetRecordReader(int width, int height, int channels, boolean appendLabel, String labelPath) {
        imageLoader = new ImageLoader(width, height, channels);
        this.labelPath = labelPath;
        this.appendLabel = appendLabel;
    }

    public ImageNetRecordReader(int width, int height, int channels, boolean appendLabel, String labelPath, String fileNameMapPath) {
        imageLoader = new ImageLoader(width, height, channels);
        this.labelPath = labelPath;
        this.appendLabel = appendLabel;
        this.fileNameMapPath = fileNameMapPath;
        this.eval = true;
    }

    @Override
    public List<String> getLabels(){
        return labels; }

    public int numLabels() { return labels.size(); } // 1860

    private boolean containsFormat(String format) {
        for(String format2 : allowedFormats)
            if(format.endsWith("." + format2))
                return true;
        return false;
    }

    private Map<String, String> defineLabels(String path) throws IOException {
        Map<String,String> tmpMap = new LinkedHashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;

        while ((line = br.readLine()) != null) {
            String row[] = line.split(",");
            tmpMap.put(row[0], row[1]);
        }
        return tmpMap;
    }

    @Override
    public void initialize(InputSplit split) throws IOException {
        // creates hashmap with WNID (synset id) as key and first descriptive word in list as the string name
        if (labelPath != null && labelFileIdMap.isEmpty()) {
            labelFileIdMap = defineLabels(labelPath);
            labels = new ArrayList<>(labelFileIdMap.values());
        }
        // creates hasmap with filename as key and WNID(synset id) as value
        if (fileNameMapPath != null && fileNameMap.isEmpty()) {
            fileNameMap = defineLabels(fileNameMapPath);
        }
        if(split instanceof FileSplit) {
            URI[] locations = split.locations();
            if(locations != null && locations.length >= 1) {
                if(locations.length > 1) {
                    List<File> allFiles = new ArrayList<>();
                    for(URI location : locations) {
                        File iter = new File(location);
                        if(!iter.isDirectory() && containsFormat(iter.getAbsolutePath()))
                            allFiles.add(iter);
                    }
                    iter =  allFiles.listIterator();
                }
                else {
                    File curr = new File(locations[0]);
                    if(!curr.exists())
                        throw new IllegalArgumentException("Path " + curr.getAbsolutePath() + " does not exist!");
                    if(curr.isDirectory())
                        iter = (ListIterator) FileUtils.iterateFiles(curr, null, true);
                    else
                        iter =  Collections.singletonList(curr).listIterator();
                }
            }
        } else {
            throw new UnsupportedClassVersionError("Split needs to be an instance of FileSplit for this record reader.");
        }

    }

    @Override
    public Collection<Writable> next() {
        if(iter != null) {
            Collection<Writable> ret = new ArrayList<>();
            File image = iter.next();

            if(image.isDirectory())
                return next();

            try {
                int labelId = -1;
                INDArray row = imageLoader.asRowVector(image);
                ret = RecordConverter.toRecord(row);
                if(appendLabel && fileNameMapPath == null) {
                    String WNID = FilenameUtils.getBaseName(image.getName()).split(Pattern.quote("_"))[0];
                    labelId = labels.indexOf(labelFileIdMap.get(WNID));
                } else if (eval) {
                    String fileName = FilenameUtils.getName(image.getName()); // currently expects file extension
                    labelId = labels.indexOf(labelFileIdMap.get(fileNameMap.get(fileName)));
                }
                if (labelId >= 0)
                    ret.add(new DoubleWritable(labelId));
                else
                    throw new IllegalStateException("Illegal label " + labelId);

            } catch (Exception e) {
                e.printStackTrace();
            }
            if(iter.hasNext()) {
                return ret;
            }
            else {
                if(iter.hasNext()) {
                    try {
                        ret.add(new Text(FileUtils.readFileToString((File) iter.next())));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return ret;
        }
        else if(record != null) {
            hitImage = true;
            return record;
        }
        throw new IllegalStateException("No more elements");
    }

    @Override
    public boolean hasNext() {
        if(iter != null) {
            return iter.hasNext();
        }
        else if(record != null) {
            return !hitImage;
        }
        throw new IllegalStateException("Indeterminant state: record must not be null, or a file iterator must exist");
    }

    @Override
    protected String getLabel(String path) {
        return null;
    }

    @Override
    public void reset() {
        while (iter.hasPrevious())
            iter.previous();
    }

}
