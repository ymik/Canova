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

	private static Logger log = LoggerFactory.getLogger(ImageNetRecordReader.class);
    protected ListIterator<File> iter;
    protected Map<String,String> labelFileIdMap = new LinkedHashMap<>();
    protected final List<String> allowedFormats = Arrays.asList("jpg", "jpeg", "JPG", "JPEG");
    private String labelPath; // "cls-loc-labels.csv"

    public ImageNetRecordReader(int width, int height, int channels, String labelPath) {
        imageLoader = new ImageLoader(width, height, channels);
        this.labelPath = labelPath;
    }

    public ImageNetRecordReader(int width, int height, int channels, boolean appendLabel, String labelPath) {
        imageLoader = new ImageLoader(width, height, channels);
        this.labelPath = labelPath;
        this.appendLabel = appendLabel;
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

    private void defineLabels() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(labelPath));
        String line;

        if (labelFileIdMap.isEmpty()) {
            while ((line = br.readLine()) != null) {
                String row[] = line.split(",");
                labelFileIdMap.put(row[0], row[1]);
            }
        }
        labels = new ArrayList<>(labelFileIdMap.values());
    }

    @Override
    public void initialize(InputSplit split) throws IOException {
        defineLabels();
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
            //remove the root directory
            FileSplit split1 = (FileSplit) split;
            labels.remove(split1.getRootDir());
        }


        else if(split instanceof InputStreamInputSplit) {
            InputStreamInputSplit split2 = (InputStreamInputSplit) split;
            InputStream is =  split2.getIs();
            URI[] locations = split2.locations();
            INDArray load = imageLoader.asRowVector(is);
            record = RecordConverter.toRecord(load);
            for(int i = 0; i < load.length(); i++) {
                if (appendLabel) {
                    Path path = Paths.get(locations[0]);
                    String imgName = path.getParent().toString();
                    if (imgName.contains("/")) {
                        imgName = imgName.substring(imgName.lastIndexOf('/') + 1);
                    }

                    imgName = imgName.split(Pattern.quote("_"))[0];
                    // use file name WNID to find corresponding name in map and index in labels to add to list
                    int labelId = labels.indexOf(labelFileIdMap.get(imgName));
                    if (labelId >= 0)
                        record.add(new DoubleWritable(labelId));
                    else
                        throw new IllegalStateException("Illegal label " + imgName);
                }
            }
            is.close();
        }
    }

    @Override
    public Collection<Writable> next() {
        if(iter != null) {
            Collection<Writable> ret = new ArrayList<>();
            File image = (File) iter.next();

            if(image.isDirectory())
                return next();

            try {
                INDArray row = imageLoader.asRowVector(image);
                ret = RecordConverter.toRecord(row);
                if(appendLabel) {
                    String imgName = FilenameUtils.getBaseName(image.getName()).split(Pattern.quote("_"))[0];
                    int labelId = labels.indexOf(labelFileIdMap.get(imgName));
                    if (labelId >= 0)
                        ret.add(new DoubleWritable(labelId));
                    else
                        throw new IllegalStateException("Illegal label " + imgName);
                }
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
    protected String getLabel(String path) {
        return null;
    }

    @Override
    public void reset() {
        while (iter.hasPrevious())
            iter.previous();
    }

}
