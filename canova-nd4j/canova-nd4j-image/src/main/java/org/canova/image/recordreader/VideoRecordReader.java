package org.canova.image.recordreader;

import org.apache.commons.io.FileUtils;
import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.Text;
import org.canova.api.records.reader.SequenceRecordReader;
import org.canova.api.split.FileSplit;
import org.canova.api.split.InputSplit;
import org.canova.api.split.InputStreamInputSplit;
import org.canova.api.writable.Writable;
import org.canova.common.RecordConverter;
import org.canova.image.loader.ImageLoader;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 *
 * A video is just a moving window of pictures.
 * It should be processed as cuh.
 * This iterates over a root folder and returns a
 *
 * @author Adam Gibson
 *
 */
public class VideoRecordReader implements SequenceRecordReader {
    private Iterator<File> iter;
    private ImageLoader imageLoader;
    private List<String> labels  = new ArrayList<>();
    private boolean appendLabel = false;
    private Collection<Writable> record;
    private boolean hitImage = false;



    /**
     * Load the record reader with the given width and height
     * @param width the width load
     * @param height the height to load
     */
    public VideoRecordReader(int width, int height,List<String> labels) {
        this(width, height,false);
        this.labels = labels;

    }

    public VideoRecordReader(int width, int height,boolean appendLabel,List<String> labels) {
        this.appendLabel = appendLabel;
        imageLoader = new ImageLoader(width,height);
        this.labels = labels;

    }

    /**
     * Load the record reader with the given width and height
     * @param width the width load
     * @param height the height to load
     */
    public VideoRecordReader(int width, int height) {
        this(width, height,false);

    }

    public VideoRecordReader(int width, int height,boolean appendLabel) {
        this.appendLabel = appendLabel;
        imageLoader = new ImageLoader(width,height);

    }

    @Override
    public void initialize(InputSplit split) throws IOException, InterruptedException {
        if(split instanceof FileSplit) {
            URI[] locations = split.locations();
            if(locations != null && locations.length >= 1) {
                if(locations.length > 1) {
                    List<File> allFiles = new ArrayList<>();
                    for(URI location : locations) {
                        File iter = new File(location);
                        if(iter.isDirectory()) {
                            allFiles.add(iter);
                            if (appendLabel) {
                                File parentDir = iter.getParentFile();
                                String name = parentDir.getName();
                                if (!labels.contains(name))
                                    labels.add(name);

                            }
                        }

                    }



                    iter = allFiles.iterator();
                }
                else {
                    File curr = new File(locations[0]);
                    if(!curr.exists())
                        throw new IllegalArgumentException("Path " + curr.getAbsolutePath() + " does not exist!");
                    if(curr.isDirectory())
                        iter = FileUtils.iterateFiles(curr, null, true);
                    else
                        iter = Collections.singletonList(curr).iterator();
                }
            }
        }


        else if(split instanceof InputStreamInputSplit) {
            InputStreamInputSplit split2 = (InputStreamInputSplit) split;
            InputStream is =  split2.getIs();
            URI[] locations = split2.locations();
            INDArray load = imageLoader.asMatrix(is);
            record = RecordConverter.toRecord(load);
            if(appendLabel) {
                Path path = Paths.get(locations[0]);
                String parent = path.getParent().toString();
                record.add(new DoubleWritable(labels.indexOf(parent)));
            }

            is.close();
        }




    }

    @Override
    public Collection<Writable> next() {
        if(iter != null) {
            List<Writable> ret = new ArrayList<>();
            File image = iter.next();
            if(image.isDirectory())
                return next();
            try {
                INDArray row = imageLoader.asRowVector(image);
                for(int i = 0; i < row.length(); i++)
                    ret.add(new DoubleWritable(row.getDouble(i)));
                if(appendLabel)
                    ret.add(new DoubleWritable(labels.indexOf(image.getParentFile().getName())));
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(iter.hasNext()) {
                return ret;
            }
            else {
                if(iter.hasNext()) {
                    try {
                        ret.add(new Text(FileUtils.readFileToString(iter.next())));
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
    public void close() throws IOException {

    }

    @Override
    public Collection<Collection<Writable>> sequenceRecord() {
        File next = iter.next();
        File[] list = next.listFiles();
        Collection<Collection<Writable>> ret = new ArrayList<>();
        for(File f : list) {
            try {
                Collection<Writable> record = RecordConverter.toRecord(imageLoader.asRowVector(f));
                ret.add(record);
                if(appendLabel)
                    record.add(new DoubleWritable(labels.indexOf(next.getName())));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
        return ret;
    }
}
