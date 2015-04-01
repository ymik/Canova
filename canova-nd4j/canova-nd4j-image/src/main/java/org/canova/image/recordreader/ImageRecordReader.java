package org.canova.image.recordreader;

import org.apache.commons.io.FileUtils;
import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.Text;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.InputSplit;
import org.canova.api.writable.Writable;
import org.canova.image.loader.ImageLoader;
import org.nd4j.linalg.api.ndarray.INDArray;

import java.io.File;
import java.io.IOException;
import java.net.URI;
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
public class ImageRecordReader implements RecordReader {
    private Iterator<File> iter;
    private ImageLoader imageLoader;
    private List<String> labels  = new ArrayList<>();
    private boolean appendLabel = true;
    public ImageRecordReader(int width, int height) {
        this(width, height,true);

    }

    public ImageRecordReader(int width, int height,boolean appendLabel) {
        this.appendLabel = appendLabel;
        imageLoader = new ImageLoader(width,height);

    }

    @Override
    public void initialize(InputSplit split) throws IOException, InterruptedException {
        URI[] locations = split.locations();
        if(locations != null && locations.length >= 1) {
            if(locations.length > 1) {
                List<File> allFiles = new ArrayList<>();
                for(URI location : locations) {
                    File iter = new File(location);
                    if(!iter.isDirectory())
                        allFiles.add(iter);
                    if(appendLabel) {
                        File parentDir = iter.getParentFile();
                        String name = parentDir.getName();
                        if(!labels.contains(name))
                            labels.add(name);

                    }

                }



                iter = allFiles.iterator();
            }
            else {
                File curr = new File(locations[0]);
                if(!curr.exists())
                    throw new IllegalArgumentException("Path " + curr.getAbsolutePath() + " does not exist!");
                if(curr.isDirectory())
                    iter = FileUtils.iterateFiles(curr,null,true);
                else
                    iter = Collections.singletonList(curr).iterator();
            }
        }


    }

    @Override
    public Collection<Writable> next() {
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

    @Override
    public boolean hasNext() {
        return iter != null && iter.hasNext();
    }

    @Override
    public void close() throws IOException {

    }
}
