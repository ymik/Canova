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

import org.apache.commons.io.FileUtils;
import org.canova.api.conf.Configuration;
import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.Text;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.FileSplit;
import org.canova.api.split.InputSplit;
import org.canova.api.split.InputStreamInputSplit;
import org.canova.api.writable.Writable;
import org.canova.common.RecordConverter;
import org.canova.image.loader.ImageLoader;
import org.nd4j.linalg.api.ndarray.INDArray;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageReaderSpi;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private boolean appendLabel = false;
    private Collection<Writable> record;
    private final List<String> allowedFormats = Arrays.asList("tif","jpg","png","jpeg","bmp");
    private boolean hitImage = false;
    private Configuration conf;
    public final static String WIDTH = NAME_SPACE + ".width";
    public final static String HEIGHT = NAME_SPACE + ".width";

    static {
        ImageIO.scanForPlugins();
        IIORegistry.getDefaultInstance().registerServiceProvider(new com.twelvemonkeys.imageio.plugins.jpeg.JPEGImageReaderSpi());
        IIORegistry.getDefaultInstance().registerServiceProvider(new com.twelvemonkeys.imageio.plugins.jpeg.JPEGImageWriterSpi());
        IIORegistry.getDefaultInstance().registerServiceProvider(new com.twelvemonkeys.imageio.plugins.psd.PSDImageReaderSpi());
        IIORegistry.getDefaultInstance().registerServiceProvider(Arrays.asList(new com.twelvemonkeys.imageio.plugins.bmp.BMPImageReaderSpi(),
                new com.twelvemonkeys.imageio.plugins.bmp.CURImageReaderSpi(),
                new com.twelvemonkeys.imageio.plugins.bmp.ICOImageReaderSpi()));
    }

    public ImageRecordReader() {
    }

    /**
     * Load the record reader with the given width and height
     * @param width the width load
     * @param height the height to load
     */
    public ImageRecordReader(int width, int height,List<String> labels) {
        this(width, height,false);
        this.labels = labels;

    }

    public ImageRecordReader(int width, int height,boolean appendLabel,List<String> labels) {
        this.appendLabel = appendLabel;
        imageLoader = new ImageLoader(width,height);
        this.labels = labels;

    }

    /**
     * Load the record reader with the given width and height
     * @param width the width load
     * @param height the height to load
     */
    public ImageRecordReader(int width, int height) {
        this(width, height,false);

    }

    public ImageRecordReader(int width, int height,boolean appendLabel) {
        this.appendLabel = appendLabel;
        imageLoader = new ImageLoader(width,height);

    }

    private boolean containsFormat(String format) {
        for(String format2 : allowedFormats)
            if(format.endsWith("." + format2))
                return true;
        return false;
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
                        if(!iter.isDirectory() && containsFormat(iter.getAbsolutePath()))
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
    public void initialize(Configuration conf, InputSplit split) throws IOException, InterruptedException {
        this.appendLabel = conf.getBoolean(APPEND_LABEL,false);
        this.labels = new ArrayList<>(conf.getStringCollection(LABELS));
        imageLoader = new ImageLoader(conf.getInt(WIDTH,28),conf.getInt(HEIGHT,28));
        this.conf = conf;
        initialize(split);
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
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
