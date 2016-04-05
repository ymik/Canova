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
import javax.imageio.spi.IIORegistry;
import java.awt.image.BufferedImage;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Base class for the image record reader
 *
 * @author Adam Gibson
 */
public abstract class BaseImageRecordReader implements RecordReader {
    protected Iterator<File> iter;
    protected Configuration conf;
    protected File currentFile;
    public List<String> labels  = new ArrayList<>();
    protected boolean appendLabel = false;
    protected Collection<Writable> record;
    protected final List<String> allowedFormats = Arrays.asList("tif", "jpg", "png", "jpeg", "bmp", "JPEG", "JPG", "TIF", "PNG");
    protected boolean hitImage = false;
    protected ImageLoader imageLoader;
    protected InputSplit inputSplit;
    protected Map<String,String> fileNameMap = new LinkedHashMap<>();
    protected String pattern; // Pattern to split and segment file name, pass in regex
    protected int patternPosition = 0;

    public final static String HEIGHT = NAME_SPACE + ".height";
    public final static String WIDTH = NAME_SPACE + ".width";
    public final static String CHANNELS = NAME_SPACE + ".channels";


    static {
        ImageIO.scanForPlugins();
        IIORegistry.getDefaultInstance().registerServiceProvider(new com.twelvemonkeys.imageio.plugins.jpeg.JPEGImageReaderSpi());
        IIORegistry.getDefaultInstance().registerServiceProvider(new com.twelvemonkeys.imageio.plugins.jpeg.JPEGImageWriterSpi());
        IIORegistry.getDefaultInstance().registerServiceProvider(new com.twelvemonkeys.imageio.plugins.psd.PSDImageReaderSpi());
        IIORegistry.getDefaultInstance().registerServiceProvider(Arrays.asList(new com.twelvemonkeys.imageio.plugins.bmp.BMPImageReaderSpi(),
                new com.twelvemonkeys.imageio.plugins.bmp.CURImageReaderSpi(),
                new com.twelvemonkeys.imageio.plugins.bmp.ICOImageReaderSpi()));
    }

    public BaseImageRecordReader() {
    }

    public BaseImageRecordReader(int height, int width, int channels, List<String> labels) {
        this(height, width, channels, false);
        this.labels = labels;
    }

    public BaseImageRecordReader(int height, int width, int channels, boolean appendLabel) {
        this.appendLabel = appendLabel;
        imageLoader = new ImageLoader(height, width, channels);
    }


    public BaseImageRecordReader(int height, int width, int channels, boolean appendLabel, List<String> labels) {
        this(height, width, channels, appendLabel);
        this.labels = labels;
    }

    public BaseImageRecordReader(int height, int width, int channels, boolean appendLabel, String pattern, int patternPosition) {
        this(height, width, channels, appendLabel);
        this.pattern = pattern;
        this.patternPosition = patternPosition;
    }

    public BaseImageRecordReader(int height, int width, int channels, boolean appendLabel, List<String> labels, String pattern, int patternPosition) {
        this(height, width, channels, appendLabel, labels);
        this.pattern = pattern;
        this.patternPosition = patternPosition;
    }

    protected boolean containsFormat(String format) {
        for(String format2 : allowedFormats)
            if(format.endsWith("." + format2))
                return true;
        return false;
    }


    @Override
    public void initialize(InputSplit split) throws IOException{
            inputSplit = split;
            if(split instanceof FileSplit) {
                URI[] locations = split.locations();
                if(locations != null && locations.length >= 1) {
                    if(locations.length > 1) {
                        List<File> allFiles = new ArrayList<>();
                        for(URI location : locations) {
                            File imgFile = new File(location);
                            if(!imgFile.isDirectory() && containsFormat(imgFile.getAbsolutePath()))
                                allFiles.add(imgFile);
                            if(appendLabel){
                                File parentDir = imgFile.getParentFile();
                                String name = parentDir.getName();
                                if(!labels.contains(name))
                                    labels.add(name);
                                if(pattern != null) {
                                    String label = name.split(pattern)[patternPosition];
                                    fileNameMap.put(imgFile.toString(), label);
                                }
                            }
                        }
                        iter = allFiles.listIterator();
                    }
                    else {
                        File curr = new File(locations[0]);
                        if(!curr.exists())
                            throw new IllegalArgumentException("Path " + curr.getAbsolutePath() + " does not exist!");
                        if(curr.isDirectory())
                            iter =  FileUtils.iterateFiles(curr, null, true);
                        else
                            iter = Collections.singletonList(curr).listIterator();

                    }
                }
                //remove the root directory
                FileSplit split1 = (FileSplit) split;
                labels.remove(split1.getRootDir());
            }


            else if(split instanceof InputStreamInputSplit) {
                InputStreamInputSplit split2 = (InputStreamInputSplit) split;
                InputStream is = split2.getIs();
                URI[] locations = split2.locations();
                INDArray load = imageLoader.asRowVector(is);
                record = RecordConverter.toRecord(load);
                for (int i = 0; i < load.length(); i++) {
                    if (appendLabel) {
                        Path path = Paths.get(locations[0]);
                        String parent = path.getParent().toString();
                        //could have been a uri
                        if (parent.contains("/")) {
                            parent = parent.substring(parent.lastIndexOf('/') + 1);
                        }
                        int label = labels.indexOf(parent);
                        if (label >= 0)
                            record.add(new DoubleWritable(labels.indexOf(parent)));
                        else
                            throw new IllegalStateException("Illegal label " + parent);
                    }
                }
                is.close();
            }
    }



    @Override
    public void initialize(Configuration conf, InputSplit split) throws IOException, InterruptedException {
        this.appendLabel = conf.getBoolean(APPEND_LABEL,false);
        this.labels = new ArrayList<>(conf.getStringCollection(LABELS));
        imageLoader = new ImageLoader(conf.getInt(HEIGHT,28), conf.getInt(WIDTH,28),conf.getInt(CHANNELS,1));
        this.conf = conf;
        initialize(split);
    }


    @Override
    public Collection<Writable> next() {
        if(iter != null) {
            Collection<Writable> ret = new ArrayList<>();
            File image = (File) iter.next();
            currentFile = image;

            if(image.isDirectory())
                return next();
            try {
                BufferedImage bimg = ImageIO.read(image);
                INDArray row = imageLoader.asRowVector(bimg);
                ret = RecordConverter.toRecord(row);
                if(appendLabel)
                    ret.add(new DoubleWritable(labels.indexOf(image.getParentFile().getName())));
            } catch (Exception e) {
                e.printStackTrace();
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


    /**
     * Get the label from the given path
     * @param path the path to get the label from
     * @return the label for the given path
     */
    public String getLabel(String path) {
        if(fileNameMap != null && fileNameMap.containsKey(path)) return fileNameMap.get(path);
        return (new File(path)).getParentFile().getName();
    }

    /**
     * Accumulate the label from the path
     * @param path the path to get the label from
     */
    protected void accumulateLabel(String path) {
        String name = getLabel(path);
        if(!labels.contains(name))
            labels.add(name);
    }

    public File getCurrentFile() {
        return currentFile;
    }

    public void setCurrentFile(File currentFile) {
        this.currentFile = currentFile;
    }

    @Override
    public List<String> getLabels(){
        return labels; }

    @Override
    public void reset() {
        if(inputSplit == null) throw new UnsupportedOperationException("Cannot reset without first initializing");
        try{
            initialize(inputSplit);
        }catch(Exception e){
            throw new RuntimeException("Error during LineRecordReader reset",e);
        }
    }

    public int numLabels() { return labels.size(); }

    @Override
    public Collection<Writable> record(URI uri, DataInputStream dataInputStream ) throws IOException {
        BufferedImage bimg = ImageIO.read(dataInputStream);
        INDArray row = imageLoader.asRowVector(bimg);
        Collection<Writable> ret = RecordConverter.toRecord(row);
        if(appendLabel) ret.add(new DoubleWritable(labels.indexOf(getLabel(uri.getPath()))));
        return ret;
    }

}
