package org.canova.sound.recordreader;

import org.apache.commons.io.FileUtils;
import org.canova.api.io.data.DoubleWritable;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.FileSplit;
import org.canova.api.split.InputSplit;
import org.canova.api.split.InputStreamInputSplit;
import org.canova.api.util.RecordUtils;
import org.canova.api.writable.Writable;
import org.canova.sound.musicg.Wave;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Wav file loader
 * @author Adam Gibson
 */
public class WavFileRecordReader implements RecordReader {
    private Iterator<File> iter;
    private Collection<Writable> record;
    private boolean hitImage = false;
    private boolean appendLabel = false;
    private List<String> labels = new ArrayList<>();

    public WavFileRecordReader(boolean appendLabel, List<String> labels) {
        this.appendLabel = appendLabel;
        this.labels = labels;
    }

    public WavFileRecordReader(List<String> labels) {
        this.labels = labels;
    }

    public WavFileRecordReader(boolean appendLabel) {
        this.appendLabel = appendLabel;
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
                            Iterator<File> allFiles2 = FileUtils.iterateFiles(iter, null, true);
                            while(allFiles2.hasNext())
                                allFiles.add(allFiles2.next());
                        }

                        else
                            allFiles.add(iter);
                    }

                    iter = allFiles.iterator();
                }
                else {
                    File curr = new File(locations[0]);
                    if(curr.isDirectory())
                        iter = FileUtils.iterateFiles(curr,null,true);
                    else
                        iter = Collections.singletonList(curr).iterator();
                }
            }
        }


        else if(split instanceof InputStreamInputSplit) {
            record = new ArrayList<>();
            InputStreamInputSplit split2 = (InputStreamInputSplit) split;
            InputStream is =  split2.getIs();
            URI[] locations = split2.locations();
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
            File next = iter.next();
            Wave wave = new Wave(next.getAbsolutePath());
            return RecordUtils.toRecord(wave.getNormalizedAmplitudes());
        }
        else if(record != null) {
            hitImage = true;
            return record;
        }

        throw new IllegalStateException("Indeterminant state: record must not be null, or a file iterator must exist");


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
}
