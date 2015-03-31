package org.canova.sound.recordreader;

import org.apache.commons.io.FileUtils;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.InputSplit;
import org.canova.api.util.RecordUtils;
import org.canova.api.writable.Writable;
import org.canova.sound.musicg.Wave;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Wav file loader
 * @author Adam Gibson
 */
public class WavFileRecordReader implements RecordReader {
    private Iterator<File> iter;


    @Override
    public void initialize(InputSplit split) throws IOException, InterruptedException {
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
    @Override
    public Collection<Writable> next() {
        File next = iter.next();
        Wave wave = new Wave(next.getAbsolutePath());
        return RecordUtils.toRecord(wave.getNormalizedAmplitudes());
    }

    @Override
    public boolean hasNext() {
        return iter != null && iter.hasNext();
    }


    @Override
    public void close() throws IOException {

    }
}
