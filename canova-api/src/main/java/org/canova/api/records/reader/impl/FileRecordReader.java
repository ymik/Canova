package org.canova.api.records.reader.impl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.canova.api.io.data.Text;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.InputSplit;
import org.canova.api.writable.Writable;


import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * File reader/writer
 *
 * @author Adam Gibson
 */
public class FileRecordReader implements RecordReader {

    private URI[] locations;
    private int currIndex = 0;
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
                        Iterator<File> allFiles2 = FileUtils.iterateFiles(iter,null,true);
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
        List<Writable> ret = new ArrayList<>();
        try {
            ret.add((Writable) new Text(FileUtils.readFileToString(iter.next())));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(iter.hasNext()) {
            return ret;
        }
        else {
            currIndex++;

            if(iter.hasNext()) {
                try {
                    ret.add((Writable) new Text(FileUtils.readFileToString(iter.next())));
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
