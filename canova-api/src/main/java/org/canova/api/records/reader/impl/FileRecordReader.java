package org.canova.api.records.reader.impl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.canova.api.conf.Configuration;
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

    protected Iterator<File> iter;
    protected Configuration conf;
    public FileRecordReader() {
    }

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
    public void initialize(Configuration conf, InputSplit split) throws IOException, InterruptedException {
        initialize(split);
    }

    @Override
    public Collection<Writable> next() {
        List<Writable> ret = new ArrayList<>();
        try {
            ret.add(new Text(FileUtils.readFileToString(iter.next())));
        } catch (IOException e) {
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

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
