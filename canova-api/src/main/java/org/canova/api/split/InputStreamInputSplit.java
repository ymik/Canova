package org.canova.api.split;

import java.io.*;
import java.net.URI;

/**
 *
 * Input stream input split.
 * The normal pattern is reading the whole
 * input stream and turning that in to a record.
 * This is meant for streaming raw data
 * rather than normal mini batch pre processing.
 * @author Adam Gibson
 */
public class InputStreamInputSplit implements InputSplit {
    private InputStream is;
    private URI[] location;
    /**
     * Instantiate with the given
     * file as a uri
     * @param is the input stream to use
     * @param path the path to use
     */
    public InputStreamInputSplit(InputStream is,String path) {
        this.is = is;
        this.location = new URI[]{URI.create(path)};
    }
    /**
     * Instantiate with the given
     * file as a uri
     * @param is the input stream to use
     * @param path the path to use
     */
    public InputStreamInputSplit(InputStream is,File path) {
        this.is = is;
        this.location = new URI[]{path.toURI()};
    }

    /**
     * Instantiate with the given
     * file as a uri
     * @param is the input stream to use
     * @param path the path to use
     */
    public InputStreamInputSplit(InputStream is,URI path) {
        this.is = is;
        this.location = new URI[] {path};
    }


    public InputStreamInputSplit(InputStream is) {
        this.is = is;
    }

    @Override
    public long length() {
        throw new UnsupportedOperationException();
    }

    @Override
    public URI[] locations() {
        return location;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    public InputStream getIs() {
        return is;
    }

    public void setIs(InputStream is) {
        this.is = is;
    }
}
