package org.canova.api.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;

/**
 * String split used for single line inputs
 * @author Adam Gibson
 */
public class StringSplit implements InputSplit {
    private String data;

    public StringSplit(String data) {
        this.data = data;
    }

    @Override
    public long length() {
        return data.length();
    }

    @Override
    public URI[] locations() {
        return new URI[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(data.getBytes());
    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    public String getData() {
        return data;
    }
}
