package org.nd4j.api.formats.input.impl;

import org.nd4j.api.formats.input.InputFormat;
import org.nd4j.api.records.reader.RecordReader;
import org.nd4j.api.records.reader.impl.LineRecordReader;
import org.nd4j.api.split.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Line input format creates an @link{LineRecordReader}
 * @author Adam Gibson
 */
public class LineInputFormat implements InputFormat {
    @Override
    public RecordReader createReader(InputSplit split) throws IOException, InterruptedException {
        LineRecordReader ret = new LineRecordReader();
        ret.initialize(split);
        return ret;

    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
