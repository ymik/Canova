package org.canova.api.io.converters;

import org.canova.api.io.WritableConverter;
import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.FloatWritable;
import org.canova.api.io.data.IntWritable;
import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;

/**
 * Convert a writable to a
 * double
 * @author Adam Gibson
 */
public class DoubleWritableConverter implements WritableConverter {
    @Override
    public Writable convert(Writable writable) throws WritableConverterException{
        if(writable instanceof Text || writable instanceof FloatWritable || writable instanceof IntWritable || writable instanceof DoubleWritable) {
            return new DoubleWritable(Double.valueOf(writable.toString()));
        }

        throw new WritableConverterException("Unable to convert type " + writable.getClass());
    }
}
