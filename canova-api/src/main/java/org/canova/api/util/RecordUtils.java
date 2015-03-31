package org.canova.api.util;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.FloatWritable;
import org.canova.api.writable.Writable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Create records from the specified input
 *
 * @author Adam Gibson
 */
public class RecordUtils {

    public static Collection<Writable> toRecord(double[] record) {
        List<Writable> ret = new ArrayList<>(record.length);
        for(int i = 0; i < record.length; i++)
           ret.add(new DoubleWritable(record[i]));

        return ret;
    }


    public static Collection<Writable> toRecord(float[] record) {
        List<Writable> ret = new ArrayList<>(record.length);
        for(int i = 0; i < record.length; i++)
            ret.add(new FloatWritable(record[i]));

        return ret;
    }

}
