package org.canova.common;

import org.canova.api.io.data.DoubleWritable;
import org.canova.api.io.data.FloatWritable;
import org.canova.api.writable.Writable;
import org.nd4j.linalg.api.buffer.DataBuffer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Adam Gibson
 */
public class RecordConverter {
    private RecordConverter() {}
    /**
     * Convert a record to an ndarray
     * @param record the record to convert
     * @return the record
     */
    public static INDArray toArray(Collection<Writable> record) {
        INDArray ret = Nd4j.create(record.size());
        Iterator<Writable> recordIter = record.iterator();
        int count = 0;
        while(recordIter.hasNext()) {
            ret.putScalar(count++,Double.valueOf(recordIter.next().toString()));
        }
       return ret;
    }

    /**
     * Convert an ndarray to a record
     * @param array the array to convert
     * @return the record
     */
    public static Collection<Writable> toRecord(INDArray array) {
        INDArray linear = array.linearView();
        Collection<Writable> writables = new ArrayList<>();
        for(int i = 0; i < linear.length(); i++) {
            writables.add(array.data().dataType() == DataBuffer.DOUBLE ? new DoubleWritable(linear.getDouble(i)) : new FloatWritable(linear.getFloat(i)));
        }
        return writables;
    }

}
