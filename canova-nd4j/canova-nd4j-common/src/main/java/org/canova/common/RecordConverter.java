/*
 *
 *  *
 *  *  * Copyright 2015 Skymind,Inc.
 *  *  *
 *  *  *    Licensed under the Apache License, Version 2.0 (the "License");
 *  *  *    you may not use this file except in compliance with the License.
 *  *  *    You may obtain a copy of the License at
 *  *  *
 *  *  *        http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  *    Unless required by applicable law or agreed to in writing, software
 *  *  *    distributed under the License is distributed on an "AS IS" BASIS,
 *  *  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  *    See the License for the specific language governing permissions and
 *  *  *    limitations under the License.
 *  *
 *
 */

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
     * Convert an ndarray to a record
     * @param record the array to convert
     * @return the record
     */
    public static INDArray toArray
    (Collection<Writable> record) {
        Iterator<Writable> writables = record.iterator();
        INDArray linear = Nd4j.zeros(record.size());

        int count = 0;
        while(writables.hasNext()) {
            linear.putScalar(count++,Double.valueOf(writables.next().toString()));
        }
        return linear;
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
            writables.add(array.data().dataType() == DataBuffer.Type.DOUBLE ? new DoubleWritable(linear.getDouble(i)) : new FloatWritable(linear.getFloat(i)));
        }
        return writables;
    }

}
