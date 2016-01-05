package org.canova.spark.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.input.PortableDataStream;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.writable.Writable;
import scala.Tuple2;

import java.io.DataInputStream;
import java.net.URI;
import java.util.Collection;


public class RecordReaderFunction implements Function<Tuple2<String,PortableDataStream>,Collection<Writable>> {
    protected RecordReader recordReader;

    public RecordReaderFunction(RecordReader recordReader){
        this.recordReader = recordReader;
    }

    @Override
    public Collection<Writable> call(Tuple2<String, PortableDataStream> value) throws Exception {
        URI uri = new URI(value._1());
        PortableDataStream ds = value._2();
        try( DataInputStream dis = ds.open() ){
            return recordReader.record(uri,dis);
        }
    }
}
