package org.canova.api.records.reader.impl;



import org.canova.api.io.data.Text;
import org.canova.api.writable.Writable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Simple csv record reader.
 *
 * @author Adam Gibson
 */
public class CSVRecordReader extends LineRecordReader {
    private boolean skippedLines = false;
    private int skipNumLines = 0;
    private String delimiter = ",";

    /**
     * Skip first n lines
     * @param skipNumLines the number of lines to skip
     */
    public CSVRecordReader(int skipNumLines) {
         this(skipNumLines,",");
    }

    /**
     * Skip lines and use delimiter
     * @param skipNumLines the number of lines to skip
     * @param delimiter the delimiter
     */
    public CSVRecordReader(int skipNumLines,String delimiter) {
        this.skipNumLines = skipNumLines;
        this.delimiter = delimiter;
    }

    public CSVRecordReader() {
        this(0,",");
    }

    @Override
    public Collection<Writable> next() {
        if(!skippedLines && skipNumLines > 0) {
            for(int i = 0; i < skipNumLines; i++) {
                if(!hasNext()) {
                    return new ArrayList<>();
                }
                super.next();
            }
            skippedLines = true;
        }
        Text t =  (Text) super.next().iterator().next();
        String val = new String(t.getBytes());
        String[] split = val.split(delimiter);
        List<Writable> ret = new ArrayList<>();
        for(String s : split)
            ret.add(new Text(s));
        return ret;

    }
}
