package org.canova.api.records.reader.impl.regex;

import org.canova.api.conf.Configuration;
import org.canova.api.io.data.Text;
import org.canova.api.records.reader.impl.LineRecordReader;
import org.canova.api.split.InputSplit;
import org.canova.api.writable.Writable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RegexLineRecordReader: Read a file, one line at a time, and split it into fields using a regex.
 * Specifically, we are using {@link java.util.regex.Pattern} and {@link java.util.regex.Matcher}
 *
 * @author Alex Black
 */
public class RegexLineRecordReader extends LineRecordReader {
    public final static String SKIP_NUM_LINES = NAME_SPACE + ".skipnumlines";

    private String regex;
    private int skipNumLines;
    private Pattern pattern;
    private int numLinesSkipped;

    public RegexLineRecordReader(String regex, int skipNumLines){
        this.regex = regex;
        this.skipNumLines = skipNumLines;
        this.pattern = Pattern.compile(regex);
    }

    @Override
    public void initialize(Configuration conf, InputSplit split) throws IOException, InterruptedException {
        super.initialize(conf, split);
        this.skipNumLines = conf.getInt(SKIP_NUM_LINES,this.skipNumLines);
    }

    @Override
    public Collection<Writable> next() {

        if(numLinesSkipped < skipNumLines) {
            for(int i = numLinesSkipped; i < skipNumLines; i++, numLinesSkipped++) {
                if(!hasNext()) {
                    return new ArrayList<>();
                }
                super.next();
            }
        }
        Text t =  (Text) super.next().iterator().next();
        String val = t.toString();
        Matcher m = pattern.matcher(val);

        List<Writable> ret;
        if(m.matches()){
            int count = m.groupCount();
            ret = new ArrayList<>(count);
            for( int i=1; i<=count; i++){    //Note: Matcher.group(0) is the entire sequence; we only care about groups 1 onward
                ret.add(new Text(m.group(i)));
            }
        } else {
            throw new IllegalStateException("Invalid line: line does not match regex \"" + regex + "\"; line=\"" + val + "\"");
        }

        return ret;
    }

    @Override
    public void reset(){
        super.reset();
        numLinesSkipped = 0;
    }

}
