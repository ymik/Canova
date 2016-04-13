package org.canova.api.records.reader.impl.regex;

import org.apache.commons.io.FileUtils;
import org.canova.api.conf.Configuration;
import org.canova.api.io.data.Text;
import org.canova.api.records.reader.SequenceRecordReader;
import org.canova.api.records.reader.impl.FileRecordReader;
import org.canova.api.records.reader.impl.LineRecordReader;
import org.canova.api.split.InputSplit;
import org.canova.api.writable.Writable;
import sun.misc.IOUtils;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RegexSequenceRecordReader: Read an entire file (as a sequence), one line at a time and
 * split each line into fields using a regex.
 * Specifically, we are using {@link Pattern} and {@link Matcher} to do the splitting into groups
 *
 * @author Alex Black
 */
public class RegexSequenceRecordReader extends FileRecordReader implements SequenceRecordReader {
    public final static String SKIP_NUM_LINES = NAME_SPACE + ".skipnumlines";

    private String regex;
    private int skipNumLines;
    private Pattern pattern;
    private int numLinesSkipped;

    public RegexSequenceRecordReader(String regex, int skipNumLines){
        this.regex = regex;
        this.skipNumLines = skipNumLines;
        this.pattern = Pattern.compile(regex);
    }

    @Override
    public void initialize(Configuration conf, InputSplit split) throws IOException, InterruptedException {
        super.initialize(conf, split);
        this.skipNumLines = conf.getInt(SKIP_NUM_LINES,this.skipNumLines);
    }

    public Collection<Collection<Writable>> sequenceRecord() {
        File next = iter.next();

        String fileContents;
        try {
            fileContents = FileUtils.readFileToString(next);
        } catch(IOException e){
            throw new RuntimeException(e);
        }
        return loadSequence(fileContents,next.toURI());
    }

    @Override
    public
    Collection<Collection<Writable>> sequenceRecord(URI uri, DataInputStream dataInputStream) throws IOException {
        String fileContents = null; //TODO
        return loadSequence(fileContents, uri);
    }
    private Collection<Collection<Writable>> loadSequence(String fileContents, URI uri){
        String[] lines = fileContents.split("\n");  //TODO this won't work if regex allows for a newline

        Collection<Collection<Writable>> out = new ArrayList<>();
        for(String line : lines){
            //Split line using regex matcher
            Matcher m = pattern.matcher(line);
            List<Writable> timeStep;
            if(m.matches()){
                int count = m.groupCount();
                timeStep = new ArrayList<>(count);
                for( int i=1; i<=count; i++){    //Note: Matcher.group(0) is the entire sequence; we only care about groups 1 onward
                    timeStep.add(new Text(m.group(i)));
                }
            } else {
                throw new IllegalStateException("Invalid line: line does not match regex \"" + regex + "\"; line=\"" + line + "\"");
            }
            out.add(timeStep);
        }

        return out;

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
