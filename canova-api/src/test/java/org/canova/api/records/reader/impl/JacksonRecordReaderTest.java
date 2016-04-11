package org.canova.api.records.reader.impl;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.canova.api.io.data.Text;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.records.reader.impl.jackson.FieldSelection;
import org.canova.api.records.reader.impl.jackson.JacksonRecordReader;
import org.canova.api.split.InputSplit;
import org.canova.api.split.NumberedFileInputSplit;
import org.canova.api.util.ClassPathResource;
import org.canova.api.writable.Writable;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by Alex on 11/04/2016.
 */
public class JacksonRecordReaderTest {

    @Test
    public void testReadingJson() throws Exception {
        //Load 3 values from 3 JSON files
        //stricture: a:value, b:value, c:x:value, c:y:value
        //And we want to load only a:value, b:value and c:x:value
        //For first JSON file: all values are present
        //For second JSON file: b:value is missing
        //For third JSON file: c:x:value is missing

        ClassPathResource cpr = new ClassPathResource("json/json_test_0.txt");
        String path = cpr.getFile().getAbsolutePath().replace("0", "%d");

        InputSplit is = new NumberedFileInputSplit(path, 0, 2);

        RecordReader rr = new JacksonRecordReader(getFieldSelection(), new ObjectMapper(new JsonFactory()));
        rr.initialize(is);

        testJacksonRecordReader(rr);
    }

    @Test
    public void testReadingYaml() throws Exception {
        //Exact same information as JSON format, but in YAML format

        ClassPathResource cpr = new ClassPathResource("yaml/yaml_test_0.txt");
        String path = cpr.getFile().getAbsolutePath().replace("0", "%d");

        InputSplit is = new NumberedFileInputSplit(path, 0, 2);

        RecordReader rr = new JacksonRecordReader(getFieldSelection(), new ObjectMapper(new YAMLFactory()));
        rr.initialize(is);

        testJacksonRecordReader(rr);
    }

    @Test
    public void testReadingXml() throws Exception {
        //Exact same information as JSON format, but in XML format

        ClassPathResource cpr = new ClassPathResource("xml/xml_test_0.txt");
        String path = cpr.getFile().getAbsolutePath().replace("0", "%d");

        InputSplit is = new NumberedFileInputSplit(path, 0, 2);

        RecordReader rr = new JacksonRecordReader(getFieldSelection(), new ObjectMapper(new XmlFactory()));
        rr.initialize(is);

        testJacksonRecordReader(rr);
    }


    private static FieldSelection getFieldSelection(){
        return new FieldSelection.Builder()
                .addField("a")
                .addField(new Text("MISSING_B"), "b")
                .addField(new Text("MISSING_CX"), "c", "x")
                .build();
    }



    private static void testJacksonRecordReader(RecordReader rr ){

        Collection<Writable> json0 = rr.next();
        List<Writable> exp0 = Arrays.asList((Writable)new Text("aValue0"), new Text("bValue0"), new Text("cxValue0"));
        assertEquals(exp0, json0);

        Collection<Writable> json1 = rr.next();
        List<Writable> exp1 = Arrays.asList((Writable)new Text("aValue1"), new Text("MISSING_B"), new Text("cxValue1"));
        assertEquals(exp1, json1);

        Collection<Writable> json2 = rr.next();
        List<Writable> exp2 = Arrays.asList((Writable)new Text("aValue2"), new Text("bValue2"), new Text("MISSING_CX"));
        assertEquals(exp2, json2);

        assertFalse(rr.hasNext());

        //Test reset
        rr.reset();
        assertEquals(exp0, rr.next());
        assertEquals(exp1, rr.next());
        assertEquals(exp2, rr.next());
        assertFalse(rr.hasNext());
    }

}
