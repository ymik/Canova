package org.canova.image.loader;

import org.apache.commons.io.FilenameUtils;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.LimitFileSplit;
import org.canova.image.recordreader.ImageRecordReader;
import org.junit.Test;
import java.io.*;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by nyghtowl on 12/17/15.
 */
public class LoaderTests {

    @Test
    public void testLfwLoader() {
        File dir = new File(FilenameUtils.concat(System.getProperty("user.home"), "lfw-a"));
        new LFWLoader(dir.toString(), true);
        assertTrue(dir.exists());

    }

    @Test
    public void testLfwReader() throws Exception {
        String subDir = "lfw-a/lfw";
        String path = FilenameUtils.concat(System.getProperty("user.home"), subDir);
        RecordReader rr = new ImageRecordReader(250, 250, 3, true, ".[0-9]+");
        rr.initialize(new LimitFileSplit(new File(path), null, 10, 5, ".[0-9]+", new Random(123)));
        assertEquals("Aaron_Sorkin", rr.getLabels().get(0));
    }

    @Test
    public void testCifarLoader() {
        File dir = new File(FilenameUtils.concat(System.getProperty("user.home"), "cifar"));
        CifarLoader cifar = new CifarLoader(dir.toString());
        assertTrue(dir.exists());

        cifar.load();
        assertTrue(cifar.getLabels() != null);

    }

    @Test
    public void testCifarInputStream() throws Exception {
        String subDir = "cifar/cifar-10-batches-bin/test_batch.bin";
        String path = FilenameUtils.concat(System.getProperty("user.home"), subDir);
        byte[] fullDataExpected = new byte[3073];
        FileInputStream inExpected = new FileInputStream(new File(path));
        inExpected.read(fullDataExpected);

        byte[] fullDataActual = new byte[3073];
        CifarLoader cifarLoad = new CifarLoader("TEST");
        InputStream inActual = cifarLoad.getInputStream();
        inActual.read(fullDataActual);
        assertEquals(fullDataExpected[0], fullDataActual[0]);
    }
}
