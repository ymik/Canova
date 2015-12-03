package org.canova.api.util;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;

import static org.junit.Assert.*;

/**
 * Created by fartovii on 03.12.15.
 */
public class ClassPathResourceTest {

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testGetFile() throws Exception {
        File intFile = new ClassPathResource("iris.dat").getFile();

        assertTrue(intFile.exists());
        assertEquals(2700, intFile.length());
    }

    @Test
    public void testInputStream() throws Exception {
        ClassPathResource resource = new ClassPathResource("csvsequence_1.txt");
        File intFile = resource.getFile();

        assertEquals(60, intFile.length());

        InputStream stream = resource.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line = "";
        int cnt = 0;
        while ((line = reader.readLine()) != null) {
            cnt++;
        }

        assertEquals(5, cnt);
    }
}