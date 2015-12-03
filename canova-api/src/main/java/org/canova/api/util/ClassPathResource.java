package org.canova.api.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Simple utility class used to get access to files bundled into jar.
 * Based on Spring ClassPathResource implementation
 *
 *
 * @author raver119@gmail.com
 */
public class ClassPathResource {

    private String resourceName;

    private static Logger log = LoggerFactory.getLogger(ClassPathResource.class);

    public ClassPathResource(String resourceName) {
        if (resourceName == null) throw new IllegalStateException("Resource name can't be null");
        this.resourceName = resourceName;
    }

    public File getFile() throws FileNotFoundException {
        ClassLoader loader = null;
        try {
            loader = Thread.currentThread().getContextClassLoader();
        } catch (Exception e) {
            // do nothing
        }

        if (loader == null) {
            loader = ClassPathResource.class.getClassLoader();
        }

        URL url = loader.getResource(this.resourceName);

        if (url == null) {
            throw new FileNotFoundException("File '" + this.resourceName + "' cannot be found.");
        }
        try {
            return new File(new URI(url.toString().replaceAll(" ","%20")).getSchemeSpecificPart());
        } catch (URISyntaxException e) {
            return new File(url.getFile());
        }
    }

    public InputStream getInputStream() throws FileNotFoundException {
        File srcFile = this.getFile();
        return new FileInputStream(srcFile);
    }
}
