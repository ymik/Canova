package org.canova.api.util;

import com.sun.istack.internal.NotNull;

import java.io.File;
import java.io.InputStream;

/**
 * Simple utility class used to get access to files bundled into jar.
 *
 *  WORK IS IN PROGRESS, PLEASE DO NOT USE
 *
 * @author raver119@gmail.com
 */
public class ClassPathResource {

    private String resourceName;

    public ClassPathResource(@NotNull String resourceName) {
        this.resourceName = resourceName;
    }

    public File getFile() {
        return null;
    }

    public InputStream getStream() {
        return null;
    }
}
