package org.canova.api.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

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

    private URL getUrl() {
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
            throw new IllegalStateException("Resource '" + this.resourceName + "' cannot be found.");
        }
        return url;
    }

    public File getFile() throws FileNotFoundException {
        URL url = this.getUrl();

        if (isJarURL(url)) {
            /*
                This is actually request for file, that's packed into jar. Probably the current one, but that doesn't matters.
             */
            try {
                url = extractActualUrl(url);
                File file = File.createTempFile("canova_temp","file");
                file.deleteOnExit();

                ZipFile zipFile = new ZipFile(url.getFile());
                ZipEntry entry = zipFile.getEntry(this.resourceName);

                InputStream stream = zipFile.getInputStream(entry);
                FileOutputStream outputStream = new FileOutputStream(file);
                byte[] array = new byte[1024];
                int rd = 0;
                do {
                    rd = stream.read(array);
                    outputStream.write(array,0,rd);
                } while (rd == 1024);

                outputStream.flush();
                outputStream.close();

                stream.close();
                zipFile.close();

                return file;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        } else {
            /*
                It's something in the actual underlying filesystem, so we can just go for it
             */

            try {
                URI uri = new URI(url.toString().replaceAll(" ", "%20"));
                log.info("URI: " + uri);
                return new File(uri.getSchemeSpecificPart());
            } catch (URISyntaxException e) {
                return new File(url.getFile());
            }
        }
    }

    private boolean isJarURL(URL url) {
        String protocol = url.getProtocol();
        return "jar".equals(protocol) || "zip".equals(protocol) || "wsjar".equals(protocol) || "code-source".equals(protocol) && url.getPath().contains("!/");
    }

    private URL extractActualUrl(URL jarUrl) throws MalformedURLException {
        String urlFile = jarUrl.getFile();
        int separatorIndex = urlFile.indexOf("!/");
        if(separatorIndex != -1) {
            String jarFile = urlFile.substring(0, separatorIndex);

            try {
                return new URL(jarFile);
            } catch (MalformedURLException var5) {
                if(!jarFile.startsWith("/")) {
                    jarFile = "/" + jarFile;
                }

                return new URL("file:" + jarFile);
            }
        } else {
            return jarUrl;
        }
    }

    public InputStream getInputStream() throws FileNotFoundException {
        URL url = this.getUrl();
        if (isJarURL(url)) {
            try {
                url = extractActualUrl(url);
                ZipFile zipFile = new ZipFile(url.getFile());
                ZipEntry entry = zipFile.getEntry(this.resourceName);

                InputStream stream = zipFile.getInputStream(entry);
                return stream;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            File srcFile = this.getFile();
            return new FileInputStream(srcFile);
        }
    }
}
