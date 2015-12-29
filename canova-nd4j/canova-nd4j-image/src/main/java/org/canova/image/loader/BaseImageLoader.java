package org.canova.image.loader;

import org.apache.commons.io.FileUtils;
import org.canova.api.records.reader.RecordReader;
import org.canova.api.split.LimitFileSplit;
import org.canova.api.util.ArchiveUtils;
import org.canova.image.recordreader.ImageRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Random;

/**
 * Created by nyghtowl on 12/17/15.
 */
public class BaseImageLoader {

    protected static final Logger log = LoggerFactory.getLogger(BaseImageLoader.class);

    public static final File BASE_DIR = new File(System.getProperty("user.home"));
    public static final String[] ALLOWED_FORMATS = {"tif", "jpg", "png", "jpeg", "bmp", "JPEG", "JPG", "TIF", "PNG"};
    protected Random rng = new Random(System.currentTimeMillis());

    public void downloadAndUntar(Map urlMap, File fullDir) {
        try {
            File file = new File(fullDir, urlMap.get("filesFilename").toString());
            if (!file.isFile()) {
                FileUtils.copyURLToFile(new URL(urlMap.get("filesURL").toString()), file);
            }

            String fileName = file.toString();
            if (fileName.endsWith(".tgz") || fileName.endsWith(".tar.gz") || fileName.endsWith(".gz") || fileName.endsWith(".zip"))
                ArchiveUtils.unzipFileTo(file.getAbsolutePath(), fullDir.getAbsolutePath());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to fetch images",e);
        }
    }

}
