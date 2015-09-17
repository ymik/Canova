package org.canova.image.loader;

import org.nd4j.linalg.api.ndarray.INDArray;

import java.awt.image.BufferedImage;
import java.util.Random;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public class TestImageLoader {

    private static long seed = 10;
    private static Random rng = new Random(seed);

    @Test
    public void testToIntArrayArray() throws Exception {
        BufferedImage img = makeRandomBufferedImage(true);

        int w = img.getWidth();
        int h = img.getHeight();
        int ch = 4;
        ImageLoader loader = new ImageLoader(0, 0, ch);
        int[][] arr = loader.toIntArrayArray(img);

        assertEquals(h, arr.length);
        assertEquals(w, arr[0].length);

        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                assertEquals(img.getRGB(j, i), arr[i][j]);
            }
        }
    }


    @Test
    public void testToINDArrayRGB() throws Exception {
        BufferedImage img = makeRandomBufferedImage(false);
        int w = img.getWidth();
        int h = img.getHeight();
        int ch = 3;

        ImageLoader loader = new ImageLoader(0, 0, ch);
        INDArray arr = loader.toINDArrayRGB(img);

        int[] shape = arr.shape();
        assertEquals(3,  shape.length);
        assertEquals(ch, shape[0]);
        assertEquals(h,  shape[1]);
        assertEquals(w,  shape[2]);

        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                int srcColor = img.getRGB(j, i);
                int a = 0xff << 24;
                int r = arr.getInt(0, i, j) << 16;
                int g = arr.getInt(1, i, j) << 8;
                int b = arr.getInt(2, i, j);
                int dstColor = a | r | g | b;
                assertEquals(srcColor, dstColor);
            }
        }
    }

    @Test
    public void testScalingIfNeed() throws Exception {
        BufferedImage img1 = makeRandomBufferedImage(true);
        BufferedImage img2 = makeRandomBufferedImage(false);

        int w1 = 60, h1 = 110, ch1 = 3;
        ImageLoader loader1 = new ImageLoader(w1, h1, ch1);

        BufferedImage scaled1 = loader1.scalingIfNeed(img1, true);
        assertEquals(w1,                          scaled1.getWidth());
        assertEquals(h1,                          scaled1.getHeight());
        assertEquals(BufferedImage.TYPE_INT_ARGB, scaled1.getType());
        assertEquals(4,                           scaled1.getSampleModel().getNumBands());

        BufferedImage scaled2 = loader1.scalingIfNeed(img1, false);
        assertEquals(w1,                         scaled2.getWidth());
        assertEquals(h1,                         scaled2.getHeight());
        assertEquals(BufferedImage.TYPE_INT_RGB, scaled2.getType());
        assertEquals(3,                          scaled2.getSampleModel().getNumBands());

        BufferedImage scaled3 = loader1.scalingIfNeed(img2, true);
        assertEquals(w1,                         scaled3.getWidth());
        assertEquals(h1,                         scaled3.getHeight());
        assertEquals(BufferedImage.TYPE_INT_RGB, scaled3.getType());
        assertEquals(3,                          scaled3.getSampleModel().getNumBands());

        BufferedImage scaled4 = loader1.scalingIfNeed(img2, false);
        assertEquals(w1,                         scaled4.getWidth());
        assertEquals(h1,                         scaled4.getHeight());
        assertEquals(BufferedImage.TYPE_INT_RGB, scaled4.getType());
        assertEquals(3,                          scaled4.getSampleModel().getNumBands());

        int w2 = 70, h2 = 120, ch2 = 4;
        ImageLoader loader2 = new ImageLoader(w2, h2, ch2);

        BufferedImage scaled5 = loader2.scalingIfNeed(img1, true);
        assertEquals(w2,                          scaled5.getWidth());
        assertEquals(h2,                          scaled5.getHeight(), h2);
        assertEquals(BufferedImage.TYPE_INT_ARGB, scaled5.getType());
        assertEquals(4,                           scaled5.getSampleModel().getNumBands());

        BufferedImage scaled6 = loader2.scalingIfNeed(img1, false);
        assertEquals(w2,                         scaled6.getWidth());
        assertEquals(h2,                         scaled6.getHeight());
        assertEquals(BufferedImage.TYPE_INT_RGB, scaled6.getType());
        assertEquals(3,                          scaled6.getSampleModel().getNumBands());

    }

    private BufferedImage makeRandomBufferedImage(boolean alpha) {
        int w = rng.nextInt() % 100 + 100;
        int h = rng.nextInt() % 100 + 100;
        int type = alpha ? BufferedImage.TYPE_INT_ARGB : BufferedImage.TYPE_INT_RGB;
        BufferedImage img = new BufferedImage(w, h, type);
        for (int i = 0; i < h; ++i) {
            for (int j = 0; j < w; ++j) {
                int a = (alpha ? rng.nextInt() : 1) & 0xff;
                int r = rng.nextInt() & 0xff;
                int g = rng.nextInt() & 0xff;
                int b = rng.nextInt() & 0xff;
                int v = (a << 24) | (r << 16) | (g << 8) | b;
                img.setRGB(j, i, v);
            }
        }
        return img;
    }
}
