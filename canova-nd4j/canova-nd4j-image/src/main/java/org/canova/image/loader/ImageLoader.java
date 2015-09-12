/*
 *
 *  *
 *  *  * Copyright 2015 Skymind,Inc.
 *  *  *
 *  *  *    Licensed under the Apache License, Version 2.0 (the "License");
 *  *  *    you may not use this file except in compliance with the License.
 *  *  *    You may obtain a copy of the License at
 *  *  *
 *  *  *        http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  *    Unless required by applicable law or agreed to in writing, software
 *  *  *    distributed under the License is distributed on an "AS IS" BASIS,
 *  *  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  *    See the License for the specific language governing permissions and
 *  *  *    limitations under the License.
 *  *
 *
 */

package org.canova.image.loader;

import com.github.jaiimageio.impl.plugins.tiff.TIFFImageReaderSpi;
import com.github.jaiimageio.impl.plugins.tiff.TIFFImageWriterSpi;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.util.ArrayUtil;

import javax.imageio.ImageIO;
import javax.imageio.spi.IIORegistry;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.*;

/**
 * Image loader for taking images
 * and converting them to matrices
 * @author Adam Gibson
 *
 */
public class ImageLoader implements Serializable {

    private int width = -1;
    private int height = -1;
    private int channels = -1;

    static {
        IIORegistry registry = IIORegistry.getDefaultInstance();
        registry.registerServiceProvider(new TIFFImageWriterSpi());
        registry.registerServiceProvider(new TIFFImageReaderSpi());
    }

    public ImageLoader() {
        super();
    }

    /**
     * Instantiate an image with the given
     * width and height
     *
     * @param width  the width to load
     * @param height the height to load
     */
    public ImageLoader(int width, int height) {
        super();
        this.width = width;
        this.height = height;
    }


    /**
     * Instantiate an image with the given
     * width and height
     *
     * @param width  the width to load
     * @param height the height to load
     * @param channels the number of channels for the image
     */
    public ImageLoader(int width, int height,int channels) {
        super();
        this.width = width;
        this.height = height;
        this.channels = channels;
    }

    /**
     * Convert a file to a row vector
     *
     * @param f the image to convert
     * @return the flattened image
     * @throws Exception
     */
    public INDArray asRowVector(File f) throws Exception {
        if(channels == 3) {
            return toRaveledTensor(f);
        }
        return ArrayUtil.toNDArray( flattenedImageFromFile(f));
    }

    /**
     * Changes the input stream in to an
     * rgb based raveled(flattened) vector
     * @param file the input stream to convert
     * @return  the raveled rgb values for this input stream
     */
    public INDArray toRaveledTensor(File file) {
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
            INDArray ret = toRaveledTensor(bis);
            bis.close();
            return ret.ravel();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    /**
     * Changes the input stream in to an
     * rgb based raveled(flattened) vector
     * @param is the input stream to convert
     * @return  the raveled rgb values for this input stream
     */
    public INDArray toRaveledTensor(InputStream is) {
        return toRgb(is).ravel();
    }

    /**
     * Convert an input stream to an rgb spectrum image
     *
     * @param file the file to convert
     * @return the input stream to convert
     */
    public INDArray toRgb(File file) {
        try {
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
            INDArray ret = toRgb(bis);
            bis.close();
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert the given image to an rgb image
     * @param arr the array to use
     * @param image the iamge to set
     */
    public void toBufferedImageRGB(INDArray arr,BufferedImage image) {
        if(arr.rank() < 3)
            throw new IllegalArgumentException("Arr must be 3d");

        image = scalingIfNeed(image, arr.size(-2), arr.size(-1));
        for (int i = 0; i < image.getWidth(); i++) {
            for (int j = 0; j < image.getHeight(); j++) {
                int r = arr.slice(0).getInt(i,j);
                int g = arr.slice(1).getInt(i,j);
                int b = arr.slice(2).getInt(i,j);
                int a = 1;
                int col = (a << 24) | (r << 16) | (g << 8) | b;
                image.setRGB(i,j,col);
            }
        }

    }

    /**
     * Convert an input stream to an rgb spectrum image
     *
     * @param inputStream the input stream to convert
     * @return the input stream to convert
     */
    public INDArray toRgb(InputStream inputStream) {
        try {
            BufferedImage image = ImageIO.read(inputStream);
            image = scalingIfNeed(image);
            return toINDArrayRGB(image);

        } catch (IOException e) {
            throw new RuntimeException("Unable to load image", e);
        }

    }

    /**
     * Convert an input stream to a matrix
     * @param inputStream the input stream to convert
     * @return the input stream to convert
     */
    public INDArray asMatrix(InputStream inputStream) {
       if(channels == 3)
           return toRgb(inputStream);
        try {
            BufferedImage image  = ImageIO.read(inputStream);
            image = scalingIfNeed(image);
            int w = image.getWidth();
            int h = image.getHeight();
            INDArray ret = Nd4j.create(h, w);

            for (int i = 0; i < h; i++) {
                for (int j = 0; j < w; j++) {
                    ret.putScalar(new int[]{i, j}, image.getRGB(i, j));
                }
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException("Unable to load image",e);
        }

    }

    public INDArray asRowVector(InputStream inputStream) {
        return asMatrix(inputStream).ravel();
    }

    /**
     * Slices up an image in to a mini batch.
     *
     * @param f the file to load from
     * @param numMiniBatches the number of images in a mini batch
     * @param numRowsPerSlice the number of rows for each image
     * @return a tensor representing one image as a mini batch
     */
    public INDArray asImageMiniBatches(File f,int numMiniBatches,int numRowsPerSlice) {
        try {
            INDArray d = asMatrix(f);
            return Nd4j.create(numMiniBatches, numRowsPerSlice, d.columns());
        }catch(Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Convert an image file
     * in to a matrix
     * @param f the file to convert
     * @return a 2d matrix of a rastered version of the image
     * @throws IOException
     */
    public INDArray asMatrix(File f) throws IOException {
        return ArrayUtil.toNDArray(fromFile(f));
    }

    public int[] flattenedImageFromFile(File f) throws Exception {
        return ArrayUtil.flatten(fromFile(f));
    }

    /**
     * Load a rastered image from file
     * @param file the file to load
     * @return the rastered image
     * @throws IOException
     */
    public int[][] fromFile(File file) throws IOException {
        BufferedImage image = ImageIO.read(file);
        image = scalingIfNeed(image);
        return toIntArrayArray(image);
    }

    /**
     * Load a rastered image from file
     * @param file the file to load
     * @return the rastered image
     * @throws IOException
     */
    public int[][][] fromFileMultipleChannels(File file) throws IOException {
        BufferedImage image = ImageIO.read(file);
        image = scalingIfNeed(image);
        Raster raster = image.getData();
        int w = raster.getWidth(), h = raster.getHeight();
        int[][][] ret = new int[w][h][channels];
        for (int i = 0; i < w; i++)
            for (int j = 0; j < h; j++) {
                Color color = new Color(image.getRGB(i,j));
                ret[i][j][0] = color.getRed();
                ret[i][j][1] = color.getBlue();
                ret[i][j][2] = color.getGreen();

            }

        return ret;
    }


    /**
     * Convert a matrix in to a buffereed image
     * @param matrix the
     * @return
     */
    public static BufferedImage toImage(INDArray matrix) {
        BufferedImage img = new BufferedImage(matrix.rows(), matrix.columns(), BufferedImage.TYPE_INT_ARGB);
        WritableRaster r = img.getRaster();
        int[] equiv = new int[matrix.length()];
        for(int i = 0; i < equiv.length; i++) {
            equiv[i] = (int) matrix.getDouble(i);
        }


        r.setDataElements(0,0,matrix.rows(),matrix.columns(),equiv);
        return img;
    }


    private static int[] rasterData(INDArray matrix) {
        int[] ret = new int[matrix.length()];
        for(int i = 0; i < ret.length; i++)
            ret[i] = (int) Math.round((double) matrix.getScalar(i).element());
        return ret;
    }

    /**
     * Converts a given Image into a BufferedImage
     *
     * @param img The Image to be converted
     * @return The converted BufferedImage
     */
    public static BufferedImage toBufferedImage(Image img) {
        if (img instanceof BufferedImage)
        {
            return (BufferedImage) img;
        }

        // Create a buffered image with transparency
        BufferedImage bimage = new BufferedImage(img.getWidth(null), img.getHeight(null), BufferedImage.TYPE_INT_ARGB);

        // Draw the image on to the buffered image
        Graphics2D bGr = bimage.createGraphics();
        bGr.drawImage(img, 0, 0, null);
        bGr.dispose();

        // Return the buffered image
        return bimage;
    }

    /**
     * Convert an image in to a row vector
     * @param image the image to convert
     * @return the row vector based on a rastered
     * representation of the image
     */
    public INDArray asRowVector(BufferedImage image) {
        image = scalingIfNeed(image);
        int[][] ret = toIntArrayArray(image);
        return ArrayUtil.toNDArray(ArrayUtil.flatten(ret));
    }

    /**
     * Convert an image in to a raveled tensor of
     * the rgb values of the image
     * @param image the image to parse
     * @return the raveled tensor of rgb values
     */
    public INDArray toRaveledTensor(BufferedImage image) {
        try {
            image = scalingIfNeed(image);
            return toINDArrayRGB(image).ravel();
        } catch (Exception e) {
            throw new RuntimeException("Unable to load image", e);
        }
    }

    private int[][] toIntArrayArray(BufferedImage image) {
        int w = image.getWidth(), h = image.getHeight();
        int[][] ret = new int[w][h];
        for (int i = 0; i < w; i++)
            for (int j = 0; j < h; j++)
                ret[i][j] = image.getRGB(i, j);
        return ret;
    }

    private INDArray toINDArrayRGB(BufferedImage image) {
        int width = image.getWidth();
        int height = image.getHeight();
        int bands = image.getSampleModel().getNumBands();;
        INDArray ret = Nd4j.create(channels, height, width);

        WritableRaster raster = image.getRaster();
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                for (int k = 0; k < channels; k++) {
                    if(k >= bands)
                        break;
                    ret.putScalar(new int[]{k, i, j}, raster.getSample(j, i, k));
                }
            }
        }
        return ret;
    }

    private BufferedImage scalingIfNeed(BufferedImage image) {
        return scalingIfNeed(image, height, width);
    }

    private BufferedImage scalingIfNeed(BufferedImage image, int dstHeight, int dstWidth) {
        if (dstHeight > 0 && dstWidth > 0 && (image.getHeight() != dstHeight || image.getWidth() != dstWidth))  {
            return toBufferedImage(image.getScaledInstance(dstHeight, dstWidth, Image.SCALE_SMOOTH));
        } else {
            return image;
        }
    }


}
