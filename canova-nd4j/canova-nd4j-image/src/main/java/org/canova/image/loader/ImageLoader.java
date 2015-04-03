package org.canova.image.loader;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.util.ArrayUtil;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * Image loader for taking images and converting them to matrices
 * @author Adam Gibson
 *
 */
public class ImageLoader implements Serializable {

    private int width = -1;
    private int height = -1;


    public ImageLoader() {
        super();
    }

    public ImageLoader(int width, int height) {
        super();
        this.width = width;
        this.height = height;
    }

    /**
     * Convert a file to a row vector
     * @param f the image to convert
     * @return the flattened image
     * @throws Exception
     */
    public INDArray asRowVector(File f) throws Exception {
        return ArrayUtil.toNDArray(flattenedImageFromFile(f));
    }


    /**
     * Convert an input stream to a matrix
     * @param inputStream the input stream to convert
     * @return the input stream to convert
     */
    public INDArray asMatrix(InputStream inputStream) {
        try {
            BufferedImage image  = ImageIO.read(inputStream);
            if (height > 0 && width > 0)
                image = toBufferedImage(image.getScaledInstance(height, width, Image.SCALE_SMOOTH));
            Raster raster = image.getData();
            int w = raster.getWidth(), h = raster.getHeight();
            int[][] ret = new int[w][h];
            for (int i = 0; i < w; i++)
                for (int j = 0; j < h; j++)
                    ret[i][j] = raster.getSample(i, j, 0);
            INDArray newRet = Nd4j.create(w,h);
            for(int i = 0; i < ret.length; i++) {
                for(int j = 0; j < ret[i].length; j++) {
                    newRet.putScalar(new int[]{i,j},ret[i][j]);
                }
            }

            return newRet;
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

    public INDArray asMatrix(File f) throws IOException {
        return ArrayUtil.toNDArray(fromFile(f));
    }

    public int[] flattenedImageFromFile(File f) throws Exception {
        return ArrayUtil.flatten(fromFile(f));
    }

    public int[][] fromFile(File file) throws IOException {
        BufferedImage image = ImageIO.read(file);
        if (height > 0 && width > 0)
            image = toBufferedImage(image.getScaledInstance(height, width, Image.SCALE_SMOOTH));
        Raster raster = image.getData();
        int w = raster.getWidth(), h = raster.getHeight();
        int[][] ret = new int[w][h];
        for (int i = 0; i < w; i++)
            for (int j = 0; j < h; j++)
                ret[i][j] = raster.getSample(i, j, 0);

        return ret;
    }


    public static BufferedImage toImage(INDArray matrix) {
        BufferedImage img = new BufferedImage(matrix.rows(), matrix.columns(), BufferedImage.TYPE_INT_ARGB);
        WritableRaster r = img.getRaster();
        int[] equiv = new int[matrix.length()];
        for(int i = 0; i < equiv.length; i++) {
            equiv[i] = (int) matrix.getScalar(i).element();
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
    public static BufferedImage toBufferedImage(Image img)
    {
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

}
