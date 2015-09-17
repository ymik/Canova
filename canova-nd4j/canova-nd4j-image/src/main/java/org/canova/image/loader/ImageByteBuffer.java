package org.canova.image.loader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.nd4j.linalg.api.buffer.BaseDataBuffer;
import org.nd4j.linalg.api.buffer.DataBuffer;
import java.lang.UnsupportedClassVersionError;

public class ImageByteBuffer extends BaseDataBuffer {

    public ImageByteBuffer(byte[] data, int length) {
        super(Unpooled.wrappedBuffer(data), length);
        this.elementSize = 1;
    }

    @Override
    protected DataBuffer create(int i) {
        throw new UnsupportedClassVersionError();
    }

    @Override
    public DataBuffer create(double[] doubles) {
        throw new UnsupportedClassVersionError();
    }

    @Override
    public DataBuffer create(float[] floats) {
        throw new UnsupportedClassVersionError();
    }

    @Override
    public DataBuffer create(int[] ints) {
        throw new UnsupportedClassVersionError();
    }

    @Override
    public DataBuffer create(ByteBuf byteBuf, int i) {
        throw new UnsupportedClassVersionError();
    }

    @Override
    public int getElementSize() {
        return this.elementSize;
    }

    @Override
    public Type dataType() {
        return DataBuffer.Type.INT;
    }

    @Override
    public int getInt(int i) {
        return ((int)dataBuffer.getByte(i)) & 0xff;
    }

    @Override
    public float getFloat(int i) {
        return (float)getInt(i);
    }

    @Override
    public Number getNumber(int i) {
        return getInt(i);
    }

    @Override
    public double getDouble(int i) {
        return (double)getInt(i);
    }
}
