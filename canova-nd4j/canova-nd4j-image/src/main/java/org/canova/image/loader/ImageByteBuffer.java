package org.canova.image.loader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.nd4j.linalg.api.buffer.BaseDataBuffer;
import org.nd4j.linalg.api.buffer.DataBuffer;

public class ImageByteBuffer extends BaseDataBuffer {

    public ImageByteBuffer(byte[] data, int length) {
        super(Unpooled.wrappedBuffer(data), length);
        this.elementSize = 1;
    }

    public ImageByteBuffer(int i) {
        super(i);
    }

    public ImageByteBuffer(double[] doubles) {
        super(doubles);
    }

    public ImageByteBuffer(float[] floats) {
        super(floats);
    }

    public ImageByteBuffer(int[] ints) {
        super(ints);
    }

    public ImageByteBuffer(ByteBuf byteBuf, int i) {
        super(byteBuf, i);
    }

    @Override
    protected DataBuffer create(int i) {
        return new ImageByteBuffer(i);
    }

    @Override
    public DataBuffer create(double[] doubles) {
        return new ImageByteBuffer(doubles);
    }

    @Override
    public DataBuffer create(float[] floats) {
        return new ImageByteBuffer(floats);
    }

    @Override
    public DataBuffer create(int[] ints) {
        return new ImageByteBuffer(ints);
    }

    @Override
    public DataBuffer create(ByteBuf byteBuf, int i) {
        return new ImageByteBuffer(byteBuf, i);
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
