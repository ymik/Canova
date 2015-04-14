package org.canova.codec.reader;

import org.canova.api.conf.Configuration;
import org.canova.api.records.reader.SequenceRecordReader;
import org.canova.api.records.reader.impl.FileRecordReader;
import org.canova.api.writable.Writable;
import org.canova.common.RecordConverter;
import org.canova.image.loader.ImageLoader;
import org.jcodec.api.FrameGrab;


import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Codec record reader for parsing:
 * H.264 ( AVC ) Main profile decoder	MP3 decoder/encoder
 Apple ProRes decoder and encoder	AAC encoder
 H264 Baseline profile encoder
 Matroska ( MKV ) demuxer and muxer
 MP4 ( ISO BMF, QuickTime ) demuxer/muxer and tools
 MPEG 1/2 decoder ( supports interlace )
 MPEG PS/TS demuxer
 Java player applet
 VP8 encoder
 MXF demuxer

 Credit to jcodec for the underlying parser
 *
 * @author Adam Gibson
 */
public class CodecRecordReader extends FileRecordReader implements SequenceRecordReader {
    private int numFrames = -1;
    private int totalFrames = -1;
    private double framesPerSecond = -1;
    private double videoLength = -1;
    private ImageLoader imageLoader;
    private boolean ravel = false;

    public final static String NAME_SPACE = "org.canova.codec.reader";
    public final static String ROWS = NAME_SPACE + ".rows";
    public final static String COLUMNS = NAME_SPACE + ".columns";
    public final static String START_FRAME = NAME_SPACE + ".frames";
    public final static String TOTAL_FRAMES = NAME_SPACE + ".frames";
    public final static String TIME_SLICE = NAME_SPACE + ".time";
    public final static String RAVEL = NAME_SPACE + ".ravel";
    public final static String VIDEO_DURATION = NAME_SPACE + ".duration";


    @Override
    public Collection<Collection<Writable>> sequenceRecord() {
        File next = iter.next();
        Collection<Collection<Writable>> record = new ArrayList<>();
        if(numFrames >= 1) {
            for(int i = numFrames; i < totalFrames; i++) {
                try {
                    BufferedImage grab = FrameGrab.getFrame(next,i + 1);
                    if(ravel)
                        record.add(RecordConverter.toRecord(imageLoader.toRaveledTensor(grab)));

                    else
                        record.add(RecordConverter.toRecord(imageLoader.asRowVector(grab)));

                } catch (Exception e) {
                   throw new RuntimeException(e);
                }
            }

        }
        else {
            if(framesPerSecond < 1)
                throw new IllegalStateException("No frames or frame time intervals specified");


            else {
                for(double i = 0; i < videoLength; i += framesPerSecond) {
                    try {
                        BufferedImage grab = FrameGrab.getFrame(next,i);
                        if(ravel)
                            record.add(RecordConverter.toRecord(imageLoader.toRaveledTensor(grab)));

                        else
                            record.add(RecordConverter.toRecord(imageLoader.asRowVector(grab)));

                    } catch (Exception e) {
                       throw new RuntimeException(e);
                    }
                }


            }
        }

        return record;
    }


    @Override
    public Collection<Writable> next() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        numFrames = conf.getInt(START_FRAME,-1);
        int rows = conf.getInt(ROWS,28);
        int cols = conf.getInt(COLUMNS,28);
        imageLoader = new ImageLoader(rows,cols);
        framesPerSecond = conf.getFloat(TIME_SLICE,-1);
        videoLength = conf.getFloat(VIDEO_DURATION,-1);
        ravel = conf.getBoolean(RAVEL, false);
        totalFrames = conf.getInt(TOTAL_FRAMES, -1);



    }

    @Override
    public Configuration getConf() {
        return super.getConf();
    }
}
