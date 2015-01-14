package org.nd4j.api.split;

import org.nd4j.api.writable.Writable;

import java.net.URI;

/**
 * An input split
 *
 *
 * @author Adam Gibson
 */
public interface InputSplit extends Writable {


    /**
     *  Length of the split
     * @return
     */
    long length();

    /**
     * Locations of the splits
     * @return
     */
    URI[] locations();

}
