package org.canova.api.io;

import org.canova.api.io.converters.WritableConverterException;
import org.canova.api.writable.Writable;

/**
 * Convert a writable to another writable (used for say: transitioning dates or categorical to numbers)
 *
 * @author Adam Gibson
 */
public interface WritableConverter {


    /**
     * Convert a writable to another kind of writable
     * @param writable the writable to convert
     * @return the converted writable
     */
    Writable convert(Writable writable) throws WritableConverterException;

}
