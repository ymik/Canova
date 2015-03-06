package org.canova.api.io.converters;

import org.canova.api.io.WritableConverter;
import org.canova.api.writable.Writable;

/**
 * Baseline writable converter
 * @author Adam Gibson
 */
public class SelfWritableConverter implements WritableConverter {
    @Override
    public Writable convert(Writable writable) {
        return writable;
    }
}
