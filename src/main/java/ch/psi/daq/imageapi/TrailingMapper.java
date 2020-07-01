package ch.psi.daq.imageapi;

import org.springframework.core.io.buffer.DataBuffer;

import java.util.function.Function;

public interface TrailingMapper<T> extends Function<DataBuffer, T> {
    void release();
    T lastResult();
}
