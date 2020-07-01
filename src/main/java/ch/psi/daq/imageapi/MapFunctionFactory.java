package ch.psi.daq.imageapi;

import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public interface MapFunctionFactory<T> {
    Function<Flux<DataBuffer>, Publisher<T>> makeTrans(KeyspaceToDataParams kspp, int fileno);
}
