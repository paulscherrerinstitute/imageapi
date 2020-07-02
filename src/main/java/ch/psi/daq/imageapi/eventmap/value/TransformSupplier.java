package ch.psi.daq.imageapi.eventmap.value;

import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public interface TransformSupplier<T> {

    Function<? super Flux<DataBuffer>, ? extends Publisher<T>> get(String channelName);

}
