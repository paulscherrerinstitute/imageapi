package ch.psi.daq.imageapi.eventmap.value;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public class JgenState {
    public boolean inChannel;
    public void beOutOfChannel(JsonGenerator jgen) {
        if (inChannel) {
            inChannel = false;
            try {
                jgen.writeEndArray();
                jgen.writeEndObject();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
