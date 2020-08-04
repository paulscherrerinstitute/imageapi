package ch.psi.daq.imageapi.controller;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

public class RequestStatus {
    public static class Error {
        public String msg;
    }
    public List<Error> errors;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ", locale = "en_GB")
    public ZonedDateTime timestamp = ZonedDateTime.now(ZoneOffset.UTC);
}
