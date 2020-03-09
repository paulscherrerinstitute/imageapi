package ch.psi.daq.imageapi;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Props2 {
    @Value("${imageapi.dataBaseDir}") public String jhjh;
    public Props2() {
    }
}
