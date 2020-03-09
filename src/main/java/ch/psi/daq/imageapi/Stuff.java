package ch.psi.daq.imageapi;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component("theStuff")
public class Stuff {
    public Stuff() {
    //public Stuff(@Value("${parameterA}") String parameterA) {
        //this.parameterA = parameterA;
    }
    public String parameterA;
}
