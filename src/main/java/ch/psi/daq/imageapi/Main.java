package ch.psi.daq.imageapi;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

@SpringBootApplication
public class Main implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) {
    }

    public static void main(final String[] args) {
        TestInfo.test();
        SpringApplication.run(Main.class, args);
    }

    public static void main_manual(final String[] args) {
        new SpringApplicationBuilder(Main.class)
        .web(WebApplicationType.REACTIVE)
        .run(args);
    }

}
