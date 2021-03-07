package ru.gex.mdm_proxy.elk_consumer.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("ru.gex")
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
}
