package org.solution;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"org.solution.delaymessage"})
public class DelayMessageApplication {

    public static void main(String[] args) {
        SpringApplication.run(DelayMessageApplication.class, args);
    }

}
