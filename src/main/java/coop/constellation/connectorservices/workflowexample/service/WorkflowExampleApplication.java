package coop.constellation.connectorservices.workflowexample.service;

// Required to create a springboot application

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;


@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
public class WorkflowExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkflowExampleApplication.class, args);
    }


}
