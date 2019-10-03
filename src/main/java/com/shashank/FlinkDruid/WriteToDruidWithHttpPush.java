package com.shashank.FlinkDruid;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;


@SpringBootApplication
public class WriteToDruidWithHttpPush {


    private static String url = "http://localhost:8200/v1/post/wikipedia";

    private static RestTemplate template;

    private static HttpHeaders headers;


    WriteToDruidWithHttpPush() {

        template = new RestTemplate();
        headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.APPLICATION_JSON);

    }

    public static void main(String[] args) throws Exception {

        SpringApplication.run(WriteToDruidWithHttpPush.class, args);

        // Creating Flink Execution Environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Define data source
        DataSet<String> data = env.readTextFile("/src/main/resources/wikiticker-2015-09-12-sampled.json");
        data.map(x -> {
            return httpsPost(x).toString();
        }).print();


    }

    // http post method to post data in Druid
    private static ResponseEntity<String> httpsPost(String json) {

        HttpEntity<String> requestEntity =
                new HttpEntity<>(json, headers);
        ResponseEntity<String> response =
                template.exchange("http://localhost:8200/v1/post/wikipedia", HttpMethod.POST, requestEntity,
                        String.class);

        return response;
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }




}

