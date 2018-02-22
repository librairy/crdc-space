package org.librairy.service.space.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.ObjectMapper;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.librairy.service.space.Application;
import org.librairy.service.space.rest.model.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.IOException;
import java.util.Arrays;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@WebAppConfiguration
public class RestTest {

    private static final Logger LOG = LoggerFactory.getLogger(RestTest.class);

    @Before
    public void setup(){
        Unirest.setObjectMapper(new ObjectMapper() {
            private com.fasterxml.jackson.databind.ObjectMapper jacksonObjectMapper
                    = new com.fasterxml.jackson.databind.ObjectMapper();

            public <T> T readValue(String value, Class<T> valueType) {
                try {
                    return jacksonObjectMapper.readValue(value, valueType);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            public String writeValue(Object value) {
                try {
                    return jacksonObjectMapper.writeValueAsString(value);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Test
    @Ignore
    public void post() throws UnirestException {

        Point req = new Point(org.librairy.service.space.facade.model.Point.newBuilder().setId("id").setName("name").setType("type").setShape(Arrays.asList(new Double[]{0.1,0.2})).build());

        HttpResponse<JsonNode> response = Unirest.post("http://localhost:7777/points")
                .header("accept", "application/json")
                .header("Content-Type", "application/json")
                .body(req)
                .asJson();

        LOG.info("Response: " + response.getBody());


    }
}