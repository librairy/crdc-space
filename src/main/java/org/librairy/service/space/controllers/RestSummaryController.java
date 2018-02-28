package org.librairy.service.space.controllers;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.avro.AvroRemoteException;
import org.librairy.service.space.facade.model.SpaceService;
import org.librairy.service.space.rest.model.ComparisonRequest;
import org.librairy.service.space.rest.model.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@RestController
@RequestMapping("/summary")
@Api(tags = "/summary", description = "network-level operations")
public class RestSummaryController {

    private static final Logger LOG = LoggerFactory.getLogger(RestSummaryController.class);

    @Autowired
    SpaceService service;

    @PostConstruct
    public void setup(){

    }

    @PreDestroy
    public void destroy(){

    }

    @ApiOperation(value = "stats about points and clusters", nickname = "getSummary", response=Summary.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Summary.class),
    })
    @RequestMapping(method = RequestMethod.GET, produces = "application/json")
    public Summary get()  {
        try {
            org.librairy.service.space.facade.model.Summary summary = service.getSummary();
            Summary response = new Summary(summary);
            return response;
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        }
    }

}
