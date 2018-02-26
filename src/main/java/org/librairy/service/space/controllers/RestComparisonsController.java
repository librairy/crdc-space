package org.librairy.service.space.controllers;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.avro.AvroRemoteException;
import org.librairy.service.space.facade.model.SpaceService;
import org.librairy.service.space.rest.model.ComparisonRequest;
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
@RequestMapping("/comparisons")
@Api(tags = "/comparisons", description = "vector-level operations")
public class RestComparisonsController {

    private static final Logger LOG = LoggerFactory.getLogger(RestComparisonsController.class);

    @Autowired
    SpaceService service;

    @PostConstruct
    public void setup(){

    }

    @PreDestroy
    public void destroy(){

    }

    @ApiOperation(value = "compare two vectors", nickname = "postCompare", response=Double.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Double.class),
    })
    @RequestMapping(method = RequestMethod.POST, produces = "application/json")
    public Double compare(@RequestBody ComparisonRequest request)  {
        try {
            return service.compare(request.getShape1(),request.getShape2());
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        }
    }

}
