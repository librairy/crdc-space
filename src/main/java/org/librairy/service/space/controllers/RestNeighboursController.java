package org.librairy.service.space.controllers;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.avro.AvroRemoteException;
import org.librairy.service.space.facade.model.SpaceService;
import org.librairy.service.space.rest.model.NeighbourList;
import org.librairy.service.space.rest.model.SimilarRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/neighbours")
@Api(tags = "/neighbours", description = "shape-level operations")
public class RestNeighboursController {

    private static final Logger LOG = LoggerFactory.getLogger(RestNeighboursController.class);

    @Autowired
    SpaceService service;

    @PostConstruct
    public void setup(){

    }

    @PreDestroy
    public void destroy(){

    }

    @ApiOperation(value = "list points close to a given vector", nickname = "postSimilar", response=NeighbourList.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = NeighbourList.class),
    })
    @RequestMapping(method = RequestMethod.POST, produces = "application/json")
    public NeighbourList similar(@RequestBody SimilarRequest request)  {
        try {
            return new NeighbourList(service.getSimilar(request.getShape(), request.getNumber(), request.getType()).stream().map(n-> new org.librairy.service.space.rest.model.Neighbour(n)).collect(Collectors.toList()));
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        }
    }

}
