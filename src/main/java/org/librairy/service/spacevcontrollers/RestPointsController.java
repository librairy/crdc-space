package org.librairy.service.space.controllers;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.avro.AvroRemoteException;
import org.librairy.service.space.rest.model.NeighbourList;
import org.librairy.service.space.rest.model.Point;
import org.librairy.service.space.rest.model.PointList;
import org.librairy.service.space.services.MyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.stream.Collectors;

@RestController
//@RequestMapping("/points")
@Api(tags="/points", description="points-level operations")
public class RestPointsController {

    private static final Logger LOG = LoggerFactory.getLogger(RestPointsController.class);

    @Autowired
    MyService service;

    @PostConstruct
    public void setup(){

    }

    @PreDestroy
    public void destroy(){

    }

    @ApiOperation(value = "add to space", nickname = "postAdd", response=Boolean.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Boolean.class),
    })
    @RequestMapping(value = "/points", method = RequestMethod.POST, produces = "application/json")
    public Boolean add(@RequestBody Point point)  {
        try {
            return service.addPoint(point);
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @ApiOperation(value = "read", response=Point.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Point.class),
    })
    @RequestMapping(value = "/points/{id:.+}", method = RequestMethod.GET, produces = "application/json")
    public Point get(@PathVariable("id") String id)  {
        try {
            return new Point(service.getPoint(id));
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @ApiOperation(value = "remove", response=Boolean.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Boolean.class),
    })
    @RequestMapping(value = "/points/{id:.+}", method = RequestMethod.DELETE, produces = "application/json")
    public Boolean remove(@PathVariable String id)  {
        try {
            return service.removePoint(id);
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @ApiOperation(value = "remove all", response=Boolean.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Boolean.class),
    })
    @RequestMapping(value = "/points", method = RequestMethod.DELETE, produces = "application/json")
    public Boolean removeAll()  {
        try {
            return service.removeAll();
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @ApiOperation(value = "list of", response=PointList.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = PointList.class),
    })
    @RequestMapping(value = "/points", method = RequestMethod.GET, produces = "application/json")
    public PointList list(@RequestParam Integer size, @RequestParam(defaultValue = "") String offset)  {
        try {
            return new PointList(service.listPoints(size,offset).stream().map(p -> new org.librairy.service.space.rest.model.Point(p)).collect(Collectors.toList()));
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        }
    }

    @ApiOperation(value = "list close to a given one", response=NeighbourList.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = NeighbourList.class),
    })
    @RequestMapping(value = "/points/{id:.+}/neighbours", method = RequestMethod.GET, produces = "application/json")
    public NeighbourList neighbours(@PathVariable("id") String id, @RequestParam Integer number, @RequestParam String type)  {
        try {
            return new NeighbourList(service.getNeighbours(id,number,type).stream().map(p -> new org.librairy.service.space.rest.model.Neighbour(p)).collect(Collectors.toList()));
        } catch (AvroRemoteException e) {
            throw new RuntimeException(e);
        }
    }

}
