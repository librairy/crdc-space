package org.librairy.service.space.controllers;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.avro.AvroRemoteException;
import org.librairy.service.space.rest.model.NeighbourList;
import org.librairy.service.space.rest.model.NeighboursRequest;
import org.librairy.service.space.rest.model.Point;
import org.librairy.service.space.rest.model.PointList;
import org.librairy.service.space.services.MyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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

    @ApiOperation(value = "add to space", nickname = "postAdd", response=Void.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Void.class),
    })
    @RequestMapping(value = "/points", method = RequestMethod.POST, produces = "application/json")
    public ResponseEntity<Void> add(@RequestBody Point point)  {
        try {
            if (!service.addPoint(point)) {
                LOG.warn("Invalid vector size");
                return new ResponseEntity<Void>(HttpStatus.BAD_REQUEST);
            }
            return new ResponseEntity<Void>(HttpStatus.OK);
        } catch (AvroRemoteException e) {
            LOG.error("AVRO error",e);
            return new ResponseEntity(HttpStatus.FAILED_DEPENDENCY);
        } catch(Exception e){
            LOG.error("unexpected error",e);
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "read", response=Point.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Point.class),
    })
    @RequestMapping(value = "/points/{id:.+}", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<Point> get(@PathVariable("id") String id)  {
        try {
            return new ResponseEntity(new Point(service.getPoint(id)), HttpStatus.ACCEPTED);
        } catch(RuntimeException e){
            // point not found
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        } catch (AvroRemoteException e) {
            LOG.error("AVRO error",e);
            return new ResponseEntity(HttpStatus.FAILED_DEPENDENCY);
        } catch(Exception e){
            LOG.error("unexpected error",e);
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "remove", response=Boolean.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Void.class),
    })
    @RequestMapping(value = "/points/{id:.+}", method = RequestMethod.DELETE, produces = "application/json")
    public ResponseEntity<Void> remove(@PathVariable String id)  {
        try {
            return new ResponseEntity(service.removePoint(id), HttpStatus.ACCEPTED);
        } catch (AvroRemoteException e) {
            LOG.error("AVRO error",e);
            return new ResponseEntity(HttpStatus.FAILED_DEPENDENCY);
        } catch(Exception e){
            LOG.error("unexpected error",e);
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "remove all", response=Boolean.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Void.class),
    })
    @RequestMapping(value = "/points", method = RequestMethod.DELETE, produces = "application/json")
    public ResponseEntity<Void> removeAll()  {
        try {
            return new ResponseEntity(service.removeAll(),HttpStatus.ACCEPTED);
        } catch (AvroRemoteException e) {
            LOG.error("AVRO error",e);
            return new ResponseEntity(HttpStatus.FAILED_DEPENDENCY);
        } catch(Exception e){
            LOG.error("unexpected error",e);
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "list of", response=PointList.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = PointList.class),
    })
    @RequestMapping(value = "/points", method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<PointList> list(@RequestParam(defaultValue = "10",required = true) Integer size, @RequestParam(defaultValue = "",required = false) String offset)  {
        try {
            org.librairy.service.space.facade.model.PointList result = service.listPoints(size, offset);
            return new ResponseEntity(new PointList(result.getNextPage(), result.getPoints().stream().map(p -> new org.librairy.service.space.rest.model.Point(p)).collect(Collectors.toList())),HttpStatus.ACCEPTED);
        } catch (AvroRemoteException e) {
            LOG.error("AVRO error",e);
            return new ResponseEntity(HttpStatus.FAILED_DEPENDENCY);
        } catch(Exception e){
            LOG.error("unexpected error",e);
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "list close to a given one", response=NeighbourList.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = NeighbourList.class),
    })
    @RequestMapping(value = "/points/{id:.+}/neighbours", method = RequestMethod.POST, produces = "application/json")
    public ResponseEntity<NeighbourList> neighbours(@PathVariable("id") String id, @RequestBody NeighboursRequest request)  {
        try {
            return new ResponseEntity(new NeighbourList(service.getNeighbours(id,request.getNumber(),request.getTypes(), request.getForce()).stream().map(p -> new org.librairy.service.space.rest.model.Neighbour(p)).collect(Collectors.toList())),HttpStatus.ACCEPTED);
        } catch (AvroRemoteException e) {
            LOG.error("AVRO error",e);
            return new ResponseEntity(HttpStatus.FAILED_DEPENDENCY);
        } catch(Exception e){
            LOG.error("unexpected error",e);
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}
