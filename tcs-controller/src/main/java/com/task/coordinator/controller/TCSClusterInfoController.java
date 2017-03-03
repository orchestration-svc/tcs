package com.task.coordinator.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@Controller
public class TCSClusterInfoController {
    @Autowired
    private TCSClusterObserver observer;

    @RequestMapping(value = "/tcs/clusterinfo", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<Map<String, List<String>>> getClusterInfo() {
        return new ResponseEntity<Map<String, List<String>>>(observer.getShardInfo(), HttpStatus.OK);
    }
}
