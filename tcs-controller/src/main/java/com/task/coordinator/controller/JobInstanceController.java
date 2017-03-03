package com.task.coordinator.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.task.coordinator.dto.JobInstanceDTO;

@Controller
public class JobInstanceController {

    @Autowired
    JobInstanceRepository repository;

    private final ObjectMapper mapper = new ObjectMapper();

    @RequestMapping(value = "/tcs/jobs", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<List<JobInstanceDTO>> getAllJobInstances() {
        final List<JobInstanceDTO> items = new ArrayList<>();
        for (final JobInstanceDAO item : repository.findAll()) {
            final JobInstanceDTO dto = toDTO(item);
            items.add(dto);
        }
        return new ResponseEntity<List<JobInstanceDTO>>(items, HttpStatus.OK);
    }

    @RequestMapping(value = "/tcs/job/{instanceId:.+}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<JobInstanceDTO> getJobInstance(@PathVariable String instanceId) {
        final JobInstanceDAO job = repository.findOne(instanceId);

        if (job == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        final JobInstanceDTO jobDTO = toDTO(job);
        return new ResponseEntity<JobInstanceDTO>(jobDTO, HttpStatus.OK);
    }

    @RequestMapping(value = "/tcs/jobsByState", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<List<JobInstanceDTO>> getJobInstancesInState(@RequestParam("state") final String state) {
        if (!validateState(state)) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }
        final List<JobInstanceDAO> jobInstances = repository.findByState(state);

        if (jobInstances != null) {
            final List<JobInstanceDTO> jobDTOList = new ArrayList<>();

            for (final JobInstanceDAO job : jobInstances) {
                jobDTOList.add(toDTO(job));
            }
            return new ResponseEntity<List<JobInstanceDTO>>(jobDTOList, HttpStatus.OK);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping(value = "/tcs/jobs/count", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<String> getJobInstanceCount() {
        return new ResponseEntity<String>("{\"count\": "+ repository.count() + "}", HttpStatus.OK);
    }

    private static boolean validateState(String state) {
        return (StringUtils.equalsIgnoreCase("READY", state) || StringUtils.equalsIgnoreCase("INPROGRESS", state)
                || StringUtils.equalsIgnoreCase("COMPLETE", state) || StringUtils.equalsIgnoreCase("FAILED", state)
                || StringUtils.equalsIgnoreCase("ROLLBACK_INPROGRESS", state)
                || StringUtils.equalsIgnoreCase("ROLLBACK_COMPLETE", state)
                || StringUtils.equalsIgnoreCase("ROLLBACK_FAILED", state));
    }

    private JobInstanceDTO toDTO(JobInstanceDAO job) {
        if (job == null) {
            return new JobInstanceDTO();
        }

        Map<String, String> map;
        try {
            map = mapper.readValue(job.getJobContext(), HashMap.class);
        } catch (final IOException e) {
            map = null;
        }

        final JobInstanceDTO jobDTO = new JobInstanceDTO(job);
        jobDTO.setJobContext(map);
        return jobDTO;
    }
}
