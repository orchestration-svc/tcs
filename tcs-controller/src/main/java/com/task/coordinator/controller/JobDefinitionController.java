package com.task.coordinator.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.task.coordinator.core.JobDefinitionTopologicalSorter;
import com.task.coordinator.dto.JobDefinitionDTO;
import com.task.coordinator.dto.TaskDefinitionDTO;

import net.tcs.task.JobDefinition;
import net.tcs.task.TaskDefinition;

@Controller
public class JobDefinitionController {

    @Autowired
    JobDefintionRepository repository;

    private final ObjectMapper mapper = new ObjectMapper();

    @RequestMapping(value = "/tcs/jobSpecs", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<List<JobDefinitionDTO>> getAllJobDefinitions()
            throws JsonParseException, JsonMappingException, IOException {
        final List<JobDefinitionDTO> items = new ArrayList<>();
        for ( final JobDefinitionDAO item : repository.findAll()) {
            final JobDefinition job = mapper.readValue(item.getBody(), JobDefinition.class);
            items.add(toDTO(job));
        }
        return new ResponseEntity<List<JobDefinitionDTO>>(items, HttpStatus.OK);
    }

    @RequestMapping(value = "/tcs/jobSpec/{jobName:.+}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<JobDefinitionDTO> getJobDefinition(@PathVariable String jobName)
            throws JsonParseException, JsonMappingException, IOException {
        final JobDefinitionDAO jobDAO = repository.findOne(jobName);

        if (jobDAO == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        final JobDefinition result = mapper.readValue(repository.findOne(jobName).getBody(), JobDefinition.class);
        final JobDefinitionDTO jobDTO = toDTO(result);
        return new ResponseEntity<JobDefinitionDTO>(jobDTO, HttpStatus.OK);
    }

    @RequestMapping(value = "/tcs/jobSpec/count", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<String> getJobCount() {
        return new ResponseEntity<String>("{\"count\": "+ repository.count() + "}", HttpStatus.OK);
    }

    private static JobDefinitionDTO toDTO(JobDefinition job) {
        if (job == null) {
            return new JobDefinitionDTO();
        }

        final JobDefinitionTopologicalSorter topologicalSorter = new JobDefinitionTopologicalSorter(job);
        final List<String> sortedTasks = topologicalSorter.topologicalSort();

        final JobDefinitionDTO jobDTO = new JobDefinitionDTO(job);

        final Map<String, String> taskToStepMap = new HashMap<>();
        final Map<String, List<String>> stepMap = job.getSteps();

        for (final Entry<String, List<String>> entry : stepMap.entrySet()) {
            for (final String task : entry.getValue()) {
                taskToStepMap.put(task, entry.getKey());
            }
        }

        for (final String taskName : sortedTasks) {
            final TaskDefinition taskDefinition = (TaskDefinition) job.getTaskSpec(taskName);
            final TaskDefinitionDTO taskDefDTO = new TaskDefinitionDTO(taskDefinition);
            if (!taskToStepMap.isEmpty()) {
                taskDefDTO.setStepName(taskToStepMap.get(taskName));
            }
            jobDTO.addTask(taskDefDTO);
        }
        return jobDTO;
    }
}
