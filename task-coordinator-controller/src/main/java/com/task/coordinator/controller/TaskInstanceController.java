package com.task.coordinator.controller;

import java.util.ArrayList;
import java.util.List;

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

import com.task.coordinator.dto.TaskInstanceDTO;

@Controller
public class TaskInstanceController {

    @Autowired
    TaskInstanceRepository repository;

    @Autowired
    TaskInstanceDataRepository dataRepository;

    @Autowired
    JobInstanceRepository jobRepository;

    @RequestMapping(value = "/tcs/tasks/{jobInstanceId:.+}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<List<TaskInstanceDTO>> getTasks(@PathVariable String jobInstanceId) {
        final List<TaskInstanceDAO> tasks = repository.findByJobInstanceId(jobInstanceId);
        if (tasks == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        final JobInstanceDAO jobDAO = jobRepository.findOne(jobInstanceId);
        if (jobDAO == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        final List<TaskInstanceDTO> taskOutput = new ArrayList<>();

        for (final TaskInstanceDAO task : tasks) {
            final TaskInstanceDataDAO taskData = dataRepository.findOne(task.getInstanceId());
            if (taskData != null) {
                taskOutput.add(new TaskInstanceDTO(jobDAO.getName(), task, taskData));
            }
        }

        return new ResponseEntity<List<TaskInstanceDTO>>(taskOutput, HttpStatus.OK);
    }

    @RequestMapping(value = "/tcs/tasksByState/{jobInstanceId:.+}", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<List<TaskInstanceDTO>> getTasksByState(@PathVariable String jobInstanceId,
            @RequestParam("state") final String state) {
        if (!validateState(state)) {
            return new ResponseEntity<>(HttpStatus.BAD_REQUEST);
        }

        final List<TaskInstanceDAO> tasks = repository.findByJobInstanceIdAndState(jobInstanceId, state);
        if (tasks == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        final JobInstanceDAO jobDAO = jobRepository.findOne(jobInstanceId);
        if (jobDAO == null) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }

        final List<TaskInstanceDTO> taskOutput = new ArrayList<>();

        for (final TaskInstanceDAO task : tasks) {
            final TaskInstanceDataDAO taskData = dataRepository.findOne(task.getInstanceId());
            if (taskData != null) {
                taskOutput.add(new TaskInstanceDTO(jobDAO.getName(), task, taskData));
            }
        }

        return new ResponseEntity<List<TaskInstanceDTO>>(taskOutput, HttpStatus.OK);
    }

    @RequestMapping(value = "/tcs/tasks/count", method = RequestMethod.GET, produces = "application/json")
    @ResponseBody
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<String> getTaskCount() {
        return new ResponseEntity<String>("{\"count\": "+ repository.count() + "}", HttpStatus.OK);
    }

    private static boolean validateState(String state) {
        return (StringUtils.equalsIgnoreCase("INIT", state) || StringUtils.equalsIgnoreCase("INPROGRESS", state)
                || StringUtils.equalsIgnoreCase("COMPLETE", state) || StringUtils.equalsIgnoreCase("FAILED", state)
                || StringUtils.equalsIgnoreCase("WAITING", state));
    }
}
