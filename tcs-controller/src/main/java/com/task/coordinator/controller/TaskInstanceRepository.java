package com.task.coordinator.controller;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

public interface TaskInstanceRepository extends CrudRepository<TaskInstanceDAO, String> {
    public List<TaskInstanceDAO> findByJobInstanceId(String jobInstanceId);

    public List<TaskInstanceDAO> findByJobInstanceIdAndState(String jobInstanceId, String state);
}
