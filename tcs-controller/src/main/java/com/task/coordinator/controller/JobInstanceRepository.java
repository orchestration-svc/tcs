package com.task.coordinator.controller;

import java.util.List;

import org.springframework.data.repository.CrudRepository;

public interface JobInstanceRepository extends CrudRepository<JobInstanceDAO, String> {
    public List<JobInstanceDAO> findByState(String state);
}
