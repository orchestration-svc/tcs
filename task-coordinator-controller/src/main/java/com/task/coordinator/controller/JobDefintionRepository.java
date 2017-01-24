package com.task.coordinator.controller;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JobDefintionRepository extends CrudRepository<JobDefinitionDAO, String>{

}
