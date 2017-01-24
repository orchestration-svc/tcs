package com.task.coordinator.controller;

import org.springframework.data.repository.CrudRepository;

public interface TaskInstanceDataRepository extends CrudRepository<TaskInstanceDataDAO, String> {
}
