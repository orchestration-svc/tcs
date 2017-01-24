package com.task.coordinator.controller;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "JobDefinition")
public class JobDefinitionDAO {
    public JobDefinitionDAO(String name) {
        this.name = name;
    }

    public JobDefinitionDAO() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    @Id
    @Column(name="name")
    private String name;

    @Column(name="body", columnDefinition="TEXT")
    private String body;
}
