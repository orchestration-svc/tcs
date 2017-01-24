CREATE DATABASE tcsdb;
USE tcsdb;
CREATE TABLE JobDefinition (name VARCHAR(255) NOT NULL, body text, lock_version INT, PRIMARY KEY (name));
CREATE TABLE JobInstance (instanceId VARCHAR(255) NOT NULL, name VARCHAR(255), shardId VARCHAR(255), state VARCHAR(255), startTime DATETIME DEFAULT CURRENT_TIMESTAMP, completionTime DATETIME NULL, jobNotificationUri VARCHAR(255), jobContext text, lock_version INT, PRIMARY KEY (instanceId));
CREATE TABLE TaskInstance (instanceId VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, jobInstanceId VARCHAR(255) NOT NULL, shardId VARCHAR(255), state VARCHAR(255), rollbackState VARCHAR(255), retryCount INT DEFAULT 0, startTime DATETIME NULL, completionTime DATETIME NULL, updateTime DATETIME NULL, rollbackStartTime DATETIME NULL, rollbackCompletionTime DATETIME NULL, parallelExecutionIndex INT DEFAULT 0, lock_version INT, PRIMARY KEY (instanceId));
CREATE TABLE TaskInstanceData (instanceId VARCHAR(255) NOT NULL, taskInput text, taskOutput text, lock_version INT, PRIMARY KEY (instanceId));

