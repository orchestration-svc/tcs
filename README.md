## Overview

TCS is a Distributed Task Orchestration Service. It orchestrates a Job across various task executors. TCS communicates with the task executor services using RabbitMQ.

![](https://github.com/orchestration-svc/tcs/blob/master/images/tcs.jpg)

## Wiki Links
[Build and Run TCS](https://github.com/orchestration-svc/tcs/wiki/How-to-run-TCS)

[Build and Run TCS Controller](https://github.com/orchestration-svc/tcs/wiki/How-to-run-TCS-REST-controller)

[TCSClient API](https://github.com/orchestration-svc/tcs/wiki/TCS-Client-API)

[TCS Controller REST API](https://github.com/orchestration-svc/tcs/wiki/TCS-REST-Controller-API)

[TCS Deployment Modes](https://github.com/orchestration-svc/tcs/wiki/TCS-Deployment-Modes)

[Run TCS in Docker](https://github.com/orchestration-svc/tcs/wiki/Run-TCS-as-a-Docker-container)

[Run TCS Controller in Docker](https://github.com/orchestration-svc/tcs/wiki/Running-TCS-REST-controller-as-a-Docker-container)

[Run infra services in Docker](https://github.com/orchestration-svc/tcs/wiki/Running-infra-services-in-Docker)


## Common Terminologies

#### Job
A Job is a Directed Acyclic Graph of Tasks.

#### Task
A Task is a unit of execution. It is part of a Job. A task is ready for execution, when all its predecessor tasks have completed execution. Each tasks's output is passed as input to its successor task(s).

#### JobSpec and TaskSpec
A JobSpec (Job specification) is a blue-print for a Job. A TaskSpec (Task specification) is a blue-print for a Task. A JobSpec contains a set of TaskSpecs, and the dependency association between the Tasks, in the form of a DAG.

#### JobInstance
An instantiation of a JobSpec.

#### Task Execution Endpoint
A RabbitMQ endpoint that the TaskExecutors bind to, in order to receive Task messages from TCS. The Task Execution endpoint URI is specified in the Task specification.

#### DAG
A Job consists of a set of Tasks whose interdependency can be specified as a Directed Acyclic Graph, or DAG. As TCS orchestrates a Job execution, it traverses the DAG, and dispatches the Tasks to the respective TaskExecutor services for execution. A Task is dispatched for execution, when all its predecessor Tasks have completed execution.

## Pariticipants/Services

#### TCS
TCS is a microservice that performs Job orchestration. It orchestrates a Job execution across multiple TaskExecutor services. TCS communicates with Task executor Services over RabbitMQ.

#### JobSubmitter
JobSubmitter is a TCSClient that initiates a Job execution.

#### TaskExecutor
TaskExecutor is a TCSClient that is a participant in the Job execution. It is responsible for executing one or more tasks. It is possible for the same service to act as both JobSubmitter and TaskExecutor.

#### JobNotification Listener
A service that subscribes to various Job notifications, such as: JobComplete, JobFailed, JobRolledBack.

#### TCS Controller
TCS-Controller is a microservice that provides REST APIs to query various Job specification and execution information.

## Life of a Job

### JobSpec Registration
A one-time JobSpec registration needs to be done with TCS, by providing a Job specification (blueprint).

A JobSpec consists of a Job name, and a DAG of TaskSpecs.
Each TaskSpec consists of a Task name, and a Task execution endpoint URI, in the form of rmq://exchange/routingKey
An TaskExecutor Service that intends to execute a Task, subscribes to the Task execution URI.

![](https://github.com/orchestration-svc/tcs/blob/master/images/job_dag.jpg)

### Running a Job
A JobSubmitter Service submits an instance of Job for execution to TCS.

TCS starts the Job orchestration, by dispatching the root task(s) to the corresponding TaskExecutor Service(s).
When a TaskExecutor has completed execution of a Task, it sends a task completion notification to TCS.

As a Task becomes complete, TCS dispatches its successor tasks for execution, if all their predecessor tasks are complete.

When all Tasks are complete, TCS marks the Job instance as complete.

When one or more Task(s) fail, TCS marks the Job instance as failed.

### Role of TaskExecutor
The interaction between TCS and TaskExecutor services are asynchronous and message-driven. All the TCS needs to know is the TaskExecutionURI to dispatch the task.

When a Task is ready for execution, the TaskExecutor receives a **BeginTask** notification from TCS. Once it successfully completes the Task, it sends a **TaskCompletion** notification back to TCS. If the Task fails, it sends a **TaskFailed** notification back to TCS.

### Task Retry
If a Task is not complete within a certain configurable period, then TCS dispatches a **TaskRetry** notification to the TaskExecutor.

The TaskExecutor Service can periodically send a **TaskInProgress** heartbeat message, to let TCS know that the Task execution is still in progress. This prevents Task retry from happening.

### Rollback of a Job
TCS provides mechanism to roll back a Failed Job instance. Only Tasks that have succeeded before, are rolled back.
Tasks are rolled back in the reverse order, by reversing the DAG.

### TCS protocol messages
#### BeginJob
JobSubmitter sends BeginJob message to TCS to begin a Job instance.

#### BeginTask
TCS sends BeginTask message to the TaskExecutor, when the Task's predecessors are done.

#### TaskComplete
TaskExecutor sends TaskComplete message to TCS, upon task completion.

#### TaskFailed
TaskExecutor sends TaskFailed message to TCS, when task execution fails.

#### JobComplete
TCS sends JobComplete message to JobStatusListener, when Job execution completes successfully.

#### JobFailed
TCS sends JobFailed message to JobStatusListener, when Job execution fails.

#### JobRolledBack
TCS sends JobRolledBack message to JobStatusListener, when Job rollback completes.

#### TaskRetry
TCS sends TaskRetry message to TaskExecutor, to retry task execution, if the task did not complete before timeout.

#### TaskInProgress
TaskExecutor sends TaskInProgress message to TCS, as a heartbeat mechanism to indicate that the Task execution is still in progress, and that TCS should not send a TaskRetry notification.

![](https://github.com/orchestration-svc/tcs/blob/master/images/job_execution.jpg)

## TCS Architecture

TCS is a microservice, that communicates with TaskExecutors over RabbitMQ.

It uses MySQL to store Job specifications and Job runtime information, such as JobInstance, TaskInstance.

[TCS Deployment modes](https://github.com/orchestration-svc/tcs/wiki/TCS-Deployment-Modes)

![](https://github.com/orchestration-svc/tcs/blob/master/images/tcs_arch.jpg)

## TCS Clustered Deployment

TCS is designed to run as a multi-node cluster. It has two clustered deployment modes: MULTI_INSTANCE, and ACTIVE_STANDBY.

MULTI_INSTANCE is the default deployment mode.

TCS uses Apache ZooKeeper to store cluster-metadata. It uses [Apache Helix](http://helix.apache.org/) for cluster state management and coordination.

TCS implements the concept of partitions, in order to provide scale-out, fault-tolerance and load-balancing capability. 

### Partitions

A Partition (also known as a Shard) is a mechanism to ensure that a Job instance is processed by one and only one TCS node.

A Partition can be thought of as a swim-lane. When a Job instance is submitted for execution, it is placed into one of the partitions. The Job instance is associated with this partition during the lifetime of its execution.

In a multi-node TCS cluster, a partition is exclusively owned by only one TCS node at any point of time. This node is responsible for processing DAGs for all running Jobs in the partition.

### Partitions placement in TCS Nodes

In a cluster deployment, there is usually a large number of TCS partitions and a small (variable) number of TCS nodes.

Helix controller (embedded with TCS runtime) ensures that the partitions are uniformly distributed among the running TCS nodes.

Partitions are automatically rebalanced whenever there is a change in cluster members, i.e, when a TCS node joins the cluster, or leaves the cluster.

The number of partitions can also be dynamically increased.

[Grow Partition count in TCS cluster](https://github.com/orchestration-svc/tcs/wiki/Grow-Partition-count-in-TCS-Cluster)

A partition is never without a owner. Which means that, a partition is always assigned to one of the running TCS nodes.

![](https://github.com/orchestration-svc/tcs/blob/master/images/tcs_paritions.jpg)

### JobInstance placement in a partition

As of now, a Job instance is placed in a partition that is randomly chosen. However, in future, we intend to select a partition that has the least number of running Jobs associated. Once a Job is placed in a partition, it is always associated with that partition. During a partition-rebalancing (as a result of cluster membership change), a partition could move from one node to another node. Thus all the Job DAG processing also moves from one partition to another partition.

### How Partitions work

As mentioned before, partitions could be thought of as swim-lanes. A partition is associated with a set of in-bound RabbitMQ queues. All the inbound messages to TCS (from TaskExecutors) are published with a partition-specific routing key; thus the messages land in inbound RabbitMQ queues associated with the partition.

This concept of swim-lanes ensures that, all the job-specific protocol messages are routed to the partition that owns the running job instances.

![](https://github.com/orchestration-svc/tcs/blob/master/images/tcs_partitions_rmq.jpg)

### Active-Standby

In this deployment, only one TCS node is active, and owns all the partitions. All the remaining nodes are in standby mode. When the active node dies, one of the standby nodes is promoted to become active, and takes ownership of all the partitions.

## Non-clustered Deployments

### Standalone

Only one instance of TCS can be run in this mode. Since there is no clustering, it does not require ZooKeeper.

### Lightweight

This deployment mode is only for testing purposes. It runs with an in-memory DB (H2 Embedded DB), and therefore does not need MySQL.

## Technologies Used

##### Apache ZooKeeper
##### Apache Helix
##### Apache Curator
##### RabbitMQ
##### MySQL
##### H2 Embedded DB
##### Spring Boot
##### ActiveJDBC

