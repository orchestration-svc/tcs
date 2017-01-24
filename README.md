TCS
===

TCS is a Distributed Task Coordination Service.

A Job is a Directed-Acyclic-Graph of Tasks.


Register a Job Specification (JobSpec)
--------------------------------------

A JobSpec needs to be registered with TCS, prior to execution.

A JobSpec includes a set of TaskSpecs.

Each TaskSpec includes a set of parent task-name(s) and a TaskExecutionTarget URI.


Execute a Job with TCS
----------------------

A runtime instance of a JobSpec is called JobInstance.

TCS client(s) act as TaskExecutionTarget endpoints. It first needs to prepare to execute a Job.

A TCS client submits a JobInstance to TCS for execution.

TCS starts executing the ready task(s) by sending BeginTask notification to TaskExecutionTarget endpoint.

A task is ready for execution when its parent tasks have completed execution.

Upon task execution completion, TCS client sends TaskComplete notification to TCS.

When all the tasks are complete, TCS marks the JobInstance as complete, and notifies all the TaskExecutionTarget endpoints.


[TCS Client API] (http://gitlab.cisco.com/tejdas/apic-tcs/wikis/tcs-client-api)
===============================================================================

