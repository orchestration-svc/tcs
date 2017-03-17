### Demo Prerequisites
[Build and Run TCS](https://github.com/orchestration-svc/tcs/wiki/Run-TCS-microservices)

[Run RabbitMQ, ZooKeeper and MySQL](https://github.com/orchestration-svc/tcs/wiki/Running-infra-services-in-Docker)

### Register Job specification

    cd tcs-demo
    java -cp target/tcs-demo-1.0.jar -DrmqIP=192.168.99.100 net.tcs.TCSJobRegisterMain

### Run task executors

Open four terminal windows.

    cd tcs-demo
    java -cp target/tcs-demo-1.0.jar -DrmqIP=192.168.99.100 net.tcs.TCSDemoTaskExecutor taskA
    java -cp target/tcs-demo-1.0.jar -DrmqIP=192.168.99.100 net.tcs.TCSDemoTaskExecutor taskB
    java -cp target/tcs-demo-1.0.jar -DrmqIP=192.168.99.100 net.tcs.TCSDemoTaskExecutor taskC
    java -cp target/tcs-demo-1.0.jar -DrmqIP=192.168.99.100 net.tcs.TCSDemoTaskExecutor taskD

### Run Job Submitter

    cd tcs-demo
    java -cp target/tcs-demo-1.0.jar -DrmqIP=192.168.99.100 net.tcs.TCSDemoJobSubmitter

