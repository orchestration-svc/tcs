package net.tcs.db;

import java.util.Date;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;
import org.javalite.activejdbc.annotations.VersionColumn;

@Table("TaskInstance")
@IdName("instanceId")
@VersionColumn("lock_version")
public class TaskInstanceDAO extends Model {
    static {
        validatePresenceOf("name", "instanceId", "shardId", "state", "startTime");
    }

    public String getInstanceId() {
        return getString("instanceId");
    }

    public void setInstanceId(String instanceId) {
        setString("instanceId", instanceId);
    }

    public String getJobInstanceId() {
        return getString("jobInstanceId");
    }

    public void setJobInstanceId(String jobInstanceId) {
        setString("jobInstanceId", jobInstanceId);
    }

    public String getShardId() {
        return getString("shardId");
    }

    public void setShardId(String shardId) {
        setString("shardId", shardId);
    }

    public String getState() {
        return getString("state");
    }

    public void setState(String state) {
        setString("state", state);
    }

    public int getRetryCount() {
        return getInteger("retryCount");
    }

    public void setRetryCount(int retryCount) {
        setInteger("retryCount", retryCount);
    }

    public Date getStartTime() {
        return getTimestamp("startTime");
    }

    public void setStartTime(Date startTime) {
        this.setTimestamp("startTime", startTime);
    }

    public Date getCompletionTime() {
        return getTimestamp("completionTime");
    }

    public void setCompletionTime(Date completionTime) {
        this.setTimestamp("completionTime", completionTime);
    }

    public Date getUpdateTime() {
        return getTimestamp("updateTime");
    }

    public void setUpdateTime(Date updateTime) {
        this.setTimestamp("updateTime", updateTime);
    }

    public String getName() {
        return getString("name");
    }

    public void setName(String name) {
        setString("name", name);
    }

    public String getRollbackState() {
        return getString("rollbackState");
    }

    public void setRollbackState(String rollbackState) {
        setString("rollbackState", rollbackState);
    }

    public Date getRollbackStartTime() {
        return getTimestamp("rollbackStartTime");
    }

    public void setRollbackStartTime(Date rollbackStartTime) {
        this.setTimestamp("rollbackStartTime", rollbackStartTime);
    }

    public Date getRollbackCompletionTime() {
        return getTimestamp("rollbackCompletionTime");
    }

    public void setRollbackCompletionTime(Date rollbackCompletionTime) {
        this.setTimestamp("rollbackCompletionTime", rollbackCompletionTime);
    }

    public int getParallelExecutionIndex() {
        return getInteger("parallelExecutionIndex");
    }

    public void setParallelExecutionIndex(int parallelExecutionIndex) {
        setInteger("parallelExecutionIndex", parallelExecutionIndex);
    }
}
