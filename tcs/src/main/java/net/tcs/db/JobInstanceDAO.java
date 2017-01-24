package net.tcs.db;

import java.util.Date;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;
import org.javalite.activejdbc.annotations.VersionColumn;

@Table("JobInstance")
@IdName("instanceId")
@VersionColumn("lock_version")
public class JobInstanceDAO extends Model {
    static {
        validatePresenceOf("name", "instanceId", "shardId", "state", "startTime");
    }

    public String getInstanceId() {
        return getString("instanceId");
    }

    public void setInstanceId(String instanceId) {
        setString("instanceId", instanceId);
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

    public String getName() {
        return getString("name");
    }

    public void setName(String name) {
        setString("name", name);
    }

    public String getJobNotificationUri() {
        return getString("jobNotificationUri");
    }

    public void setJobNotificationUri(String jobNotificationUri) {
        setString("jobNotificationUri", jobNotificationUri);
    }

    public String getJobContext() {
        return getString("jobContext");
    }

    public void setJobContext(String jobContext) {
        setString("jobContext", jobContext);
    }
}
