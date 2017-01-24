package net.tcs.db;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;
import org.javalite.activejdbc.annotations.VersionColumn;

@Table("TaskInstanceData")
@IdName("instanceId")
@VersionColumn("lock_version")
public class TaskInstanceDataDAO extends Model {
    static {
        validatePresenceOf("instanceId");
    }

    public String getInstanceId() {
        return getString("instanceId");
    }

    public void setInstanceId(String instanceId) {
        setString("instanceId", instanceId);
    }

    public String getTaskInput() {
        return getString("taskInput");
    }

    public void setTaskInput(String taskInput) {
        setString("taskInput", taskInput);
    }

    public String getTaskOutput() {
        return getString("taskOutput");
    }

    public void setTaskOutput(String taskOutput) {
        setString("taskOutput", taskOutput);
    }
}
