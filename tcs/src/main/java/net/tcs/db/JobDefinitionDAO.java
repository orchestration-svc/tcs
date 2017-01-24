package net.tcs.db;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;
import org.javalite.activejdbc.annotations.VersionColumn;

@Table("JobDefinition")
@IdName("name")
@VersionColumn("lock_version")
public class JobDefinitionDAO extends Model {
    static {
        validatePresenceOf("name", "body");
    }
    public String getName() {
        return getString("name");
    }

    public void setName(String name) {
        set("name", name);
    }

    public String getBody() {
        return getString("body");
    }

    public void setBody(String body) {
        set("body", body);
    }
}
