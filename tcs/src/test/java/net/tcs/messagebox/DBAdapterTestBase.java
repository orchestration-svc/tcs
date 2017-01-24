package net.tcs.messagebox;

import java.io.IOException;
import java.sql.SQLException;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.tcs.db.ActiveJDBCEmbeddedDBAdapter;
import net.tcs.db.adapter.JobInstanceDBAdapter;
import net.tcs.db.adapter.TaskInstanceDBAdapter;
import net.tcs.drivers.TCSDriver;

public class DBAdapterTestBase extends ActiveJDBCEmbeddedDBAdapter {
    protected final ObjectMapper mapper = new ObjectMapper();
    protected final JobInstanceDBAdapter jobInstanceAdapter = new JobInstanceDBAdapter();
    protected final TaskInstanceDBAdapter taskInstanceAdapter = new TaskInstanceDBAdapter();

    public void setup() throws ClassNotFoundException, SQLException, IOException {
        super.initialize(ActiveJDBCEmbeddedDBAdapter.H2_EMBEDDED_DB_URL, "root", "root");
        TCSDriver.setDbAdapter(this);
    }

    @Override
    public void cleanup() {
        try {
            super.dbConnection.createStatement().execute("SHUTDOWN");
            super.cleanup();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
