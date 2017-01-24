package net.tcs.db;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.javalite.activejdbc.Base;

import com.mchange.v2.c3p0.DataSources;

public class ActiveJDBCAdapter {

    private volatile DataSource dataSource;

    public void initialize(String dbConnectURL, String user, String password) {
        try {
            final DataSource dataSourceUnpooled = DataSources.unpooledDataSource(dbConnectURL, user, password);

            /*
             * Check if a connection could be opened to DB
             */
            final Connection conn = dataSourceUnpooled.getConnection(user, password);
            conn.close();

            dataSource = DataSources.pooledDataSource(dataSourceUnpooled);
        } catch (final SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize ActiveJDBCAdapter", e);
        }
    }

    /**
     * Get Connection from Pool
     */
    public void openConnection() {
        Base.open(dataSource);
    }

    /**
     * Return Connection to Pool
     */
    public void closeConnection() {
        Base.close();
    }

    public void cleanup() {
        if (dataSource != null) {
            try {
                DataSources.destroy(dataSource);
            } catch (final SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void beginTX() {
        Base.openTransaction();
    }

    public void commitTX() {
        Base.commitTransaction();
    }

    public void rollbackTX() {
        Base.rollbackTransaction();
    }
}
