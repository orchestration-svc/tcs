package net.tcs.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ActiveJDBCEmbeddedDBAdapter extends ActiveJDBCAdapter {
    public static String H2_EMBEDDED_DB_URL = "jdbc:h2:mem:tcsdb";

    protected Connection dbConnection = null;

    @Override
    public void initialize(String dbConnectURL, String user, String password) {
        try {

            Class.forName("org.h2.Driver");
            dbConnection = DriverManager.getConnection(H2_EMBEDDED_DB_URL, "root", "root");
            createTables();
        } catch (final SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize ActiveJDBCAdapter", e);
        } catch (final ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize ActiveJDBCAdapter", e);
        }
        super.initialize(H2_EMBEDDED_DB_URL, user, password);
    }

    private void createTables() throws SQLException {

        final Statement stmt = dbConnection.createStatement();

        try (InputStream ins = getClass().getClassLoader().getResourceAsStream("create_tcs_db.sql")) {
            final BufferedReader fis = new BufferedReader(new InputStreamReader(ins));
            String str;
            while ((str = fis.readLine()) != null) {
                if (!str.contains("CREATE DATABASE") && !str.contains("USE")) {
                    stmt.executeUpdate(str);
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to initialize ActiveJDBCAdapter", e);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (dbConnection != null) {
            try {
                dbConnection.close();
            } catch (final SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
