package hs.database.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Provider;

public class DatabaseUpdater {
  private static final Logger LOGGER = Logger.getLogger(DatabaseUpdater.class.getName());

  private final Provider<Connection> connectionProvider;
  private final DatabaseStatementTranslator translator;

  @Inject
  public DatabaseUpdater(Provider<Connection> connectionProvider, DatabaseStatementTranslator translator) {
    this.connectionProvider = connectionProvider;
    this.translator = translator;
  }

  public void updateDatabase(String resourcePath) {
    int version = getDatabaseVersion();

    try {
      for(;;) {
        version++;

        String scriptName = String.format(resourcePath + "/db-v%04d.sql", version);

        LOGGER.fine("Checking for newer database version update script at: " + scriptName);

        try(InputStream sqlStream = getClass().getClassLoader().getResourceAsStream(scriptName)) {
          if(sqlStream == null) {
            version--;
            break;
          }

          LOGGER.info("Updating database to version " + version);

          try {
            applyUpdateScript(version, sqlStream);
          }
          catch (Exception e) {
            throw new DatabaseUpdateException("Exception while executing update script: " + scriptName, e);
          }
        }
      }

      LOGGER.info("Database up to date at version " + version);
    }
    catch(IOException | SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Sets the database version.  Useful for debugging scripts.
   */
  public void setDatabaseVersion(int version) {
    try {
      try(Connection connection = connectionProvider.get()) {
        try {
          connection.setAutoCommit(false);

          updateDatabaseVersion(version, connection);

          LOGGER.info("Forcing database to version " + version);

          connection.commit();
        }
        finally {
          if(!connection.isClosed()) {
            connection.rollback();
          }
        }
      }
    }
    catch(SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static void updateDatabaseVersion(int version, Connection connection) throws SQLException {
    try(PreparedStatement statement = connection.prepareStatement("UPDATE dbinfo SET value = '" + version + "' WHERE name = 'version'")) {
      if(statement.executeUpdate() != 1) {
        throw new IllegalStateException("Unable to update version information to " + version);
      }
    }
  }

  private void applyUpdateScript(int version, InputStream sqlStream) throws SQLException, IOException {
    try(Connection connection = connectionProvider.get()) {
      try {
        connection.setAutoCommit(false);

        try(LineNumberReader reader = new LineNumberReader(new InputStreamReader(sqlStream))) {

          statementExecuteLoop:
          for(;;) {
            String sqlStatement = "";

            while(!sqlStatement.endsWith(";")) {
              String line = reader.readLine();

              if(line == null) {
                if(!sqlStatement.trim().isEmpty()) {
                  throw new DatabaseUpdateException("Unexpected EOF, last statement was: " + sqlStatement);
                }

                break statementExecuteLoop;
              }

              int hash = line.indexOf('#');

              if(hash >= 0) {
                line = line.substring(0, hash);
              }

              sqlStatement += line.trim();
            }

            sqlStatement = translator.translate(sqlStatement.substring(0, sqlStatement.length() - 1));  // strip off semi-colon

            try(PreparedStatement statement = connection.prepareStatement(sqlStatement)) {
              LOGGER.fine(sqlStatement);

              statement.execute();
            }
            catch(SQLException e) {
              throw new DatabaseUpdateException("Exception at line " + reader.getLineNumber() + ": " + sqlStatement, e);
            }
          }
        }

        updateDatabaseVersion(version, connection);

        connection.commit();
      }
      finally {
        if(!connection.isClosed()) {
          connection.rollback();
        }
      }
    }
  }

  private int getDatabaseVersion() {
    try(Connection connection = connectionProvider.get()) {
      DatabaseMetaData dbm = connection.getMetaData();

      try(ResultSet rs1 = dbm.getTables(null, null, "dbinfo", null);
          ResultSet rs2 = dbm.getTables(null, null, "DBINFO", null)) {
        if(!rs1.next() && !rs2.next()) {
          LOGGER.fine("No dbinfo table exists, returning version 0");

          return 0;
        }
      }

      try(PreparedStatement statement = connection.prepareStatement("SELECT value FROM dbinfo WHERE name = 'version'")) {
        try(ResultSet rs = statement.executeQuery()) {
          if(rs.next()) {
            return Integer.parseInt(rs.getString("value"));
          }
        }
      }
    }
    catch(SQLException e) {
      throw new IllegalStateException("Unable to get version information from the database", e);
    }

    return 0;
  }
}
