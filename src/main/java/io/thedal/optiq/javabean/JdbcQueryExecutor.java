package io.thedal.optiq.javabean;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.OptiqConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes a sql query over an Optiq Schema
 * 
 * @author Abishek Baskaran
 *
 */
public class JdbcQueryExecutor {

  final Logger logger = LoggerFactory.getLogger(JdbcQueryExecutor.class);
  private Connection connection;
  private Statement statement;

  /**
   * Constructor to instantiate a JdbcQueryExecutor
   * 
   * @param schema
   *          The schema to execute queries.
   */
  public JdbcQueryExecutor(JavaBeanSchema schema) {
    try {
      Class.forName("net.hydromatic.optiq.jdbc.Driver");
      connection = DriverManager.getConnection("jdbc:optiq:");
      OptiqConnection optiqConnection = connection
          .unwrap(OptiqConnection.class);
      SchemaPlus rootSchema = optiqConnection.getRootSchema();
      rootSchema.add(schema.getName(), schema);
      logger.info("Created connection to schema: " + schema.getName());
    } catch (Exception e) {
      logger.error("Could not create Optiq Connection");
    }
  }

  /**
   * Executes a SQL query.
   * 
   * @param sql
   *          SQL query in string.
   * @return JDBC result set.
   */
  public ResultSet execute(String sql) {
    ResultSet results = null;
    try {
      logger.debug("Creating a statement");
      statement = connection.createStatement();
      logger.debug("Going to execute query: " + sql);
      results = statement.executeQuery(sql);
      logger.debug("Execution complete");
    } catch (SQLException e) {
      logger.error("Could not create a statement.  " + e);
    }
    return results;
  }

  /**
   * Closed the connection and statement used for executing query.
   */
  public void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        logger.error("Could not close Optiq connection");
      }
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          logger.error("Could not close Optiq statement");
        }
      }
    }
  }

}
