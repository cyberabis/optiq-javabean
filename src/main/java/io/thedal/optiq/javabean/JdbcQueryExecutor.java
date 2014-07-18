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

public class JdbcQueryExecutor {

  final Logger logger = LoggerFactory.getLogger(JdbcQueryExecutor.class);
  private Connection connection;
  private Statement statement;

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

  public ResultSet execute(String sql) {
    ResultSet results = null;
    try {
      statement = connection.createStatement();
      results = statement.executeQuery(sql);
    } catch (SQLException e) {
      logger.error("Could not create a statement" + e);
    }
    return results;
  }

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
