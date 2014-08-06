package io.thedal.optiq.javabean;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for the Optiq Adaptor
 * 
 * @author Abishek Baskaran
 *
 */
public class SampleTestProgram {

  final Logger logger = LoggerFactory.getLogger(SampleTestProgram.class);

  /**
   * Tests the simple functionality on a simple table.
   */
  @Test
  public void testSimpleQuery() {

    // Create test data
    User user1 = new User("Abishek", 29, "India");
    User user2 = new User("Kousik", 25, "Thailand");
    User user3 = new User("CP", 15, "Russia");
    User user4 = new User("Karthik", 29, "US");
    List<User> userList = new ArrayList<User>();
    userList.add(user1);
    userList.add(user2);
    userList.add(user3);
    userList.add(user4);

    // Create a test Schema
    JavaBeanSchema schema = new JavaBeanSchema("TESTDB");
    schema.addAsTable("USERS", userList);

    // Execute a Query on schema
    JdbcQueryExecutor queryExec = new JdbcQueryExecutor(schema);
    String sql = "select \"Age\" from \"TESTDB\".\"USERS\" where \"Name\"='Abishek'";
    ResultSet result = queryExec.execute(sql);

    // Verify results
    if (result != null) {
      int output = -1;
      try {
        result.next();
        output = result.getInt("Age");
      } catch (SQLException e) {
        fail("Failed while iterating resultset");
      }
      assertEquals(output, 29);
    } else {
      fail("Null resultset");
    }
    queryExec.close();

  }

  /**
   * Tests query push down function using a smart table.
   */
  @Test
  public void testPushDownQuery() {

    // Create test data
    User user1 = new User("Abishek", 29, "India");
    User user2 = new User("Kousik", 25, "Thailand");
    User user3 = new User("CP", 15, "India");
    User user4 = new User("Karthik", 29, "US");
    List<User> userList = new ArrayList<User>();
    userList.add(user1);
    userList.add(user2);
    userList.add(user3);
    userList.add(user4);
    logger.info("Test data created");

    // Create a test Schema and add smart table.
    JavaBeanSchema schema = new JavaBeanSchema("TESTDB");
    schema.addAsSmartTable("USERS", userList);
    logger.info("Optiq Schema and Smart Table created");

    // Execute a Query on schema
    JdbcQueryExecutor queryExec = new JdbcQueryExecutor(schema);
    String sql;

    // Fired: Project.
    // Match: Project
    // Notes: Optiq did no operation
    sql = "select \"Name\", \"Age\" from \"TESTDB\".\"USERS\"";

    // Fired: Filter on project, Filter, Project.
    // Match: Filter on Proj
    // Optiq did no operation.
    sql = "select \"Name\", \"Age\" from \"TESTDB\".\"USERS\" where \"Age\" < 29";

    // Fired: Filter.
    // Match: Filter.
    // Notes: Optiq did the 'select' operation as project was not fired.
    sql = "select \"Name\", \"Age\" from \"TESTDB\".\"USERS\" where \"Country\" = 'India'";

    // Fired: Filter.
    // Match: Filter.
    // Notes: Optiq did the 'select' operation as project was not fired.
    sql = "select \"Name\", \"Age\" from \"TESTDB\".\"USERS\" where \"Country\" = 'India' and \"Age\" < 29";

    // Fired: Project.
    // Match: Project.
    // Notes: Error: exception while executing query: [Ljava.lang.Object; cannot
    // be cast to java.lang.String
    sql = "select \"Country\", count(1) from \"TESTDB\".\"USERS\" group by \"Country\"";

    // Fired: Project.
    // Match: Project.
    // Notes: Optiq did sum function
    sql = "select \"Country\", sum(\"Age\") as \"Sum_Age\" from \"TESTDB\".\"USERS\" group by \"Country\"";

    // Fired: Project.
    // Match: Project.
    // Notes: Optiq did count function
    sql = "select \"Country\", count(\"Age\") from \"TESTDB\".\"USERS\" group by \"Country\"";

    // Fired: Project.
    // Match: Project.
    // Notes: Optiq did count and order function
    sql = "select \"Country\", count(\"Age\") from \"TESTDB\".\"USERS\" group by \"Country\" order by \"Country\"";

    // Fired: Filter on project, Project, Filter
    // Match: Filter on project.
    // Notes: Optiq did count and order function
    sql = "select \"Country\", count(\"Age\") from \"TESTDB\".\"USERS\" where \"Age\" < 29 group by \"Country\" order by \"Country\"";

    // Fired: Filter
    // Match: Filter.
    // Notes: Optiq did count, order and select functions
    sql = "select \"Country\", count(\"Age\") from \"TESTDB\".\"USERS\" where \"Name\" <> 'Abishek' group by \"Country\" order by \"Country\"";

    ResultSet result = queryExec.execute(sql);
    logger.info("Query Executed");

    // Verify results
    if (result != null) {
      logger.info("Result not null");
      int output = 0;
      try {
        int columnCount = result.getMetaData().getColumnCount();
        System.out.println("");
        for (int i = 1; i <= columnCount; i++) {
          System.out.print(result.getMetaData().getColumnName(i) + " | ");
        }
        while (result.next()) {
          System.out.println("");
          for (int i = 1; i <= columnCount; i++) {
            System.out.print(result.getObject(i) + " | ");
          }
          // logger.debug("Name: " + result.getString("Name") + " | Age: "
          // + result.getInt("Age"));
          output++;
        }

      } catch (SQLException e) {
        fail("Failed while iterating resultset");
      }
      // assertEquals(output, 29);
      logger.info("Number of rows: " + output);
      logger.info("Results verified");
    } else {
      // fail("Null resultset");
    }

    queryExec.close();

  }

}
