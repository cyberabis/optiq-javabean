package io.thedal.optiq.javabean;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class SampleTestProgram {

  @Test
  public void testQuery() {

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
        while (result.next()) {
          output = result.getInt("Age");
        }
      } catch (SQLException e) {
        fail("Failed while iterating resultset");
      }
      assertEquals(output, 29);
    } else {
      fail("Null resultset");
    }

    queryExec.close();

  }

}
