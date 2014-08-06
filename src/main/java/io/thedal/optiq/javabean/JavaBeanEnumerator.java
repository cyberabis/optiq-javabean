package io.thedal.optiq.javabean;

import io.thedal.optiq.javabean.utils.JavaBeanInspector;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.hydromatic.linq4j.Enumerator;

/**
 * JavaBeanEnumerator converts a JavaBean List into rows. A Row is an Object
 * array of columns. The iterator over every row is called rows.
 * 
 * @author Abishek Baskaran
 *
 */
public class JavaBeanEnumerator implements Enumerator<Object> {

  static final Logger logger = LoggerFactory
      .getLogger(JavaBeanEnumerator.class);
  private Object current;
  private Iterator<Object[]> rowIterator;

  /**
   * Constructor - forms the row iterator.
   * 
   * @param javaBeanList
   */
  public <E> JavaBeanEnumerator(List<E> javaBeanList) {
    List<Object[]> rows = new ArrayList<Object[]>();
    for (Object javaBean : javaBeanList) {
      rows.add(getRow(javaBean));
    }
    rowIterator = rows.iterator();
    logger.debug("Created an iterator for the enumerator");
  }

  /**
   * This constructor is for a smart table. Projection fields will tell what
   * columns or fields are required. Filter will select rows.
   * 
   * @param javaBeanList
   *          The JavaBean list
   * @param projectionFields
   *          The fields or columns in the select clause of query.
   */
  public <E> JavaBeanEnumerator(List<E> javaBeanList,
      Set<String> projectFieldNames, String filterExpression) {
    List<Object[]> rows = new ArrayList<Object[]>();
    // This is where the query push down happens.
    // In this example query push down is in the same program, but
    // in real world case a method on the data source will be called
    // to push down the query.
    for (Object javaBean : javaBeanList) {
      if (filterJavaBean(javaBean, filterExpression))
        rows.add(getProjectedRow(javaBean, projectFieldNames));
    }
    // Query push down ends
    rowIterator = rows.iterator();
    logger.debug("Created an iterator for the enumerator with projects");
  }

  /**
   * This method will return true if the JavaBean passes the filter condition.
   * 
   * @param javaBean
   *          The java bean object
   * @param filterExpression
   * @return return true on filter condition pass
   */
  private boolean filterJavaBean(Object javaBean, String filterExpression) {
    // TODO Auto-generated method stub
    return true;
  }

  private Object[] getProjectedRow(Object javaBean,
      Set<String> projectFieldNames) {
    List<Object> row = new ArrayList<Object>();
    Class clazz = javaBean.getClass();
    Method[] methods = clazz.getMethods();
    for (Method method : methods) {
      if (JavaBeanInspector.checkMethodEligiblity(method)) {
        if (projectFieldNames.contains(method.getName().substring(3))) {
          try {
            row.add(method.invoke(javaBean));
          } catch (IllegalAccessException e) {
            logger.error("Unable to invoke method via reflection");
          } catch (IllegalArgumentException e) {
            logger.error("Unable to invoke method via reflection");
          } catch (InvocationTargetException e) {
            logger.error("Unable to invoke method via reflection");
          }
        }
      }
    }
    logger.debug("Formed projected row is: " + row);
    return row.toArray();
  }

  private Object[] getRow(Object javaBean) {
    List<Object> row = new ArrayList<Object>();
    Class clazz = javaBean.getClass();
    Method[] methods = clazz.getMethods();
    for (Method method : methods) {
      if (JavaBeanInspector.checkMethodEligiblity(method)) {
        try {
          row.add(method.invoke(javaBean));
        } catch (IllegalAccessException e) {
          logger.error("Unable to invoke method via reflection");
        } catch (IllegalArgumentException e) {
          logger.error("Unable to invoke method via reflection");
        } catch (InvocationTargetException e) {
          logger.error("Unable to invoke method via reflection");
        }
      }
    }
    logger.debug("Formed row is: " + row);
    return row.toArray();
  }

  @Override
  public void close() {
    // Nothing to do
  }

  @Override
  public Object current() {
    if (current == null) {
      this.moveNext();
    }
    return current;
  }

  @Override
  public boolean moveNext() {
    if (this.rowIterator.hasNext()) {
      final Object[] row = this.rowIterator.next();
      current = row;
      return true;
    } else {
      current = null;
      return false;
    }
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }

  public static int[] identityList(int fieldCount) {
    int[] integers = new int[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      integers[i] = i;
    }
    return integers;
  }

}
