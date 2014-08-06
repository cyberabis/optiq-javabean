package io.thedal.optiq.javabean;

import io.thedal.optiq.javabean.utils.JavaBeanInspector;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelOptTable.ToRelContext;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.hydromatic.linq4j.AbstractEnumerable;
import net.hydromatic.linq4j.Enumerable;
import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.QueryProvider;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.TranslatableTable;
import net.hydromatic.optiq.impl.AbstractTableQueryable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.JavaRules;

/**
 * JavaBeanSmartTable is an Optiq Smart table that accepts a List of JavaBeans.
 * JavaBeans can have only fields of eligible type / class defined in
 * JavaBeanInspector.checkMethodEligibility.
 * 
 * @author Abishek Baskaran
 *
 * @param <E>
 *          Table contains items for a specific Class E
 */
public class JavaBeanSmartTable<E> extends AbstractQueryableTable implements
    TranslatableTable {

  static final Logger logger = LoggerFactory.getLogger(JavaBeanSmartTable.class);
  private List<E> javaBeanList;
  private List<String> fieldNames = new ArrayList<String>();

  /**
   * Constructor
   * 
   * @param javaBeanList
   *          A JavaBean List
   */
  public JavaBeanSmartTable(List<E> javaBeanList) {
    super(Object[].class);
    this.javaBeanList = javaBeanList;
  }


  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    List<String> names = new ArrayList<String>();
    List<RelDataType> types = new ArrayList<RelDataType>();
    if ((javaBeanList != null) && (javaBeanList.size() > 0)) {
      Class sample = javaBeanList.get(0).getClass();
      Method[] methods = sample.getMethods();
      for (Method method : methods) {
        if (JavaBeanInspector.checkMethodEligiblity(method)) {
          String name = method.getName().substring(3);
          Class type = method.getReturnType();
          names.add(name);
          types.add(typeFactory.createJavaType(type));
          logger.info("Added field name: " + name + " of type: "
              + type.getSimpleName());
          fieldNames.add(name);
        }
      }
    }
    return typeFactory.createStructType(Pair.zip(names, types));
  }

  @Override
  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    logger.info("Got query request for: " + tableName);
    return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
      public Enumerator<T> enumerator() {
        // noinspection unchecked
        try {
          JavaBeanEnumerator enumerator = new JavaBeanEnumerator(javaBeanList);
          return (Enumerator<T>) enumerator;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  /**
   * For a smart table, the toRel Method needs to be overridden Custom Scan.
   * This involves registering optimizer rules, and when the rules match with
   * the query expression, The Enumerator is made to return only a subset of the
   * data instead of sending back the whole date to Optiq.
   */
  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    logger.debug("Smart table toRel call received");
    final RelOptCluster cluster = context.getCluster();

    // Send all projections and a null filter expression.
    // After rules are fired and rule match the projection field and
    // filter will be used to create a new Table scan with values.
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final int[] projectFields = JavaBeanEnumerator.identityList(fieldCount);
    return new JavaBeanTableScan(cluster, relOptTable, this, projectFields,
        null, "Rule register scan");
  }

  /** Returns an enumerable over a given projection of the fields. */
  public Enumerable<Object> pushdown(int[] projectFields,
      String filterExpression) {
    logger.debug("Smart table pushdown call received.");
    logger.debug("No. of projection field ids: " + projectFields.length);
    logger.debug("Filter Expression: " + filterExpression);
    Set<String> projectFieldNames = new HashSet<String>();
    for (int i : projectFields) {
      logger.debug("Adding name for project number: " + i);
      projectFieldNames.add(fieldNames.get(i));
    }

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        // TODO apply push down
        return new JavaBeanEnumerator(javaBeanList, projectFieldNames,
            filterExpression);
      }
    };
  }

}
