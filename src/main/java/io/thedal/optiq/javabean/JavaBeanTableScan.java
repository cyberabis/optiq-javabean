package io.thedal.optiq.javabean;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.hydromatic.linq4j.expressions.Blocks;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.optiq.rules.java.EnumerableConvention;
import net.hydromatic.optiq.rules.java.EnumerableRel;
import net.hydromatic.optiq.rules.java.EnumerableRelImplementor;
import net.hydromatic.optiq.rules.java.PhysType;
import net.hydromatic.optiq.rules.java.PhysTypeImpl;
import net.hydromatic.linq4j.expressions.Primitive;

import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelWriter;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaBeanTableScan extends TableAccessRelBase implements
    EnumerableRel {

  static final Logger logger = LoggerFactory.getLogger(JavaBeanTableScan.class);
  final JavaBeanSmartTable javaBeanSmartTable;
  final int[] projectFields;
  final Set<String> projectFieldNames;
  final String filterExpression; 
  final String scanName;

  protected JavaBeanTableScan(RelOptCluster cluster, RelOptTable table,
      JavaBeanSmartTable javaBeanSmartTable, int[] projectFields,
      String filterExpression,
      String scanName) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), table);
    this.javaBeanSmartTable = javaBeanSmartTable;
    this.projectFields = projectFields;
    this.scanName = scanName;
    this.filterExpression = filterExpression;
    HashSet<String> fieldNames = new HashSet<String>();
    if (projectFields != null) {
      List<String> allFields = table.getRowType().getFieldNames();
      for (int i : projectFields) {
        fieldNames.add(allFields.get(i));
      }
    }
    projectFieldNames = fieldNames;

    assert javaBeanSmartTable != null;
    logger.debug("JavaBean table scan created. Projected field Names: "
        + projectFieldNames);
    logger.debug("Table scan name: " + scanName);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    logger.debug("JavaBean table scan copy call received.");
    assert inputs.isEmpty();
    return new JavaBeanTableScan(getCluster(), table, javaBeanSmartTable,
        projectFields, filterExpression, scanName);
  }


  @Override
  public RelWriter explainTerms(RelWriter pw) {
    logger.debug("Table Scan explain terms call received.");
    return super.explainTerms(pw)
        .item("projectFields", Primitive.asList(projectFields))
        .item("filterExpression", filterExpression);
  }


  
  @Override
  public RelDataType deriveRowType() {
    logger.debug("Table scan derive row type call received.");
    final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
    final RelDataTypeFactory.FieldInfoBuilder builder = getCluster()
        .getTypeFactory().builder();
    // Return only projected fields
    for (int i : projectFields) {
        builder.add(fieldList.get(i));
      logger.debug("Project field type resolved as: "
          + fieldList.get(i).getType().getSqlTypeName().getName());
        logger
            .debug("Adding project field name: " + fieldList.get(i).getName());
        projectFieldNames.add(fieldList.get(i).getName());
    }
    return builder.build();
  }


  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(JavaBeanPushDownRule.PROJECT_ON_FILTER);
    planner.addRule(JavaBeanPushDownRule.FILTER_ON_PROJECT);
    planner.addRule(JavaBeanPushDownRule.FILTER);
    planner.addRule(JavaBeanPushDownRule.PROJECT);
    logger.debug("JavaBean Smart Table rules added.");
  }


  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(),
        getRowType(), pref.preferCustom());
    logger.debug("Table scan implement call received. Project fields length: "
        + projectFieldNames.size());
    logger.debug("We are in table scan: " + scanName);

    return implementor.result(physType, Blocks.toBlock(Expressions.call(
        table.getExpression(JavaBeanSmartTable.class), "pushdown",
        Expressions.constant(projectFields),
        Expressions.constant(filterExpression))));
  }

}
