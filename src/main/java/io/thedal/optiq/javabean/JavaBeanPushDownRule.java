package io.thedal.optiq.javabean;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexSlot;
import org.eigenbase.sql.SqlBinaryOperator;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.NlsString;
import org.eigenbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class JavaBeanPushDownRule extends RelOptRule {

  static final Logger logger = LoggerFactory
      .getLogger(JavaBeanPushDownRule.class);

  private static final Set<SqlKind> SUPPORTED_OPS = ImmutableSet.of(
      SqlKind.CAST, SqlKind.EQUALS, SqlKind.LESS_THAN,
      SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN,
      SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.NOT_EQUALS, SqlKind.LIKE,
      SqlKind.AND, SqlKind.OR, SqlKind.NOT);

  public static final JavaBeanPushDownRule PROJECT_ON_FILTER = new JavaBeanPushDownRule(
      operand(
          ProjectRel.class,
          operand(
              FilterRel.class,
              operand(ProjectRel.class,
                  operand(JavaBeanTableScan.class, none())))),
      "Project on filter on project");

  public static final JavaBeanPushDownRule FILTER_ON_PROJECT = new JavaBeanPushDownRule(
      operand(FilterRel.class,
          operand(ProjectRel.class, operand(JavaBeanTableScan.class, none()))),
      "Filter on project");

  public static final JavaBeanPushDownRule FILTER = new JavaBeanPushDownRule(
      operand(FilterRel.class, operand(JavaBeanTableScan.class, none())),
      "Filter");

  public static final JavaBeanPushDownRule PROJECT = new JavaBeanPushDownRule(
      operand(ProjectRel.class, operand(JavaBeanTableScan.class, none())),
      "Project");


  protected JavaBeanPushDownRule(RelOptRuleOperand rule, String id) {
    super(rule, "JBPushDownRule: " + id);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    logger.info("Rule Match for: " + description);

    int relLength = call.rels.length;
    logger.debug("relLength: " + relLength);
    JavaBeanTableScan javaBeanRel = (JavaBeanTableScan) call.rels[relLength - 1];

    FilterRel filter;
    ProjectRel topProj = null;
    ProjectRel bottomProj = null;

    RelDataType topRow = javaBeanRel.getRowType();

    int filterIdx = 2;
    if (call.rels[relLength - 2] instanceof ProjectRel) {
      bottomProj = (ProjectRel) call.rels[relLength - 2];
      filterIdx = 3;

      // bottom projection will change the field count/order
      topRow = bottomProj.getRowType();
    }
    logger.debug("filterIdx: " + filterIdx);

    String filterString;

    if (filterIdx <= relLength
        && call.rels[relLength - filterIdx] instanceof FilterRel) {
      filter = (FilterRel) call.rels[relLength - filterIdx];

      int topProjIdx = filterIdx + 1;
      if (topProjIdx <= relLength
          && call.rels[relLength - topProjIdx] instanceof ProjectRel) {
        topProj = (ProjectRel) call.rels[relLength - topProjIdx];
      }

      RexCall filterCall = (RexCall) filter.getCondition();
      SqlOperator op = filterCall.getOperator();
      List<RexNode> operands = filterCall.getOperands();

      logger.info("FieldNames: " + getFieldsString(topRow));

      final StringBuilder buf = new StringBuilder();
      if (getFilter(op, operands, buf, topRow.getFieldNames())) {
        filterString = buf.toString();
      } else {
        return; // can't handle
      }
    } else {
      filterString = "";
    }

    // top projection will change the field count/order
    if (topProj != null) {
      topRow = topProj.getRowType();
    }
    logger.info("Pre transformTo fieldNames (top row): "
        + getFieldsString(topRow));
    logger.info("Filter String: " + filterString);

    // Testing get topFields and bottom fields
    List<RelDataTypeField> bottomFields = null;
    List<RelDataTypeField> topFields = topRow == null ? null : topRow
        .getFieldList();
    if (bottomFields == null) {
      bottomFields = javaBeanRel.getRowType().getFieldList();
    }

    // handle bottom projection (ie choose a subset of the table fields)
    if (bottomProj != null) {
      List<RelDataTypeField> tmp = new ArrayList<RelDataTypeField>();
      List<RelDataTypeField> dRow = bottomProj.getRowType().getFieldList();
      for (RexNode rn : bottomProj.getProjects()) {
        RelDataTypeField rdtf;
        if (rn instanceof RexSlot) {
          RexSlot rs = (RexSlot) rn;
          rdtf = bottomFields.get(rs.getIndex());
        } else {
          rdtf = dRow.get(tmp.size());
        }
        tmp.add(rdtf);
      }
      bottomFields = tmp;
    }
    for (RelDataTypeField bottomField : bottomFields) {
      logger.debug("bottomField: " + bottomField.getName());
    }

    // field renaming: to -> from
    List<Pair<String, String>> renames = new LinkedList<Pair<String, String>>();

    // handle top projection (ie reordering and renaming)
    List<RelDataTypeField> newFields = bottomFields;
    if (topProj != null) {
      logger.info("topProj: " + String.valueOf(topProj.getPermutation()));
      newFields = new ArrayList<RelDataTypeField>();
      int i = 0;
      for (RexNode rn : topProj.getProjects()) {
        RexInputRef rif = (RexInputRef) rn;
        RelDataTypeField field = bottomFields.get(rif.getIndex());
        if (!bottomFields.get(rif.getIndex()).getName()
            .equals(topFields.get(i).getName())) {
          renames.add(new Pair<String, String>(bottomFields.get(rif.getIndex())
              .getName(), topFields.get(i).getName()));
          field = topFields.get(i);
        }
        newFields.add(field);
      }
    }
    for (RelDataTypeField topField : topFields) {
      logger.debug("topField: " + topField.getName());
    }
    for (RelDataTypeField newField : newFields) {
      logger.debug("newField: " + newField.getName());
    }


    // Top row is the projects
    // int[] intProjections = null;
    // if (topRow.getFieldCount() > 0) {
    // intProjections = new int[topRow.getFieldCount()];
    // List<RelDataTypeField> topProjections = topRow.getFieldList();
    // int i = 0;
    // for (RelDataTypeField topProjection : topProjections) {
    // logger.debug("Extracting project for: " + topProjection.getName()
    // + " | " + topProjection.getIndex());
    // intProjections[i] = topProjection.getIndex();
    // i++;
    // }
    // }

    logger.debug("Getting projections.. FilterIdx: " + filterIdx);
    int[] intProjections = null;
    if (filterIdx == 3)
      intProjections = getProjectFields(bottomProj.getProjects());
    else {
      if (topRow.getFieldCount() > 0) {
         intProjections = new int[topRow.getFieldCount()];
         List<RelDataTypeField> topProjections = topRow.getFieldList();
         int i = 0;
         for (RelDataTypeField topProjection : topProjections) {
         logger.debug("Extracting project for: " + topProjection.getName()
         + " | " + topProjection.getIndex());
         intProjections[i] = topProjection.getIndex();
         i++;
         }
         }
    }
    logger.debug("Extracted projections: " + Arrays.toString(intProjections));

    call.transformTo(new JavaBeanTableScan(javaBeanRel.getCluster(),
        javaBeanRel.getTable(), javaBeanRel.javaBeanSmartTable, intProjections,
        filterString, description));
  }

  private int[] getProjectFields(List<RexNode> exps) {
    final int[] fields = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        fields[i] = ((RexInputRef) exp).getIndex();
      } else {
        return null; // not a simple projection
      }
    }
    return fields;
  }

  // Copied from Splunk Adapter
  private boolean getFilter(SqlOperator op, List<RexNode> operands,
      StringBuilder s, List<String> fieldNames) {
    if (!valid(op.getKind())) {
      return false;
    }

    boolean like = false;
    switch (op.getKind()) {
    case NOT:
      // NOT op pre-pended
      s = s.append(" NOT ");
      break;
    case CAST:
      return asd(false, operands, s, fieldNames, 0);
    case LIKE:
      like = true;
      break;
    }

    for (int i = 0; i < operands.size(); i++) {
      if (!asd(like, operands, s, fieldNames, i)) {
        return false;
      }
      if (op instanceof SqlBinaryOperator && i == 0) {
        s.append(" ").append(op).append(" ");
      }
    }
    return true;
  }

  // Copied from Splunk Adapter
  private boolean asd(boolean like, List<RexNode> operands, StringBuilder s,
      List<String> fieldNames, int i) {
    RexNode operand = operands.get(i);
    if (operand instanceof RexCall) {
      s.append("(");
      final RexCall call = (RexCall) operand;
      boolean b = getFilter(call.getOperator(), call.getOperands(), s,
          fieldNames);
      if (!b) {
        return false;
      }
      s.append(")");
    } else {
      if (operand instanceof RexInputRef) {
        if (i != 0) {
          return false;
        }
        int fieldIndex = ((RexInputRef) operand).getIndex();
        String name = fieldNames.get(fieldIndex);
        s.append(name);
      } else { // RexLiteral
        String tmp = toString(like, (RexLiteral) operand);
        if (tmp == null) {
          return false;
        }
        s.append(tmp);
      }
    }
    return true;
  }

  // Copied from Splunk Adapter
  private boolean valid(SqlKind kind) {
    return SUPPORTED_OPS.contains(kind);
  }

  // Copied from Splunk adapter
  private String toString(boolean like, RexLiteral literal) {
    String value = null;
    SqlTypeName litSqlType = literal.getTypeName();
    if (SqlTypeName.NUMERIC_TYPES.contains(litSqlType)) {
      value = literal.getValue().toString();
    } else if (litSqlType.equals(SqlTypeName.CHAR)) {
      value = ((NlsString) literal.getValue()).getValue();
      if (like) {
        value = value.replaceAll("%", "*");
      }
      value = searchEscape(value);
    }
    return value;
  }


  // Copied from Splunk Adapter
  public static String getFieldsString(RelDataType row) {
    return row.getFieldNames().toString();
  }

  // Copied from Splunk adapter
  public static String searchEscape(String str) {
    if (str.isEmpty()) {
      return "\"\"";
    }
    StringBuilder sb = new StringBuilder(str.length());
    boolean quote = false;

    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      if (c == '"' || c == '\\') {
        sb.append('\\');
      }
      sb.append(c);

      quote |= !(Character.isLetterOrDigit(c) || c == '_');
    }

    if (quote || sb.length() != str.length()) {
      sb.insert(0, '"');
      sb.append('"');
      return sb.toString();
    }
    return str;
  }


}
