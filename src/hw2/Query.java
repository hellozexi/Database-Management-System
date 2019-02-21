package hw2;

import java.util.ArrayList;
import java.util.List;

import hw1.Catalog;
import hw1.Database;
import hw1.Field;
import hw1.HeapFile;
import hw1.RelationalOperator;
import hw1.Tuple;
import hw1.TupleDesc;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		
		
		//your code here
		//load the database needed
		Catalog catalog = Database.getCatalog(); 
		System.out.print("sb" + sb);
		System.out.print("selected::::" + sb.getSelectItems());
		System.out.print("Join:::::" + sb.getJoins());
		List<SelectItem> selectItems = sb.getSelectItems();
		//get all tuples from a table which "from" request
		FromItem fromItem = sb.getFromItem();
		HeapFile file = catalog.getDbFile(catalog.getTableId(fromItem.toString()));
		ArrayList<Tuple> tuples = file.getAllTuples();
		TupleDesc tDesc = file.getTupleDesc();
		Relation relation = new Relation(tuples, tDesc);
		
		
		//Build a new relation after join
		List<Join> joins = sb.getJoins();
		if(joins != null) {
			for(int i = 0; i < joins.size(); i++) {
				//System.out.print("Join++++" + joins.get(i));
				System.out.print("Join++++" + joins.get(i).getRightItem());
				HeapFile tempFile = catalog.getDbFile(catalog.getTableId(joins.get(i).getRightItem().toString()));
				ArrayList<Tuple> tempTuples = tempFile.getAllTuples();
				TupleDesc temptDesc = tempFile.getTupleDesc();
				Relation tempRelation = new Relation(tempTuples, temptDesc);
				String onString = joins.get(i).getOnExpression().toString();
				//System.out.print("ON++++" + onString);
				//test.c1 = a.a1
				String[] onArray = onString.split(" = ");
				String[] leftStrings = onArray[0].split("\\.");
				String[] rightStrings = onArray[1].split("\\.");
				/*System.out.print("left::::");
				for(int j = 0; j < 2; j++) {
					System.out.print(leftStrings[j]);
				}
				System.out.print("right::::");
				for(int j = 0; j < 2; j++) {
					System.out.print(rightStrings[j]);
				}*/
				System.out.print(leftStrings[0].trim());
				int field1 = 0;
				int field2 = 0;
				String[] fieldStrings = null;
				String[] fieldStrings2 = null;
				if(leftStrings[0].equalsIgnoreCase(fromItem.toString())) {
					fieldStrings = tDesc.getFields();
					fieldStrings2 = temptDesc.getFields();
				} else {
					fieldStrings = temptDesc.getFields();
					fieldStrings2 = tDesc.getFields();
				}
				int x = 0;
				for(x = 0; x < fieldStrings.length; x++) {
					if(leftStrings[1].equalsIgnoreCase(fieldStrings[x])) {
						field1 = x;
						break;
					}
				}
				
				for(x = 0; x < fieldStrings2.length; x++) {
					if(rightStrings[1].equalsIgnoreCase(fieldStrings2[x])){
						field2 = x;
						break;
					}
				}
				relation = tempRelation.join(relation, field1, field2);
				//System.out.print("Size join:::" + relation.getTuples().size());
			}
		}
		
		//Bulid Relation after Where
		Expression whereExpression= sb.getWhere();
		if(whereExpression != null) {
			//System.out.print("where::::" + whereExpression);
			WhereExpressionVisitor wVisitor = new WhereExpressionVisitor();
			whereExpression.accept(wVisitor);
			String leftString = wVisitor.getLeft();
			System.out.print("leftString::::" + leftString);
			String[] fieldStrings = relation.getDesc().getFields();
			int intField = 0;
			for(int i = 0; i < fieldStrings.length; i++) {
				if(fieldStrings[i] == leftString) {
					intField = i;
					break;
				}
			}
			RelationalOperator opField = wVisitor.getOp();
			Field field = wVisitor.getRight();
			relation = relation.select(intField, opField, field);
		}
		//System.out.print("where::::" + wherExpression.toString());
		boolean groupBy;
		String groupString = null;
		if(sb.getGroupByColumnReferences() != null) {
			groupBy = true;
			groupString = sb.getGroupByColumnReferences().get(0).toString();
			//System.out.print("GB::::" + groupString);
		} else {
			groupBy = false;
		}
		List<SelectItem> selectCols = sb.getSelectItems();
		ArrayList<ColumnVisitor> columnVisitors = new ArrayList<ColumnVisitor>();
		for(int i = 0; i < selectCols.size(); i++) {
			if(selectCols.get(i).toString().equals("*")) {
				return relation;
			}
			columnVisitors.add(new ColumnVisitor());
			selectCols.get(i).accept(columnVisitors.get(i));
		}
		//select all * case
		ArrayList<Integer> fields = new ArrayList<Integer>();
		ArrayList<Integer> fields2 = new ArrayList<Integer>();
		String[] fields1 = relation.getDesc().getFields();
		boolean isAgg = false;
		AggregateOperator aggregateOperator = null;
		for(int i = 0; i < selectCols.size(); i++) {
			if(columnVisitors.get(i).isAggregate()) {
				isAgg = true;
				aggregateOperator = columnVisitors.get(i).getOp();
				if(groupBy) {
					for(int j = 0; j < fields1.length; j++) {
						if(fields1[j].equalsIgnoreCase(groupString)) {
							fields2.add(j);
						}
					}
				}
				for(int j = 0; j < fields1.length; j++) {
					if(fields1[j].equalsIgnoreCase(columnVisitors.get(i).getColumn())) {
						fields2.add(j);
					}
				}
				
			}
		}
		
		for(int i = 0; i < selectCols.size(); i++) {
			if(columnVisitors.get(i).isAggregate()) {
				
			}
			String itemString = selectCols.get(i).toString();
			for(int j = 0; j < fields1.length; j++) {
				if(fields1[j].equalsIgnoreCase(itemString)) {
					fields.add(j);
				}
			}
		}
		if(isAgg) {
			relation = relation.project(fields2);
			relation = relation.aggregate(aggregateOperator, groupBy);
		} else {
			relation = relation.project(fields);
		}
		
		return relation;
	}
}
