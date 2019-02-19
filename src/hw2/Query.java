package hw2;

import java.util.ArrayList;
import java.util.List;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
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
		List<Join> joins = sb.getJoins();
		if(joins != null) {
			for(int i = 0; i < joins.size(); i++) {
				System.out.print("Join++++" + joins.get(i));
				//System.out.print("Join++++" + joins.get(i).getRightItem());
				HeapFile tempFile = catalog.getDbFile(catalog.getTableId(joins.toString()));
				ArrayList<Tuple> tempTuples = tempFile.getAllTuples();
				TupleDesc temptDesc = tempFile.getTupleDesc();
				Relation tempRelation = new Relation(tempTuples, temptDesc);
				
			}
		}
		return null;
		
	}
}
