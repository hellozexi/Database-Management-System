package hw2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import hw1.Field;
import hw1.IntField;
import hw1.StringField;
import hw1.Tuple;
import hw1.TupleDesc;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {

	
	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	private boolean groupBy;
	private AggregateOperator o;
	private int sum = 0;
	private int res = 0;
	private int count = 0;
	private Field currentGroupByKey = null;
	private int numTuplesToProcess = 0;
	private int numTuplesProcessed = 0;
	private Map<Field, Integer> map;
	private Map<Field, String> mapString;

	private int min = Integer.MAX_VALUE;
	private int max = Integer.MIN_VALUE;
	private String defaultStr = "";
	
	
		
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		this.o = o;
		this.td = td;
		this.groupBy = groupBy;
		this.tuples = new ArrayList<Tuple>();
		//your code here
		this.map = new HashMap<Field, Integer>();
		this.mapString = new HashMap<Field, String>();

	}
	
	public void setSize(int size) {
		this.numTuplesToProcess = size;
	}
	

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
//		TODO
//		System.out.println("merging "+ t.toString());

		int indexOFColumnToAggregate;
		if  (groupBy) {	indexOFColumnToAggregate = 1;}
		else {	indexOFColumnToAggregate = 0;	}

		//check if we have a intField
		if (t.getField(indexOFColumnToAggregate) instanceof IntField) {
			//grab the value to aggregate
			IntField intFiledToAggregate = (IntField) t.getField(indexOFColumnToAggregate);
			int intValueToAggregate = intFiledToAggregate.getValue();
			numTuplesProcessed ++;
			if (!groupBy) {
				switch(this.o) {
				case MAX:
					if (intValueToAggregate > max) {
						max = intValueToAggregate;
						res = max;
					}
					break;
				case MIN:
					if (intValueToAggregate < min) {
						min = intValueToAggregate;
						res = min;
					}
					break;
				case AVG:
					sum = sum + intValueToAggregate;
					count = count + +1;
					res = sum / count;
					break;
				case COUNT:
					res = res + 1;
					break;
				case SUM:
					res = res + intValueToAggregate;
					break;
				}
				//add new tuple to arrayList
				//hoohoh
				Tuple resultTuple = new Tuple(this.td);
				if (this.tuples.size() == 0) {//add tuple
					resultTuple.setField(indexOFColumnToAggregate, new IntField(res));
					this.tuples.add(resultTuple);
				}
				else {//set field
					this.tuples.get(0).setField(indexOFColumnToAggregate, new IntField(res));
				}
			}
			else { // groupBy
				Field groupByKey = t.getField(0);
				//grab res previously updated
				if (map.containsKey(groupByKey)) {
					res = map.get(groupByKey);
				}
				switch(this.o) {
				case MAX:
					if (intValueToAggregate > max) {
						max = intValueToAggregate;
						res = max;
					}
					break;
				case MIN:
					if (intValueToAggregate < min) {
						min = intValueToAggregate;
						res = min;
					}
				case AVG:
					sum = sum + intValueToAggregate;
					count = count +1;
					res = sum / count;
					break;
					//
				case COUNT:
					res = res + 1;
					break;
				case SUM:
					res = res + intValueToAggregate;
					break;
				}
				
				//put updated value to pockets
				map.put(groupByKey, res);
				//grab all tuples from hashmap
				if (numTuplesToProcess==numTuplesProcessed) {
			        for (Map.Entry<Field,Integer> entry : map.entrySet())  {
						Tuple resultTuple = new Tuple(this.td);
						resultTuple.setField(0, entry.getKey());
						resultTuple.setField(indexOFColumnToAggregate, new IntField(entry.getValue()));
						this.tuples.add(resultTuple);
			        }
			        System.out.println("-----"+this.tuples.size());
				}
			}
		}
		
		
			
		
		
		else if (t.getField(indexOFColumnToAggregate) instanceof StringField) {
			StringField strFieldToAggregate = (StringField) t.getField(indexOFColumnToAggregate);
			String strValueToAggregate = strFieldToAggregate.getValue();
			if (0==numTuplesProcessed) {
				defaultStr = strValueToAggregate;
				System.out.println("current!!!!" + defaultStr);
			}
			numTuplesProcessed ++;
			if (!groupBy) {
				switch(this.o) {
				case MAX:
					if (strValueToAggregate.compareTo(defaultStr) >= 0 ) {
						defaultStr = strValueToAggregate;
					}
					break;
				case MIN:
					if (strValueToAggregate.compareTo(defaultStr) < 0 ) {
						defaultStr = strValueToAggregate;
					}
					break;
				}
				if (this.tuples.size() == 0) {//add tuple
					Tuple resultTuple = new Tuple(this.td);
					resultTuple.setField(indexOFColumnToAggregate, new StringField(defaultStr));
					this.tuples.add(resultTuple);
				}
				else {//set field
					this.tuples.get(0).setField(indexOFColumnToAggregate, new StringField(defaultStr));
				}
			}
			else {
				Field groupByKey = t.getField(0);
				//grab res previously updated
				if (mapString.containsKey(groupByKey)) {
					defaultStr = mapString.get(groupByKey);
				}
				switch(this.o) {
				case MAX:
					if (strValueToAggregate.compareTo(defaultStr) >= 0 ) {
						defaultStr = strValueToAggregate;
					}
					break;
				case MIN:
					if (strValueToAggregate.compareTo(defaultStr) < 0 ) {
						defaultStr = strValueToAggregate;
					}
					break;
				}
				//put updated value to pockets
				mapString.put(groupByKey, defaultStr);
				//grab all tuples from hashmap
				if (numTuplesToProcess==numTuplesProcessed) {
			        for (Map.Entry<Field,String> entry : mapString.entrySet())  {
						Tuple resultTuple = new Tuple(this.td);
						resultTuple.setField(0, entry.getKey());
						resultTuple.setField(indexOFColumnToAggregate, new StringField(entry.getValue()));
						this.tuples.add(resultTuple);
			        }
			        System.out.println("-----"+this.tuples.size());
				}
			
			
			
			}
		}
		else {	throw new IllegalArgumentException();	}
		//your code here
		//
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		if (tuples!=null) {
			return this.tuples;
		}
		else {
			return null;
		}
		//your code here
//		return null;
	}

}
