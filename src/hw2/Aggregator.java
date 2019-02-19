package hw2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

import hw1.Field;
import hw1.IntField;
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
	
		
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		this.o = o;
		this.td = td;
		this.groupBy = groupBy;
		this.tuples = new ArrayList();
		//your code here

	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
//		TODO
//		System.out.println("merging "+ t.toString());

		int indexOFColumnToAggregate;
		if  (groupBy == true) {	indexOFColumnToAggregate = 1;	}
		else {	indexOFColumnToAggregate = 0;	}
//		System.out.println("merging "+ t.toString());

		//check if we have a intField
		if (t.getField(indexOFColumnToAggregate) instanceof IntField) {
			IntField intFiledToAggregate = (IntField) t.getField(indexOFColumnToAggregate);
			int intValueToAggregate = intFiledToAggregate.getValue();


			//this.tuples.add(t);
			//assume that the relation being aggregated has a single column being aggregated
			int res = 0;
			Tuple resultTuple = new Tuple(this.td);

			switch(this.o) {
			case MAX:
				if (intValueToAggregate > res) {
					res = intValueToAggregate;
					System.out.println(res);
				}
	
				break;
			case MIN:
	
				break;
			case AVG:
	
				break;
			case COUNT:
	
				break;
			case SUM:
				res = res + intValueToAggregate;
				System.out.println("res "+ res);

				break;
			}
			
			//create new Tuple
			//add new tuple to arrayList
			if (this.tuples.size() == 0) {
				resultTuple.setField(indexOFColumnToAggregate, new IntField(res));
				this.tuples.add(resultTuple);
			}
			else {
				this.tuples.get(0).setField(indexOFColumnToAggregate, new IntField(res));
			}
			

		
		
		}
		else {	throw new IllegalArgumentException();	}
		
		
		
		//your code here
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		return this.tuples;
		//your code here
//		return null;
	}

}
