package hw2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.OptionalDouble;
import static java.util.stream.Collectors.groupingBy;

import org.junit.jupiter.params.shadow.com.univocity.parsers.common.fields.FieldSet;

import hw1.Field;
import hw1.IntField;
import hw1.RelationalOperator;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;




/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		if (l == null || td == null) {	throw new IllegalArgumentException();	}
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//TODO

		ArrayList<Tuple> newTupleList = new ArrayList<>();
        float OPERAND = Float.valueOf(operand.toString());
        
		//iterate this tuple list	
		for (Tuple t: this.tuples) {
	        float currentValue = Float.valueOf(t.getField(field).toString());
		switch(op) {
			//EQ("="), GTE(">="), GT(">"), LT("<"), LTE("<=");
			case GT:
				if (currentValue > OPERAND) {  
					newTupleList.add(t);
				}
				break;
			case LT:
				if (currentValue < OPERAND) {  
					newTupleList.add(t);
				}
				break;
			case EQ:
				if (currentValue == OPERAND) { 
					newTupleList.add(t);
				}
				break;
			case GTE:
				if (currentValue >= OPERAND) {  
					newTupleList.add(t);
				}
				break;
			case LTE:
				if (currentValue <= OPERAND) {  
					newTupleList.add(t);
				}
				break;
			case NOTEQ:
				if (currentValue != OPERAND) {  
					newTupleList.add(t);
				}
				break;
		}
		//your code here
		}
		return new Relation(newTupleList,td);

	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		if (fields.size() != names.size()) {	throw new IllegalArgumentException();	}
		for (int i = 0; i < fields.size(); i++) {
			this.td.setFieldName(i, names.get(i));
		}
		//your code here
		return new Relation(tuples, td);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//TODO
		
		//create new tupleDesc
		List<Type> typeArrThis = new ArrayList<>();
		List<String> fieldArrThis = new ArrayList<>();

		//iterate field number to select fields
		for (int fieldIndex: fields) {
			typeArrThis.add(this.td.getType(fieldIndex));
			fieldArrThis.add(this.td.getFieldName(fieldIndex));
		}
		//create new tupleDesc
		Type[] typeArr = typeArrThis.toArray(Type[]::new);
		String[] fieldArr = fieldArrThis.toArray(String[]::new);
		TupleDesc newTupleDesc = new TupleDesc(typeArr, fieldArr);
		
		
		//create new tuples
		ArrayList<Tuple> newTupleList = new ArrayList<>();
		for (int i=0; i<this.tuples.size(); i++) {
			//for each tuple, keep wanted columns
			Tuple newTuple = new Tuple(newTupleDesc);
			int j = 0;
			for (int fieldIndex: fields) {
				newTuple.setField(j, tuples.get(i).getField(fieldIndex));
				j++;
			}
			newTupleList.add(newTuple);
		}
				
		//your code here
		return new Relation(newTupleList, newTupleDesc);

	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		
		/**
		 * create a larger tupleDesc
		 */
		//old arr
		List<Type> typeArrThis = new ArrayList<>(Arrays.asList(this.td.getTypes()));
		List<String> fieldArrThis = new ArrayList<>(Arrays.asList(this.td.getFields()));
		//other arr
		List<Type> typeArrOther = new ArrayList<>(Arrays.asList(other.td.getTypes()));
		List<String> fieldArrOther = new ArrayList<>(Arrays.asList(other.td.getFields()));
		//typeArrOther.remove(field2);
		//fieldArrOther.remove(field2);
		
		//concatenate
		typeArrThis.addAll(typeArrOther);
		fieldArrThis.addAll(fieldArrOther);
		//convert back to array
		Type[] typeArrCombined = typeArrThis.toArray(Type[]::new);
		String[] fieldArrCombined = fieldArrThis.toArray(String[]::new);

		//create new tuple Desc
		TupleDesc tupleDescCombined = new TupleDesc(typeArrCombined, fieldArrCombined);
		
			
		/**
		 * produce a larger tuple to include all columns
		 */
		ArrayList<Tuple> joinedTupleList = new ArrayList<>();
		//iterate this.tuples
		for (int j=0; j<this.tuples.size(); j++) {
			//iterate other.tuples
			for (int k=0; k<other.tuples.size(); k++) {
				//if found match, add a new row combining two
				if (this.tuples.get(j).getField(field1).equals(other.tuples.get(k).getField(field2))) {
					Tuple joinedTuple = new Tuple(tupleDescCombined);
					//set fields of combined tuple
					for (int i=0; i < td.numFields();i++) {
						joinedTuple.setField(i, this.tuples.get(j).getField(i));
					}
					for (int i=0; i<other.td.numFields();i++) {
						joinedTuple.setField(i + td.numFields(), other.tuples.get(k).getField(i));
					}
					joinedTupleList.add(joinedTuple);
				}			
			}
		}
				
		//your code here
		return new Relation(joinedTupleList, tupleDescCombined);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here


		ArrayList<Tuple> newTupleList = new ArrayList<>();
		TupleDesc newTupleDesc = td;
	    Field newField;
	    
		Aggregator ag = new Aggregator(op, groupBy, newTupleDesc);
		for (Tuple tuple: tuples) {
			ag.merge(tuple);
		}
		//get resutl from aggretator
		newTupleList = ag.getResults();
		
		for (Tuple tuple: tuples) {
			this.aggregate(op, groupBy);
		}

		

		
		
		return new Relation(newTupleList, newTupleDesc);
	}
	
	public TupleDesc getDesc() {
		return this.td;
		//your code here
	}
	
	public ArrayList<Tuple> getTuples() {
		return this.tuples;
		//your code here
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("TupleDesc " + td.toString());
		sb.append("\n");
		for (Tuple t: tuples) {
		    sb.append(t.toString());
		    sb.append("\n");
		}
		//your code here
		return sb.toString();
	}
}
