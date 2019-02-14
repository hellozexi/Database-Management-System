package hw1;

import java.sql.Types;
import java.util.HashMap;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	private TupleDesc tupleDesc;
	private int pageId;
	private int tupleId;
	private Field[] fields;
	
	
	
	public Tuple(TupleDesc t) {
//		store actual data
		//your code here
		if ( t == null ) {  return;  }
		
		this.tupleDesc = t;
		this.fields = new Field[t.numFields()];
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.tupleDesc;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return pageId;
	}

	public void setPid(int pid) {
		//your code here
		this.pageId = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
		return this.tupleId;
	}

	public void setId(int id) {
		//your code here
		this.tupleId = id;
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		this.tupleDesc = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		if ( v == null) {  return;  }
//		System.out.println(i);
//		System.out.println(this.tupleDesc.numFields());
//		System.out.println(v.getType());

		if ( i < 0 ) {  return;  }
		else if (i > this.tupleDesc.numFields()-1) {  return;  }
		else {
//			System.out.println(this.fields);

			this.fields[i] = v;

		}
		
	}
	
	
	/**
	 * given i
	 * will return i th field
	 */
	public Field getField(int i) {
		//your code here
		return fields[i];
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		StringBuilder sBuilder = new StringBuilder();
		for(int i = 0; i < fields.length; i++) {
			sBuilder.append(fields[i].toString() + "\n");
		}
		return sBuilder.toString();
	}
}
	