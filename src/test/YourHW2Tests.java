package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.IntField;
import hw1.Tuple;
import hw1.TupleDesc;
import hw2.AggregateOperator;
import hw2.Relation;

public class YourHW2Tests {

	private HeapFile testhf;
	private TupleDesc testtd;
	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;

	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
		
		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
	}
	
	
	/**
	 * check construcor or Relation Class and getters
	 */
	@Test
	public void testRelationGetters() {
//		try {
//			Tuple t = new Tuple(td);
//			t.setField(0, new IntField(new byte[] {0, 0, 0, (byte)131}));
//			byte[] s = new byte[129];
//			s[0] = 2;
//			s[1] = 98;
//			s[2] = 121;
//			t.setField(1, new StringField(s));
//			Tuple t2 = t;
//			ArrayList<Tuple> tupleList = new ArrayList<>();
//			tupleList.add(t);
//			tupleList.add(t2);
//			
//			Relation relation = new Relation(tupleList, td);
//			assertEquals("check getter of getDesc ", relation.getDesc(), td);
//			assertEquals("constructor ", relation.getTuples(), tupleList);
//		} catch (Exception e) {
//			fail("error in test");
//			e.printStackTrace();
//		}
	}
	
	
	/**
	 *	input 1,2,3,1
	 */
	@Test
	public void testAggregateCount() {
		try {
			
			Tuple tupleA = ahf.getAllTuples().get(0);
			Tuple tupleB = ahf.getAllTuples().get(1);
			Tuple tupleC = ahf.getAllTuples().get(2);
	
			ArrayList<Tuple> tupleList = new ArrayList<>();
			tupleList.add(tupleA);
			tupleList.add(tupleB);
			tupleList.add(tupleC);
			tupleList.add(tupleA);
			Relation relation = new Relation(tupleList, atd);
			
			ArrayList<Integer> c = new ArrayList<Integer>();
			c.add(1);
			relation = relation.project(c);
			
			Relation result = relation.aggregate(AggregateOperator.COUNT, false);			
			IntField agg = (IntField) result.getTuples().get(0).getField(0);
			assertTrue(agg.getValue() == 4);
			
		} catch (Exception e) {
			fail("error in test");
			e.printStackTrace();
		}
	}
	
	
	
	
	@Test
	public void testAggregateMin() {
		try {
			
			Tuple tupleA = ahf.getAllTuples().get(0);
			Tuple tupleB = ahf.getAllTuples().get(1);
			Tuple tupleC = ahf.getAllTuples().get(2);
	
			ArrayList<Tuple> tupleList = new ArrayList<>();
			tupleList.add(tupleA);
			tupleList.add(tupleB);
			tupleList.add(tupleC);
			tupleList.add(tupleA);

			Relation relation = new Relation(tupleList, atd);
			
			ArrayList<Integer> c = new ArrayList<Integer>();
			c.add(1);
			relation = relation.project(c);
			
			Relation result = relation.aggregate(AggregateOperator.MIN, false);			
			IntField agg = (IntField) result.getTuples().get(0).getField(0);
			assertTrue(agg.getValue() == 1);
			
		} catch (Exception e) {
			fail("error in test");
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testAggregateMax() {
		try {
			
			Tuple tupleA = ahf.getAllTuples().get(0);
			Tuple tupleB = ahf.getAllTuples().get(1);
			Tuple tupleC = ahf.getAllTuples().get(2);
	
			ArrayList<Tuple> tupleList = new ArrayList<>();
			tupleList.add(tupleA);
			tupleList.add(tupleB);
			tupleList.add(tupleC);
			tupleList.add(tupleA);

			Relation relation = new Relation(tupleList, atd);
			
			ArrayList<Integer> c = new ArrayList<Integer>();
			c.add(1);
			relation = relation.project(c);
			
			Relation result = relation.aggregate(AggregateOperator.MAX, false);			
			IntField agg = (IntField) result.getTuples().get(0).getField(0);
			assertTrue(agg.getValue() == 3);
			
		} catch (Exception e) {
			fail("error in test");
			e.printStackTrace();
		}
	}
	
	
	

}
