package com.siwind.routingloop;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.WritableComparable;



/**
 * 
 * @author user
 *
 */
public class SortedArrayWritable extends ArrayWritable {

	private Class<? extends WritableComparable> valueClass;
	private WritableComparable[] values;

	/**
	 * sort for values
	 */
	private void sort(WritableComparable[] v){
		
		Arrays.sort(v);
		
//		if( null == v ) return;
//		boolean flags = true;
//		
//		for( int i=v.length-1;i>0 && flags;i--){ //buble sort
//			
//			flags = false;        //no sequance!
//			for(int j=0;j<i;j++){
//				if( v[j].compareTo(v[j+1]) > 0 ){ //exchange the bigger to next location.
//					WritableComparable tmp = v[j];
//					v[j] = v[j+1];
//					v[j+1] = tmp;
//					
//					flags = true;
//				}
//			}
//		}
	}
	public SortedArrayWritable(Class<? extends WritableComparable> valueClass) {
		super(valueClass);
		// TODO Auto-generated constructor stub
	}

	public SortedArrayWritable(Class<? extends WritableComparable> valueClass,WritableComparable[] values) {
		this(valueClass);
		sort(values);           //sort first 
		super.set(values);      //update super class
		
	}
	
	public SortedArrayWritable(String[] strings) { //here may be NOT necessary?
		super(strings);
		sort((WritableComparable[])get());
	}
	
	public void add(WritableComparable v){ //no need here!
		
	}
}
