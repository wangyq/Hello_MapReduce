package com.siwind.routingloop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Currency;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Charsets;

import java.util.Collections;

public class RoutingLoopDetection {

	/**
	 * Read four strings from each line and generate a key, value pair as (<ip,
	 * mask_len>, <from,to>) for example: 192.168.1.0 24 1 3
	 * */
	public static class MapClass extends
			Mapper<Object, Text, Text, PairInt> {

		private final Text key = new Text();
		private final PairInt value = new PairInt();
		
		@Override
		public void map(Object inKey, Text inValue, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(inValue.toString());
			int ipaddr = 0, masklen = 0, from = 0, to = 0;

			if (itr.hasMoreTokens()) {
				ipaddr = IpUtil.Ip2Int(itr.nextToken());
			}
			if (itr.hasMoreTokens()) {
				masklen = Integer.parseInt(itr.nextToken());
			}
			if (itr.hasMoreTokens()) {
				from = Integer.parseInt(itr.nextToken());
			}
			if (itr.hasMoreTokens()) {
				to = Integer.parseInt(itr.nextToken());
			}
			
			//
			ipaddr = IpUtil.getNetInt(ipaddr, masklen);
			
			key.set(IpUtil.Ip2Str(ipaddr)+"/"+masklen);
			value.set(from, to);
			context.write(key, value);
			//System.out.println(key.toString() + "-->" + value.toString());
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class ReduceClass extends Reducer<Text, PairInt, Text, Text> {
		private ArrayList<PairInt> pairs = new ArrayList<PairInt>();
		private Text value = new Text();

		@Override
		public void reduce(Text key, Iterable<PairInt> values, Context context)
				throws IOException, InterruptedException {

			int i = 0;
			boolean isLooping = false;
			
			//ArrayList<PairInt> pairs = new ArrayList<PairInt>();
			pairs.clear(); // clear all data in it!

			for (PairInt v : values) {
				pairs.add(v.clone());    //why here must be clone()?
				//System.out.println(v.toString());
				// context.write(first, value);
			}
			
			//Collections.sort(pairs); //sort first!
			isLooping = isRoutingLoop(pairs);
			
			StringBuilder str = new StringBuilder();
			for (i = 0; i < pairs.size() - 1; i++) {
				str.append(pairs.get(i).toString() + ",");
			}
			str.append(pairs.get(i).toString());

			str.append(" --> ");
			if (isLooping) {
				str.append("YES");
			} else {
				str.append("NO");
			}

			value.set(str.toString());
			context.write(key, value);
			
			//System.out.println("Reduce: " + key.toString() + "-->" + value.toString());
			
		}
	}

	/**
	 * 
	 * @param pairs
	 * @return
	 */
	public static boolean isRoutingLoop(ArrayList<PairInt> pairs) {
		boolean isLoop = false;

		//sort first!
		Collections.sort(pairs);
		
		if (null != pairs) {
			int last = 0;
			int i = 0, j = 0;
			PairInt p;
			
			for (i = 0; i < pairs.size()-1 && !isLoop; i++) {
				last = i;  //
				for(j=i+1;j<pairs.size();j++){//find a loop from start node of i
					p = pairs.get(j);
					if( pairs.get(last).getSecond() == p.getFirst() ){ //
						last = j;
						if( pairs.get(i).getFirst() == p.getSecond() ){
							isLoop = true;
							break;
						}
					}
					
				}
			}
		}
		return isLoop;
	}

	public static void findLooping(Path path, Configuration conf)
			throws IOException {

		FileSystem fs = FileSystem.get(conf);
		Path file = new Path(path, "part-r-00000");

		if (!fs.exists(file))
			throw new IOException("Output not found!");

		BufferedReader br = null;

		// average = total sum / number of elements;
		try {
			br = new BufferedReader(new InputStreamReader(fs.open(file),
					Charsets.UTF_8));

			long count = 0;
			long length = 0;

			String line;
			while ((line = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line);
			}

		} finally {
			if (br != null) {
				br.close();
			}
		}
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: RoutingLoopDetection <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "RoutingLoopDetection");
		job.setJarByClass(RoutingLoopDetection.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		// the map output is IntPair, IntWritable
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PairInt.class);

		// the reduce output is Text, IntWritable
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path inputpath = new Path(otherArgs[0]);
		Path outputpath = new Path(otherArgs[1]);

		FileSystem.get(conf).delete(outputpath,true);
		
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
		
		
		boolean result = job.waitForCompletion(true);
		findLooping(outputpath, conf);

		// return (result ? 0 : 1);

	}
}