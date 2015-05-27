package com.siwind.routingloop;

import com.google.common.base.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;

public class RoutingLoopDetection {

    /**
     * @param pairs
     * @return
     */
    public static boolean isRoutingLoop(ArrayList<PairInt> pairs, ArrayList<PairInt> looping) {
        boolean isLoop = false;

        //sort first!
        Collections.sort(pairs);

        if (null != pairs) {
            int i = 0, j = 0;
            PairInt p;
            boolean bFlags = true;

            for (i = 0; i < pairs.size() - 1 && !isLoop && bFlags; i++) {
                int first = pairs.get(i).getFirst();
                int last = pairs.get(i).getSecond();
                bFlags = false;
                for (j = i + 1; j < pairs.size(); j++) {//find whether a loop occur from start node  i
                    p = pairs.get(j);
                    if (last == p.getFirst()) {
                        last = p.getSecond();
                        if (first == last) {// is looping now
                            isLoop = true;
                            looping.add(pairs.get(i));
                            looping.add(p);
                            break;
                        }
                    } else {
                        bFlags = true;  // the link broken.
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
        //job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(PairInt.class);
        job.setMapOutputValueClass(PairInt.class);

        // the reduce output is Text, IntWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputpath = new Path(otherArgs[0]);
        Path outputpath = new Path(otherArgs[1]);

        FileSystem.get(conf).delete(outputpath, true);

        FileInputFormat.addInputPath(job, inputpath);
        FileOutputFormat.setOutputPath(job, outputpath);


        boolean result = job.waitForCompletion(true);
        findLooping(outputpath, conf);

        // return (result ? 0 : 1);

    }

    /**
     * Read four strings from each line and generate a key, value pair as
     * (<ip, mask_len>, <from,to>)
     * for example:
     * 192.168.1.0 24 1 3
     */
    public static class MapClass extends
            Mapper<Object, Text, PairInt, PairInt> {
            //Mapper<Object, Text, Text, PairInt> {

        //private final Text key = new Text();
        private final PairInt key = new PairInt();
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

            key.set(ipaddr, masklen);
            value.set(from, to);
            context.write(key, value);
            //System.out.println(key.toString() + "-->" + value.toString());
        }
    }

    /**
     * A reducer class that just emits the sum of the input values.
     */
    public static class ReduceClass
            //extends Reducer<Text, PairInt, Text, Text> {
            extends Reducer<PairInt, PairInt, Text, Text> {
        private ArrayList<PairInt> pairs = new ArrayList<PairInt>();
        private ArrayList<PairInt> looping = new ArrayList<PairInt>();
        private Text keyText = new Text();
        private Text value = new Text();

        @Override
        public void reduce(PairInt key, Iterable<PairInt> values, Context context)
        //public void reduce(Text key, Iterable<PairInt> values, Context context)
                throws IOException, InterruptedException {

            int i = 0;
            boolean isLooping = false;

            //ArrayList<PairInt> pairs = new ArrayList<PairInt>();
            pairs.clear(); // clear all data in it!
            looping.clear();

            for (PairInt v : values) {
                //why here must be clone()? v is a temproraley variable,and should not using it!
                pairs.add(v.clone());
                //pairs.add(v);
                //System.out.println(v.toString());
            }

            //Collections.sort(pairs); //sort first!
            isLooping = isRoutingLoop(pairs, looping);

            StringBuilder str = new StringBuilder("(");
            for (i = 0; i < pairs.size() - 1; i++) {
                str.append(pairs.get(i).toString() + ",");
            }
            str.append(pairs.get(i).toString() + ")");

            if (looping.size() > 0) {//find looping
                str.append(" [" + looping.get(0) + "," + looping.get(1) + "]");
            }

            str.append(" --> ");
            if (isLooping) {
                str.append("YES");
            } else {
                str.append("NO");
            }

            value.set(str.toString());
            keyText.set(IpUtil.Ip2Str(key.getFirst())+"/"+key.getSecond());
            context.write(keyText, value);
            //context.write(key, value);

            //System.out.println("Reduce: " + key.toString() + "-->" + value.toString());

        }
    }
}
