package org.myorg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.nio.charset.CharacterCodingException;

public class Music_Jacc extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Music_Jacc(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		// Set attributes for job1
		String userIdVal = args[2]; // customer id given as command line argument
		String userid = userIdVal.substring(5, userIdVal.length()); // userid format in the dataset - "User_000006" - fetching last part(integer part)
		int id = Integer.parseInt(userid);
		Configuration confa = new Configuration();
		confa.set("user", userIdVal); // set user in configuration to access in job
		confa.setInt("userid", id); // set userid in configuration to access in job
		Job job1 = Job.getInstance(getConf(), "rating");
		job1.setJarByClass(this.getClass());
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[0] + "1/"));
		job1.setMapperClass(Map1.class);
		job1.setCombinerClass(Reduce1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.waitForCompletion(true);
		// New job to get intermediate out
		// Set attributes for job2
		Configuration conf1 = new Configuration();
		conf1.set("user", userIdVal);
		conf1.set("add", args[0]);
		Job findjob = Job.getInstance(conf1, "ranking");
		findjob.setJarByClass(this.getClass());
		findjob.setOutputKeyClass(Text.class);
		findjob.setOutputValueClass(Text.class);
		findjob.setMapperClass(findUserMap.class);
		findjob.setNumReduceTasks(0);
		FileInputFormat.addInputPath(findjob, new Path(args[0] + "1/"));
		FileOutputFormat.setOutputPath(findjob, new Path(args[0] + "2/"));
		findjob.waitForCompletion(true);
		// New job to get intermediate out
		// Set attributes for job3
		Configuration conf2 = new Configuration();
		conf2.set("add", args[0]);
		conf2.set("user", userIdVal);
		conf2.setInt("userid", id);
		Job Caljob = Job.getInstance(conf2, "try");
		Caljob.setJarByClass(this.getClass());
		Caljob.setOutputKeyClass(DoubleWritable.class);
		Caljob.setOutputValueClass(Text.class);
		Caljob.setMapperClass(SimilarFindMap.class);
		Caljob.setReducerClass(SimilarFindReduce.class);
		Caljob.setNumReduceTasks(1);
		FileInputFormat.addInputPath(Caljob, new Path(args[0] + "1/"));
		FileOutputFormat.setOutputPath(Caljob, new Path(args[1]));
		Caljob.waitForCompletion(true);
		// New job to get intermediate out
		// Set attributes for job4
		Configuration conf3 = new Configuration();
		conf3.set("add", args[1]);
		conf3.set("user", userIdVal);
		conf3.setInt("userid", id);
		Job finjob = Job.getInstance(conf3, "try");
		finjob.setJarByClass(this.getClass());
		finjob.setOutputKeyClass(Text.class);
		finjob.setOutputValueClass(Text.class);
		finjob.setMapperClass(FindMap.class);
		finjob.setReducerClass(FindReduce.class);
		FileInputFormat.addInputPath(finjob, new Path(args[1]));
		FileOutputFormat.setOutputPath(finjob, new Path(args[1] + "2/"));
		finjob.waitForCompletion(true);
		// New job to get intermediate out
		// Set attributes for job5
		Job rankOrdering = Job.getInstance(getConf(), " runrankordering ");
		rankOrdering.setJarByClass(this.getClass());
		rankOrdering.setMapperClass(RankingMapper.class);
		rankOrdering.setReducerClass(RankingReducer.class);
		rankOrdering.setMapOutputKeyClass(DoubleWritable.class);
		rankOrdering.setMapOutputValueClass(Text.class);
		rankOrdering.setSortComparatorClass(MyKeyComparator.class);
		rankOrdering.setOutputKeyClass(Text.class);
		rankOrdering.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(rankOrdering, new Path(args[1] + "2/"));
		FileOutputFormat.setOutputPath(rankOrdering, new Path(args[1] + "3/"));
		rankOrdering.setInputFormatClass(TextInputFormat.class);
		rankOrdering.setOutputFormatClass(TextOutputFormat.class);
		return rankOrdering.waitForCompletion(true) ? 0 : 1;

	}
	// custom key comparator to sort the recommended tracks in decreasing order of ratings instead of increasing one(which is default one)
	public static class MyKeyComparator extends WritableComparator {
		protected MyKeyComparator() {
			super(DoubleWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable key1 = (DoubleWritable) w1;
			DoubleWritable key2 = (DoubleWritable) w2;
			return -1 * key1.compareTo(key2);
		}
	}
	// Job 1 Mapper class  - Creates the records with information -userid,tracknames,trackids and ratings for all users 
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			String line = lineText.toString();
			String trackid = "";
			String userid = "";
			String rating = "";
			String result = "";
			String trackname = "";
			if (line.trim() != null) {
				String[] music = line.split("\t"); // Splitting the csv file
				if (music.length == 7 && music[4].length() != 0) { // Consider
																	// records
																	// where
																	// track id
																	// and name
																	// are
																	// available
					userid = music[0]; // get userid
					trackid = music[4]; // get trackid
					trackname = music[5]; // get trackname
					rating = music[6]; // get rating
					String value = "";
					result = trackid + "##" + trackname + "::" + rating + ";"; // Create
																				// string
																				// by
																				// combining
																				// track
																				// id,
																				// track
																				// name
																				// and
																				// rating
																				// with
																				// 3
																				// delimiters

					context.write(new Text(userid), new Text(result)); // emit
																		// userid
																		// and
																		// result
																		// as
																		// key
																		// and
																		// value
				}
			}
		}
	}
	// Job 1's Reducer class - Combine all the tracks grouped by users 
	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String musicval = "";
			for (Text musicvalu : values) {
				musicval = musicval + musicvalu.toString(); // combine
			}
			context.write(word, new Text(musicval));
		}
	}
	// Job 2 Mapper class  - Creates the records with information -userid,tracknames,trackids and ratings for customer whose id is given as the command line argument  

	public static class findUserMap extends
			Mapper<LongWritable, Text, Text, Text> {

		

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] userval = line.split("\\t");

			Configuration conf = context.getConfiguration();
			String userF = conf.get("user");  // get the user's information which is set in configuration 

			if (userval[0].equals("") || userval[0] == null) {
			} else if (userval[0].equals(userF)) {
				context.write(new Text(userF), new Text(userval[1])); //get the customer's information by comparing it with all users in 1st job's output file   
			}
		}
	}
	// Job 3 Mapper class  - This class is to find the users with same taste as customer to whom songs are getting recommended 

	public static class SimilarFindMap extends
			Mapper<LongWritable, Text, DoubleWritable, Text> {

	
		private static String query = "";
		private static HashMap<String, String> user1M = new HashMap<String, String>();  
		private static String userF = "";

		// stores the customer's tracks in a HashMap for easy access
		// Setup job to read the output file of 2nd job which contains the records of tracks of customer
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String add = conf.get("add") + "2/";
			FileSystem fs = FileSystem.get(conf);
			userF = conf.get("user");
			FileStatus[] status = fs.listStatus(new Path(add)); // you need to pass in your hdfs path
			for (int i = 0; i < status.length; i++) {

				if (status[i].getPath().toString().contains("part")
						&& !status[i].getPath().toString().endsWith("crc")) {
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(status[i].getPath())));
					String line;
					line = br.readLine();
					while (line != null) {    // Read the file line bye line 
						query = line.split("\\t")[1];
						String[] user1 = query.split(";");
						for (String record : user1) {
							String[] values = record.split("::");   // split the records to store trackname and id as key and rating as value in hash map
							user1M.put(values[0], values[1]);  
						}
						line = br.readLine();
					}
				}
				
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			Configuration conf = context.getConfiguration();
			String customer = conf.get("user");
			int userid = conf.getInt("userid", 1);
			customer = Integer.toString(userid);   // Take integer part of userid as string
			String[] compare = line.split("\\t");
			String user2 = compare[0];
			String[] tracks = compare[1].split(";");   //split the record with ";"  as delimiter to create the array of tracks for other users    

			int track_match_counter = 0; 
			int track_and_rating_match = 0; 

			HashMap<String, String> user2M = new HashMap<String, String>();  //Hash map 2M to store other user's tracks

			if (customer.equals(user2)) {
			} else { // consider only other users to find the similarity score , exclude the customer
				for (String track : tracks) {

					String[] values = track.split("::");
					if (values.length == 2)
						user2M.put(values[0], values[1]);
				}
				for (String trackid : user1M.keySet()) { // checking for tracks match
					if (user2M.keySet().contains(trackid)) {
						track_match_counter++;

						String user1rat = user1M.get(trackid);
						String user2rat = user2M.get(trackid);
						double rat1 = Double.parseDouble(user1rat);
						double rat2 = Double.parseDouble(user2rat);

						if (rat1 >= 3.0 && rat2 >= 3.0) {
							track_and_rating_match++;
						} else if (rat1 < 3.0 && rat2 < 3.0) {
							track_and_rating_match++;
						}
					}
				}

				if (track_and_rating_match == 0) {   //if no match in tracks or ratings
					double score = 1.0;
					if (user1M.size() == 0) {     //if customer is new user without past records, send all tracks to next job    
						context.write(new DoubleWritable(), new Text(line));
					}

				} else {
					double score = -(double) track_and_rating_match/ track_match_counter; // Score to calculate  similarity of
																// user
					context.write(new DoubleWritable(score), new Text(line)); // Sending
																				// score
																				// as
																				// key
																				// to
																				// get
																				// sorted
																				// list
																				// in
																				// reducer
				}
			}
		}
	}

	public static class SimilarFindReduce extends
			Reducer<DoubleWritable, Text, Text, Text> {

		private static String query = "";
		private static String userF = "";
		private static HashMap<String, String> user1M = new HashMap<String, String>();
		// stores the customer's tracks in a HashMap for easy access
		// Setup job to read the output file of 2nd job which contains the records of tracks of customer


		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String add = conf.get("add") + "2/";
			FileSystem fs = FileSystem.get(conf);
			userF = conf.get("user");
			FileStatus[] status = fs.listStatus(new Path(add)); // you need to
																// pass in your
																// hdfs path
			for (int i = 0; i < status.length; i++) {

				if (status[i].getPath().toString().contains("part")
						&& !status[i].getPath().toString().endsWith("crc")) {
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(status[i].getPath())));
					String line;
					line = br.readLine();
					while (line != null) {
						query = line.split("\\t")[1];
						String[] user1 = query.split(";");
						for (String record : user1) {
							String[] values = record.split("::");
							if (values.length == 2)
								user1M.put(values[0], values[1]);
						}
						line = br.readLine();
					}
				}

			}
		}

		@Override
		public void reduce(DoubleWritable word, Iterable<Text> listM,
				Context context) throws IOException, InterruptedException {
			for (Text lines : listM) {
				String line = lines.toString();
				HashMap<String, String> user2M = new HashMap<String, String>();
				String[] compare = line.toString().split("\\t");
				String[] tracks = compare[1].split(";");

				for (String record : tracks) {
					String[] values = record.split("::");
					if (values.length == 2)
						user2M.put(values[0], values[1]);
				}
				for (String key : user2M.keySet()) {
					if (user1M.keySet().contains(key)) {
					} else {
						String user2rat = user2M.get(key);
						double rat1 = (Double.parseDouble(user2rat));
						if (rat1 >= 3.0) {
							context.write(new Text(key), new Text(compare[0]
									+ "DDDD" + "song"));

						}
					}
				}
				
			}
		}
	}
	// Job 4 Mapper class  - This class is to give ratings to the tracks based on no of users with similar rating and users with similar intrest
	public static class FindMap extends Mapper<LongWritable, Text, Text, Text> {

		@SuppressWarnings("unused")
		private static String query = "";
		private static String userF = "";
		private static Set<Integer> unique_users_list = new HashSet<Integer>();
		
		// Setup job to read the output file of 3rd job to calculate unique users count who have highly rated the tracks

		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String add = conf.get("add");
			FileSystem fs = FileSystem.get(conf);
			userF = conf.get("user");
			FileStatus[] status = fs.listStatus(new Path(add)); // you need to
																// pass in your
																// hdfs path
			for (int i = 0; i < status.length; i++) {

				if (status[i].getPath().toString().contains("part")
						&& !status[i].getPath().toString().endsWith("crc")) {
					BufferedReader br = new BufferedReader(
							new InputStreamReader(fs.open(status[i].getPath())));
					String line;
					line = br.readLine();
					while (line != null) {
						query = line.split("\\t")[1];
						String[] user1 = query.split("DDDD");
						unique_users_list.add(Integer.parseInt(user1[0]
								.substring(5, user1[0].length())));

						line = br.readLine();
					}
				}
			}

		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String mid[] = line.split("\\t");

			String[] name_id = mid[0].split("##");
			if (name_id.length == 2)
				context.write(new Text(name_id[1]),
						new Text(String.valueOf(unique_users_list.size())));
			

		}
	}

	public static class FindReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Text stars = new Text();

			int count = 0;
			for (Text musicvalu : values) {
				count++;
				stars = musicvalu;
			}
			// logic to assign ratings from 1 to 5 considering percentage distribution of users with similar interest
			float div = count / (Integer.parseInt(stars.toString()));
			div = div * 100;
			if (div <= 10)
				context.write(new Text(String.valueOf(1)), word);
			else if (div <= 30 && div > 10)
				context.write(new Text(String.valueOf(2)), word);
			else if (div <= 60 && div > 30)
				context.write(new Text(String.valueOf(3)), word);
			else if (div <= 80 && div > 60)
				context.write(new Text(String.valueOf(4)), word);
			else
				context.write(new Text(String.valueOf(5)), word);
		}
	}

	// Job 5 Mapper class  - This class is to sort the tracks according to ranks in decreasing order  
	public static class RankingMapper extends
			Mapper<LongWritable, Text, DoubleWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			 String line  = value.toString();
                    String[] words=line.split("\\t");

			int parseInt = Integer.parseInt(words[0]);

			Text page = new Text(words[1]);
			DoubleWritable rank = new DoubleWritable(parseInt);

			context.write(rank, page);
		}

	}

	// Reducer job to replace ranks got from privious job with stars

	public static class RankingReducer extends
			Reducer<DoubleWritable, Text, Text, Text> {
		@Override
		public void reduce(DoubleWritable rank, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Text count = new Text();
			for (Text page : values) {

				String rank_stars = rank.toString();
				if (Double.parseDouble(rank_stars) == 5.0)
					context.write(page, new Text("*****"));
				else if (Double.parseDouble(rank_stars) == 4.0)
					context.write(page, new Text("****"));
				else if (Double.parseDouble(rank_stars) == 3.0)
					context.write(page, new Text("***"));
				else if (Double.parseDouble(rank_stars) == 2.0)
					context.write(page, new Text("**"));
				else
					context.write(page, new Text("*"));

			}

		}

	}

}
