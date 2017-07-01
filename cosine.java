package org.myorg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.fs.*;
import java.lang.Math.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.nio.charset.CharacterCodingException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class cosine extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new cosine(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		// Set attributes for job1
		String userIdVal = args[2]; // customer id given as command line
									// argument
		String userid = userIdVal.substring(5, userIdVal.length());
		int id = Integer.parseInt(userid);

		Configuration confa = new Configuration();

		confa.set("user", userIdVal); // set user in configuration to access in
										// job
		confa.setInt("userid", id); // set userid in configuration to access in
									// job

		Job job1 = Job.getInstance(getConf(), "rating"); // Initializing the
															// rating job
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
		Caljob.setOutputKeyClass(Text.class);
		Caljob.setOutputValueClass(Text.class);
		Caljob.setMapperClass(pearsonScoreMap.class);
		Caljob.setReducerClass(scoreReduce.class);
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
		Job Caljob1 = Job.getInstance(conf3, "try");
		Caljob1.setJarByClass(this.getClass());
		Caljob1.setOutputKeyClass(Text.class);
		Caljob1.setOutputValueClass(Text.class);
		Caljob1.setMapperClass(pearsonRecomMap.class);
		Caljob1.setReducerClass(FindReduce.class);
		Caljob1.setNumReduceTasks(1);
		FileInputFormat.addInputPath(Caljob1, new Path(args[1]));
		FileOutputFormat.setOutputPath(Caljob1, new Path(args[1] + "2/"));
		Caljob1.waitForCompletion(true);
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

	// custom key comparator to sort the recommended tracks in decreasing order
	// of ratings instead of increasing one(which is default one)
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

	// Job 1 Mapper class - Creates the records with information
	// -userid,tracknames,trackids and ratings for all users
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
					rating = music[6]; // get rating
					trackname = music[5]; // get trackname
					String value = "";
					result = trackname + "TTTT" + trackid + "::" + rating + ";"; // Create
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
				musicval = musicval + musicvalu.toString();
			}
			context.write(word, new Text(musicval));
		}
	}

	// Job 2 Mapper class - Creates the records with information
	// -userid,tracknames,trackids and ratings for customer whose id is given as
	// the command line argument

	public static class findUserMap extends
			Mapper<LongWritable, Text, Text, Text> {

		// Mapper to find the single given user

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] userval = line.split("\\t");

			Configuration conf = context.getConfiguration(); // get the user's
																// information
																// which is set
																// in
																// configuration
			String userF = conf.get("user");

			if (userval[0].equals("") || userval[0] == null) {
			} else if (userval[0].equals(userF)) {
				context.write(new Text(userF), new Text(userval[1])); // get the
																		// customer's
																		// information
																		// by
																		// comparing
																		// it
																		// with
																		// all
																		// users
																		// in
																		// 1st
																		// job's
																		// output
																		// file
			}
		}
	}

	// Job 3 Mapper class - This class is to find the users with same taste as
	// customer to whom songs are getting recommended

	public static class pearsonScoreMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private static String query = "";
		private static HashMap<String, String> userFHash = new HashMap<String, String>();
		private static String userF = "";

		// stores the customer's tracks in a HashMap for easy access
		// Setup job to read the output file of 2nd job which contains the
		// records of tracks of customer
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
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
					while (line != null) { // Read the file line bye line
						query = line.split("\\t")[1];
						String[] songs1 = query.split(";");

						for (String song : songs1) {
							String[] values = song.split("::"); // split the
																// records to
																// store
																// trackname and
																// id as key and
																// rating as
																// value in hash
																// map
							userFHash.put(values[0], values[1]);

						}

						line = br.readLine();
					}
				}
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			int cnt = 0;
			double sum = 0.0;
			double sum1 = 0.0;
			double score = 0.0;
			double avg_rating_user1 = 0.0;
			double avg_rating_user2 = 0.0;
			double mul = 0.0;
			double sq2 = 0.0;
			double sq1 = 0.0;
			String result = "";
			List<String> coommonrating1 = new ArrayList<>();
			List<String> coommonrating2 = new ArrayList<>();
			double cosinesim = 0.0;

			String line = value.toString();
			Configuration conf = context.getConfiguration();
			String userF = conf.get("user");
			String[] compare = line.split("\\t");
			String user2 = compare[0];
			String[] songs2 = compare[1].split(";");
			int x = songs2.length;
			for (String songs : songs2) {

				String[] song = songs.split("::"); // split the record with ";"
													// as delimiter to create
													// the array of tracks for
													// other users

			}

			int flag = 0;

			HashMap<String, String> user2Hash = new HashMap<String, String>(); // Hash
																				// map
																				// 2M
																				// to
																				// store
																				// other
																				// user's
																				// tracks
			String tracks = "";
			if (userF.compareTo(user2) == 0) {
			} else { // consider only other users to find the similarity score ,
						// exclude the customer

				for (String song : songs2) {
					String[] values = song.split("::");
					user2Hash.put(values[0], values[1]);
					if (Integer.parseInt(values[1]) >= 3)
						tracks = tracks + song + ";";

				}
			}
			String uni_songID = "";
			for (String songID : userFHash.keySet()) { // checking for the key
														// in both keysets
				if (user2Hash.keySet().contains(songID)) {
					for (String mtch : user2Hash.keySet())
						coommonrating2.add(user2Hash.get(mtch));
					for (String match : userFHash.keySet()) {
						coommonrating1.add(userFHash.get(match));
					}
					int size_array = coommonrating1.size();

					for (int i = 0; i < size_array; i++) {

						mul += Double.parseDouble(coommonrating1.get(i))
								* Double.parseDouble(coommonrating2.get(i));

					}
					for (int j = 0; j < size_array; j++) {
						sq1 += Double.parseDouble(coommonrating1.get(j))
								* Double.parseDouble(coommonrating1.get(j));
						sq2 += Double.parseDouble(coommonrating2.get(j))
								* Double.parseDouble(coommonrating2.get(j));

					}
					sq1 = Math.sqrt(sq1);
					sq2 = Math.sqrt(sq2);
					cosinesim = mul / Math.max((sq1 * sq2), 1);
					tracks = tracks.replace(
							songID + "::" + user2Hash.get(songID) + ";", "");
					result = user2 + "##" + tracks + "PPPP" + cosinesim;
					uni_songID = songID;

				}

			}
			if (result.length() != 0 && tracks.length() != 0)
				context.write(new Text(uni_songID), new Text(result));
		}
	}

	// reducer

	public static class scoreReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String musicval = "";
			for (Text musicvalu : values) {
				String id_score[] = (musicvalu.toString()).split("PPPP");
				if (id_score.length == 2)
					context.write(new Text(id_score[0]), new Text(id_score[1])); // userid##tracks,
																					// score
			}

		}

	}

	// Job 4 Mapper class - This class is to give ratings to the tracks based on
	// no of users with similar rating and users with similar intrest

	public static class pearsonRecomMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private static String query = "";
		private static HashMap<String, String> userFHash = new HashMap<String, String>();
		private static String userF = ""; // command line user
		private static Set<Integer> unique_users_list = new HashSet<Integer>();

		// Setup job to read the output file of 3rd job to calculate unique
		// users count who have highly rated the tracks

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
						query = line.split("\\t")[0];
						String[] songs1 = query.split("##");

						unique_users_list.add(Integer.parseInt(songs1[0]
								.substring(5, songs1[0].length())));
						line = br.readLine();

					}
				}
			}

		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] userval = line.split("\\t");

			Configuration conf = context.getConfiguration();
			String userF = conf.get("user");
			String[] user_track = userval[0].split("##");
			if (user_track.length == 2) {

				for (String str : (user_track[1].split(";"))) {
					if (str.length() != 0) {
						String[] track_rating = str.split("::");
						if (track_rating.length == 2) {

							String[] name_id = track_rating[0].split("TTTT");

							context.write(
									new Text(name_id[0]),
									new Text(user_track[0]
											+ "DDDD"
											+ String.valueOf(unique_users_list
													.size())));

						}
					}

				}
			}
		}

	}// mapper class end

	public static class FindReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Set<Integer> unique_users_ratings = new HashSet<Integer>();

			Text stars = new Text();
			int count = 0;
			for (Text musicvalu : values) {
				String[] users = musicvalu.toString().split("DDDD");
				unique_users_ratings.add(Integer.parseInt(users[0].substring(5,
						users[0].length())));
				stars = new Text(users[1]);
			}
			// logic to assign ratings from 1 to 5 considering percentage
			// distribution of users with similar interest
			float div = unique_users_ratings.size()
					/ (Integer.parseInt(stars.toString()));
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

	// Job 5 Mapper class - This class is to sort the tracks according to ranks
	// in decreasing order
	public static class RankingMapper extends
			Mapper<LongWritable, Text, DoubleWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split("\\t");

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

