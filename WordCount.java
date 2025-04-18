import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
// FileSplit fileSplit = (FileSplit)context.getInputSplit();
// String filename = fileSplit.getPath().getName();
      String line = value.toString();
      String[] fields = line.split(";");

      if (fields.length != 6) { //titleid, titletype, title, year, rating, genre(s)
        return;
      }

      String titleType = fields[1];
      String titleYear = fields[3];
      String titleRating = fields[4];
      String[] titleGenre = fields[5].split(",");

      // ignore all titles that are not 'movie'
      if (!titleType.equals("movie")) {
        return;
      }

      int year = 0;
      try { year = Integer.parseInt(titleYear); 
      } catch (NumberFormatException e) {
        return;
      }

      String yearRange = null;

      if (year >= 1991 && year <= 2000) {
        yearRange = "[1991-2000]";
      }
      else if (year >= 2001 && year <= 2010) {
        yearRange = "[2001-2010]";
      }
      else if (year >= 2011 && year <= 2020) {
        yearRange = "[2011-2020]";
      }
      else {
        return; //ignore all other years
      }

      double rating = 0.0;

      try { rating = Double.parseDouble(titleRating); 
      } catch (NumberFormatException e) {
        return;
      }
      
      if (rating < 7.0)
      { return; }

      List<String> genres = new ArrayList<>();

      for (String genre : titleGenre) {
        genres.add(genre);
      }

      String[][] genrePairs = {
        {"Action","Thriller"},
        {"Adventure", "Drama"},
        {"Comedy", "Romance"}
      };

      for (String[] pair : genrePairs) {
        if (genres.contains(pair[0]) && genres.contains(pair[1])) {
          String writeKey = String.format("%s,%s,%s", yearRange, pair[0], pair[1]);
          word.set(writeKey);
          context.write(word, one);
        }
      }

      

      // StringTokenizer itr = new StringTokenizer(value.toString());
      // while (itr.hasMoreTokens()) {
      //   word.set(itr.nextToken());
      //   context.write(word, one);
      // }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}