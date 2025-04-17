import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class GenreAnalysis {
    public static class MovieMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text result = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            
            // Expected format: tconst, titleType, primaryTitle, startYear, genres, averageRating, numVotes
            if (fields.length >= 7 && fields[1].equals("movie")) {
                try {
                    int year = Integer.parseInt(fields[3]);
                    double rating = Double.parseDouble(fields[5]);
                    String[] genres = fields[4].split(",");
                    Set<String> genreSet = new HashSet<>(Arrays.asList(genres));

                    // Check if rating is >= 7.0
                    if (rating >= 7.0) {
                        String period = "";
                        if (year >= 1991 && year <= 2000) period = "1991-2000";
                        else if (year >= 2001 && year <= 2010) period = "2001-2010";
                        else if (year >= 2011 && year <= 2020) period = "2011-2020";
                        else return;

                        // Check for genre combinations
                        if (genreSet.contains("Action") && genreSet.contains("Thriller")) {
                            result.set(period + ",Action;Thriller");
                            context.write(result, one);
                        }
                        if (genreSet.contains("Adventure") && genreSet.contains("Drama")) {
                            result.set(period + ",Adventure;Drama");
                            context.write(result, one);
                        }
                        if (genreSet.contains("Comedy") && genreSet.contains("Romance")) {
                            result.set(period + ",Comedy;Romance");
                            context.write(result, one);
                        }
                    }
                } catch (NumberFormatException e) {
                    // Skip malformed entries
                }
            }
        }
    }

    public static class MovieReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
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
        Job job = Job.getInstance(conf, "genre analysis");
        job.setJarByClass(GenreAnalysis.class);
        job.setMapperClass(MovieMapper.class);
        job.setCombinerClass(MovieReducer.class);
        job.setReducerClass(MovieReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
} 