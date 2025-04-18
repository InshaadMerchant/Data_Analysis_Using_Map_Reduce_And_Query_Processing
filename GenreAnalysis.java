import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenreAnalysis {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text genreAndPeriod = new Text();
        private static int counter = 0;

        // Function to check the genre of the movie
        public static String checkGenre(String[] genres) {
            if (genres == null || genres.length == 0) return null;
            
            List<String> genreList = Arrays.asList(genres);
            if (genreList.contains("Action") && genreList.contains("Thriller")) {
                return "Action;Thriller";
            } else if (genreList.contains("Adventure") && genreList.contains("Drama")) {
                return "Adventure;Drama";
            } else if (genreList.contains("Comedy") && genreList.contains("Romance")) {
                return "Comedy;Romance";
            }
            return null;
        }

        // Function to check the Year of the movie
        public static String checkPeriod(int year) {
            if (year >= 1991 && year <= 2000) {
                return "[1991-2000]";
            } else if (year >= 2001 && year <= 2010) {
                return "[2001-2010]";
            } else if (year >= 2011 && year <= 2020) {
                return "[2011-2020]";
            }
            return null;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            
            // Print first 5 lines for debugging
            if (counter < 5) {
                System.err.println("DEBUG - Processing line: " + line);
                counter++;
            }

            if (line.isEmpty()) {
                System.err.println("DEBUG - Empty line skipped");
                return;
            }

            String[] parts = line.split(";");
            if (parts.length < 6) {
                System.err.println("DEBUG - Invalid line format (length=" + parts.length + "): " + line);
                return;
            }

            try {
                String type = parts[1].trim();
                String ratingStr = parts[4].trim();
                String yearStr = parts[3].trim();
                String genresStr = parts[5].trim();

                if (counter <= 5) {
                    System.err.println("DEBUG - Type: " + type);
                    System.err.println("DEBUG - Rating: " + ratingStr);
                    System.err.println("DEBUG - Year: " + yearStr);
                    System.err.println("DEBUG - Genres: " + genresStr);
                }

                // Check if it's a movie with high rating
                if (type.equals("movie")) {
                    double rating = Double.parseDouble(ratingStr);
                    if (rating >= 7.5) {
                        int year = Integer.parseInt(yearStr);
                        String[] genres = genresStr.split(",");
                        
                        String period = checkPeriod(year);
                        String genre = checkGenre(genres);

                        if (counter <= 5) {
                            System.err.println("DEBUG - Period: " + period);
                            System.err.println("DEBUG - Genre combination: " + genre);
                        }

                        if (period != null && genre != null) {
                            genreAndPeriod.set(period + "," + genre);
                            context.write(genreAndPeriod, one);
                            if (counter <= 5) {
                                System.err.println("DEBUG - Emitting: " + genreAndPeriod.toString() + " -> 1");
                            }
                        }
                    }
                }
            } catch (NumberFormatException e) {
                System.err.println("DEBUG - Number format exception in line: " + line);
                e.printStackTrace();
            } catch (Exception e) {
                System.err.println("DEBUG - Unexpected error processing line: " + line);
                e.printStackTrace();
            }
        }
    }

    public static class MovieReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private static int counter = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            
            // Print first 5 keys for debugging
            if (counter < 5) {
                System.err.println("DEBUG - Reducing key: " + key.toString());
                counter++;
            }
            
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
            
            if (counter <= 5) {
                System.err.println("DEBUG - Output: " + key.toString() + " -> " + sum);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.err.println("DEBUG - Starting GenreAnalysis job");
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Movie count for specific genre and decade");
        job.setJarByClass(GenreAnalysis.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(MovieReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.err.println("DEBUG - Job configuration complete");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
} 