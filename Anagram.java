import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
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
public class Anagram {

        static Collection<Text> anagrams = new HashSet<Text>();

        public static class AnagramMakerMapper
                extends Mapper<Object, Text, Text, Text> {

            public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString());

                while (itr.hasMoreTokens()) {
                    String word = itr.nextToken().replace(',', ' ');
                    char[] arr = word.toCharArray();
                    Arrays.sort(arr);
                    String wordKey = new String(arr);
                    context.write(new Text(wordKey), new Text(word));
                }
            }
        }

        public static class AnagramAggregatorReducer
                extends Reducer<Text, Text, Text, Text> {
            public void reduce(Text key, Iterable<Text> values,
                               Context context
            ) throws IOException, InterruptedException {
                // Collection<Text> anagrams = new HashSet<Text>();
                Text anagramlist=new Text();
                String anagram = "";

                for (Text val : values) {
                        anagram = anagram + '~' + val.toString();
                    }
                StringTokenizer newtoken=new StringTokenizer(anagram,"~");
                if(newtoken.countTokens()>=2) {
                    anagram = anagram.replace("~", ",");
                    anagramlist.set(anagram);
                    context.write(key,anagramlist);
                }

                // anagrams.add(val);
                }
            }

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "anagram");
            job.setJarByClass(Anagram.class);
            job.setMapperClass(AnagramMakerMapper.class);
            job.setReducerClass(AnagramAggregatorReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

    }

