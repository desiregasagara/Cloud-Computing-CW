import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Anagram {

    static Collection<Text> anagramlist = new HashSet<Text>();



    public static class AnagramMakerMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            ArrayList<String> stopwords =new ArrayList<String>(Arrays.asList("'tis","'twas","a","able","about","across","after","ain't","all","almost","also","am","among","an","and","any","are","aren't","as","at","be","because","been","but","by","can","can't","cannot","could","could've","couldn't","dear","did","didn't","do","does","doesn't","don't","either","else","ever","every","for","from","get","got","had","has","hasn't","have","he","he'd","he'll","he's","her","hers","him","his","how","how'd","how'll","how's","however","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","just","least","let","like","likely","may","me","might","might've","mightn't","most","must","must've","mustn't","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","said","say","says","shan't","she","she'd","she'll","she's","should","should've","shouldn't","since","so","some","than","that","that'll","that's","the","their","them","then","there","there's","these","they","they'd","they'll","they're","they've","this","tis","to","too","twas","us","wants","was","wasn't","we","we'd","we'll","we're","were","weren't","what","what'd","what's","when","when","when'd","when'll","when's","where","where'd","where'll","where's","which","while","who","who'd","who'll","who's","whom","why","why'd","why'll","why's","will","with","won't","would","would've","wouldn't","yet","you","you'd","you'll","you're","you've","your"));
            ArrayList<String> listOfWords = new ArrayList<String>();

            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String word = itr.nextToken().replace(',', ' ');
                String wordc = word.toLowerCase();
                if (!stopwords.contains(wordc)) {
                    listOfWords.add(wordc);
                    }
                }
                for (String newword : listOfWords) {
                    char[] arr = newword.toCharArray();
                    Arrays.sort(arr);
                    String wordKey = new String(arr);
                    context.write(new Text(wordKey), new Text(newword));
                }
            }
        }

    public static class Combiner extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<Text> uniques = new HashSet<Text>();
            for (Text value : values) {
                if (uniques.add(value)) {
                    context.write(key, value);
                }
            }
        }
    }



    public static class AnagramAggregatorReducer
            extends Reducer<Text, Text, Text, Text> {
        Text anagramlist=new Text();
        ArrayList<String>anagram = new ArrayList<String>();
        Set<Text> uniques = new HashSet<Text>();
        int size=0;
        ArrayList<ArrayList<String>> arrayList= new ArrayList<ArrayList<String>>();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Collection<Text> anagrams = new HashSet<Text>();

            for (Text val : values) {
                if (uniques.add(val)) {
                    size++;
                }
                anagram.add(val.toString());
                /*Collections.sort(arrayList, new Comparator<ArrayList<String>>() {
                            @Override
                            public int compare(ArrayList<String> o1, ArrayList<String> o2) {
                                return o1.get(0).compareTo(o2.get(0));

                            }
            });
            */
            }
            arrayList.add(anagram);
            Collections.sort(anagram);
            //StringTokenizer newtoken=new StringTokenizer(anagram.toString(),",");
            //String alist = String.join(",",anagram);
           // String alistsort = String.join(",", arrayList);
            StringBuffer sb=new StringBuffer();
            /*for(String s : anagram){
                sb.append(s);
                sb.append("");
            }
            String str =sb.toString();
            */
            for (Object o :arrayList) {
                StringTokenizer newtoken=new StringTokenizer(o.toString(),",");
                String alist = String.join(",",o.toString());
                if (newtoken.countTokens() >= 2) {
                    anagramlist.set(alist);
                    context.write(key, anagramlist);

                }
            }


            // anagrams.add(val);
        }
        @Override
        protected void cleanup(
                Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

                    Collections.sort(arrayList, new Comparator<ArrayList<String>>() {
                        @Override
                        public int compare(ArrayList<String> o1, ArrayList<String> o2) {
                            return o1.get(0).compareTo(o2.get(0));

                        }

                    });

            }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "anagram");
        job.setJarByClass(Anagram.class);
        job.setMapperClass(AnagramMakerMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(AnagramAggregatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}


