
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.TreeSet;
public class CountingWords {
	public static void main(String[] args) throws Exception 
	{
	Job work = new Job(new Configuration());
	Path input=new Path(args[0]);
    Path output=new Path(args[1]);
    work.setJarByClass(CountingWords.class);
	work.setNumReduceTasks(1);
    work.setOutputKeyClass(Text.class);
	work.setOutputValueClass(Text.class);
	work.setMapperClass(CountingWordsMap.class);
	work.setCombinerClass(CountingWordsComb.class);
	work.setReducerClass(CountingWordsRedu.class);
	work.setInputFormatClass(TextInputFormat.class);
	work.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(work, input);
	FileOutputFormat.setOutputPath(work, output);
	work.waitForCompletion(true);
	}
	
public static class CountingWordsMap extends Mapper<LongWritable, Text, Text, Text> {
public void map(LongWritable val, Text typ, Context con) throws IOException, InterruptedException {
			
String line = typ.toString();
String[] words=line.split(" "); 

			//String[] words = typ.toString().split(" ");
for(int z=1;z<words.length;z++){
String temporaryline = words[z-1].toLowerCase()+","+words[z].toLowerCase();
con.write(new Text(temporaryline), new Text("1"));
}
}
}
	
private static class CountingWordsComb extends Reducer<Text, Text, Text, Text> {
public void reduce(Text independent, Iterable<Text> values, Context con) throws IOException, InterruptedException {
int mark = 0;
for (Text value : values) {
mark += Integer.parseInt(value.toString());
}
con.write(independent, new Text(String.valueOf(mark)));
}
}
public static class CountingWordsRedu extends Reducer<Text, Text, Text, Text> {
TreeSet<Pair> resultList = new TreeSet<>();
double sum = 0;
public void reduce(Text val, Iterable<Text> values, Context con) throws IOException, InterruptedException {
int num = 0;
for (Text value : values) {
num += Integer.parseInt(value.toString());
}
if (val.toString().matches(".*\\*")) {
sum = num;
} else {
String[] pair = val.toString().split(",");
resultList.add(new Pair(num/sum, pair[0], pair[1]));
if (resultList.size() > 100) {
resultList.pollFirst();
}
}
}

protected void cleanup(Context con)
throws IOException,
InterruptedException {
while (!resultList.isEmpty()) {
Pair pair = resultList.pollLast();
con.write(new Text(pair.ring), new Text(pair.pop));
}
}

class Pair implements Comparable<Pair> {
double freq;
String ring;
String pop;
Pair(double freq, String ring, String pop) {
this.freq = freq;
this.ring = ring;
this.pop = pop;
}
@Override
public int compareTo(Pair wpair) {
if (this.freq >= wpair.freq) {
return 1;
} else {
return -1;
}
}
}
}
}
