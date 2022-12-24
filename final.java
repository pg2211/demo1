// mapper.java

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCounterMap extends Mapper <LongWritable,Text,Text,IntWritable>
{

	protected void map(LongWritable key, Text value,Context context)
	throws IOException, InterruptedException
	{
		String [] words=value.toString().split(",");
		for(String word:words)
		context.write(new Text(word),new IntWritable(1))
		
	}


}

// reduce.java

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcounterRed extends Reducer <Text,IntWritable,Text,LongWritable>
{
	protected void red(Text word, Iterable<IntWritable> values,Iterable<LongWriotable> keys,Context context)
	throws IOExcedption, InterruptedException
	{
		Integer count=0;
		for(IntWritable val: values)
		{
			count+=val.get();	
		}
		for(LongWritable key: keys)
		{
			count+=key.get();
		}
			context.write(word,new IntWritable(count),Longwritable(count));
}
}


	}


}
