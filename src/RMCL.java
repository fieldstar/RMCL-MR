
//hadoop
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
//java 
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
//import java.text.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

class FloatArrayWritable implements Writable{

	private float[] data;
	
	public FloatArrayWritable(){
		data = null;
	}
	
	public float[] get(){
		return this.data;
	}
	public void set(float[] d){
		this.data = d;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		//Deserialize the fields of this object from in.
		int len = in.readInt();
		this.data = new float[len];
		for(int i = 0;i < len;i++){
			data[i] = in.readFloat();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//Serialize the fields of this object to out.
		int len = 0;
		if(data != null){
			len = data.length;
		}
		out.writeInt(len);
		
		for(int i = 0;i < len;i++){
			out.writeFloat(data[i]);
		}
	}
	public String toString(){
		StringBuilder sb = new StringBuilder();
		int i;
		for(i = 0;i < data.length;i++){
			sb.append(data[i]).append(" ");
		}
		if(sb.length() > 0){
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.toString();
	}
}

public class RMCL extends Configured implements Tool {

	protected static enum rmclCounter{
		CONVERGE_CHECK
	}
	
	public static class EdgeToAdjListMapper extends Mapper<LongWritable,Text,IntWritable,Text> {
		private IntWritable outputKey = new IntWritable();
		private Text outputValue = new Text();

		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("#################### EdgeToAdjListMapper ####################");
		}
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			if(value.toString().startsWith("#"))
				return;
			String[] valueArr = value.toString().split("\t");
			
			outputKey.set(Integer.parseInt(valueArr[0]));
			outputValue.set(valueArr[1]);
			context.write(outputKey, outputValue);
			
			outputKey.set(Integer.parseInt(valueArr[1]));
			outputValue.set(valueArr[0]);
			context.write(outputKey, outputValue);		//make it undirected
		}
	}
	public static class EdgeToAdjListCombiner extends Reducer<IntWritable,Text,IntWritable,Text>{
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### EdgeToAdjListCombiner ####################");
		}
		protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuilder sb = new StringBuilder();
			for(Text v:values)
				sb.append(v.toString()).append(" ");
			
			if(sb.length() > 1){
				sb.deleteCharAt(sb.length() - 1);
				outputValue.set(sb.toString());
				context.write(key, outputValue);
			}
		}
	}
	public static class EdgeToAdjListReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### EdgeToAdjListReducer ####################");
		}
		protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuilder sb = new StringBuilder();
			for(Text v:values)
				sb.append(v.toString()).append(" ");
			
			if(sb.length() > 1){
				sb.deleteCharAt(sb.length() - 1);
				outputValue.set(sb.toString());
				context.write(key, outputValue);
			}
		}
	}
	
	public static class MapStage1 extends Mapper<LongWritable,Text,IntWritable,Text> {
		
		private IntWritable outputKey = new IntWritable();
		private Text outputValue = new Text();
		//private DecimalFormat df = new DecimalFormat("#.######");

		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("#################### Map Stage 1 ####################");
		}
		
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
			int i;
			float sum = 0;
			
			if(value.toString().startsWith("#"))
				return;
			
			String[] keyValue = value.toString().split("\t");
			String[] valArr = keyValue[1].split(" ");
			StringBuilder sb = new StringBuilder();
//			StringBuffer adjList = new StringBuffer();
			
			sum = valArr.length + 1;		// + 1 to take self loop into account.
				
			//sb.append(keyValue[0]).append(" ").append(df.format((1.0 / sum)));
			sb.append(keyValue[0]).append(" ").append((1.0 / sum));
				
			for(i = 0;i < valArr.length; i++){ 
				//sb.append(" ").append(valArr[i]).append(" ").append(df.format(1.0 / sum));
				sb.append(" ").append(valArr[i]).append(" ").append((1.0 / sum));
			}
				
//			adjList.append(keyValue[0]).append(" ").append(keyValue[1]);		// for self loops
			
			sb.append(":").append(keyValue[1]).append(":").append(sum);			
			
			outputKey.set(Integer.parseInt(keyValue[0]));
			outputValue.set(sb.toString());
			context.write(outputKey,outputValue);
		}
	}
/*	
	public static class ReduceStage1 extends Reducer<Text,Text,Text,Text> {
		protected void setup(Context context) throws IOException, InterruptedException { 
			System.out.println("#################### Reduce Stage 1 ####################");
		}
		// identity reduce method
	}
*/
	public static class MapStage2 extends Mapper<Text,Text,IntWritable,FloatArrayWritable>{
		
		private IntWritable outputKey = new IntWritable();
//		private DecimalFormat df = new DecimalFormat("#.######");
		
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("#################### Map Stage 2 ####################");
		}
		
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException {
			
			int i,j;
			String[] valArr = value.toString().split(":");
			String[] adjList = valArr[1].split(" ");
			String[] columnArr = valArr[0].split(" ");
			
			float[] column = new float[columnArr.length];
			
			for(i = 0;i < columnArr.length;i++)
				column[i] = Float.parseFloat(columnArr[i]);
			
			//encoding: -o,len_column,column,len_adjList,adjList,sum
			//start of column:2,end of column:2 + colAdjListSum[1] - 1		//including these values
			//start of adjList:2 + colAdjListSum[1] + 1,end of column:colAdjListSum.length - 2	//including these values
			//sum = colAdjListSum[colAdjListSum.length -1]
			
			float[] colAdjListSum = new float[column.length + adjList.length + 4];
			
			colAdjListSum[0] = -(float)'o';	//-111.0		//marker value:-o indicating old column
			
			colAdjListSum[1] = column.length;		//length of column
			
			i = 2;
			for(j = 0;j < column.length;j++)
				colAdjListSum[i++] = column[j];
			
			colAdjListSum[i++] = adjList.length;		//length of adjList
			
			for(j = 0;j < adjList.length;j++)
				colAdjListSum[i++] = Float.parseFloat(adjList[j]);
			
			colAdjListSum[i] = Float.parseFloat(valArr[2]);		//sum
			
			FloatArrayWritable wcol = new FloatArrayWritable();
			wcol.set(column);
			
			FloatArrayWritable wcolAdjListSum = new FloatArrayWritable();
			wcolAdjListSum.set(colAdjListSum);
			
			for(i = 2 + (int)colAdjListSum[1] + 1;i <= colAdjListSum.length - 2;i++){
				outputKey.set((int)colAdjListSum[i]);
				context.write(outputKey,wcol);
//				context.write(new Text(adjList[i]),new Text(valArr[0]));
			}
			
			
			outputKey.set(Integer.parseInt(key.toString()));
			context.write(outputKey,wcolAdjListSum);
//			context.write(new Text(adjList[0]),new Text(keyVal[1]));
		}
	}
	public static class CombineStage2 extends Reducer<IntWritable,FloatArrayWritable,IntWritable,FloatArrayWritable>{
		
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("#################### Combine Stage 2 ####################");
//			Configuration conf = context.getConfiguration();
//			NUM_NODES = Integer.parseInt(conf.get("NUM_NODES"));
		}
		protected void reduce(IntWritable key,Iterable<FloatArrayWritable> values,Context context) throws IOException, InterruptedException{
			
			int i,index;
			float[] pColumn;
//			StringBuffer sb = new StringBuffer();
			HashMap<Integer,Float> column = new HashMap<Integer,Float>();
			
			for(FloatArrayWritable val:values){
				pColumn = val.get();
				if(pColumn.length == 0)
					continue;
//				String tmp = val.toString();
				if(pColumn[0] == -(float)'o')
					context.write(key, val);
				else{
//					String[] tmpArr = tmp.split(" ");
					for(i = 0;i < pColumn.length;i = i + 2){
						index = (int)pColumn[i];
						if(column.containsKey(index)){
							column.put(index,column.get(index) + pColumn[i+1]);
						}
						else{
							column.put(index,pColumn[i+1]);
						}
					}
				}
			}
			
			pColumn = new float[column.size() * 2];
			
			Set<Integer> keys = column.keySet();
			Iterator<Integer> itr = keys.iterator();
			i = 0;
			while(itr.hasNext()){
				index = itr.next();
				pColumn[i++] = (float)index;
				pColumn[i++] = column.get(index);
//				sb.append(" ").append(index).append(" ").append(column.get(index));
			}
			
			FloatArrayWritable wpColumn = new FloatArrayWritable();
			wpColumn.set(pColumn);
			context.write(key,wpColumn);
		} 
	}
	public static class ReduceStage2 extends Reducer<IntWritable,FloatArrayWritable,IntWritable,Text> {
//		private int NUM_NODES;
		private float INFLATE_CONSTANT;
		private float MLRMCL_PRUNE_A;
		private float MLRMCL_PRUNE_B;
		private IntWritable mc = new IntWritable(-(int)'c');
		private Text outputValue = new Text();
		//private DecimalFormat df = new DecimalFormat("#.######");
		
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("#################### Reduce Stage 2 ####################");
			Configuration conf = context.getConfiguration();
//			NUM_NODES = Integer.parseInt(conf.get("NUM_NODES"));
			
			INFLATE_CONSTANT = conf.getFloat("INFLATE_CONSTANT",INFLATE_CONSTANT);
			System.out.println("INFLATE_CONSTANT:" + INFLATE_CONSTANT);
			
			MLRMCL_PRUNE_A = conf.getFloat("MLRMCL_PRUNE_A",MLRMCL_PRUNE_A);
			System.out.println("MLRMCL_PRUNE_A:" + MLRMCL_PRUNE_A);
			
			MLRMCL_PRUNE_B = conf.getFloat("MLRMCL_PRUNE_B",MLRMCL_PRUNE_B);
			System.out.println("MLRMCL_PRUNE_B:" + MLRMCL_PRUNE_B);
		}
		
		protected void reduce(IntWritable key,Iterable<FloatArrayWritable> values,Context context) throws IOException, InterruptedException {

			int i,index;
			float sum1 = 0,val;
			
			HashMap<Integer,Float> newColumn = new HashMap<Integer,Float>();
			HashMap<Integer,Float> oldColumn = new HashMap<Integer,Float>();
			
			StringBuilder adjList = new StringBuilder();
			float[] tmp;
			
			for(FloatArrayWritable txt:values){
				tmp = txt.get();
				if(tmp.length == 0)
					continue;
				if(tmp[0] == -(float)'o'){
					
					for(i = 2;i < 2 + (int)tmp[1];i = i + 2){
						oldColumn.put((int)tmp[i],tmp[i+1]);
						if(newColumn.containsKey((int)tmp[i]))
							newColumn.put((int)tmp[i],newColumn.get((int)tmp[i]) + tmp[i+1]);
						else
							newColumn.put((int)tmp[i], tmp[i+1]);
					}

					for(i = 3 + (int)tmp[1];i < tmp.length - 1;i++){
						adjList.append(tmp[i]).append(" ");
					}
					
					adjList.deleteCharAt(adjList.length() - 1);
					
					sum1 = tmp[tmp.length - 1];
				}
				else {
					for(i = 0;i < tmp.length;i = i + 2){
						index = (int)tmp[i];
						if(newColumn.containsKey(index)){
							newColumn.put(index,newColumn.get(index) + tmp[i+1]);
						}
						else{
							newColumn.put(index,tmp[i+1]);
						}
					}
				}
			}
			
			float sum2 = 0;
			float max = 0;
			int nnz = 0;
			
		    Set<Integer> keys = newColumn.keySet();
		    Iterator<Integer> itr = keys.iterator();
		    
		    while(itr.hasNext()){
		    	index = itr.next();
		    	val = (float)Math.pow(newColumn.get(index)/sum1,INFLATE_CONSTANT);
		    	if(val > 1e-6){
		    		newColumn.put(index, val);
		    		sum2 += val;
		    		nnz++;
		    		if(val > max){
		    			max = val;
		    		}
		    	}
		    	else{
		    		newColumn.put(index,(float)0.0);
		    		//newColumn.remove(index);
		    	}
		    }
		    
		    newColumn.values().removeAll(Collections.singleton((float)0.0));
		    
		    float threshold = computeThreshold(sum2 / nnz, max);
		    sum2 = 0;
		    
		    keys = newColumn.keySet();
		    itr = keys.iterator();
		    
		    while(itr.hasNext()){
		    	index = itr.next();
		    	val = newColumn.get(index);
		    	if(val > threshold){
		    		sum2 += val;
		    	}
		    	else{
		    		newColumn.put(index,(float)0.0);
		    		//newColumn.remove(index);
		    	}
		    }
		    
		    newColumn.values().removeAll(Collections.singleton((float)0.0));
		    
		    keys = newColumn.keySet();
		    itr = keys.iterator();

		    while(itr.hasNext()){
		    	index = itr.next();
		    	newColumn.put(index,newColumn.get(index) / sum2);
		    }
		    
		    float change = 0;

		    //go through newColumn
		    keys = newColumn.keySet();
		    itr = keys.iterator();
		    
		    while(itr.hasNext()){
		    	index = itr.next();
		    	if(oldColumn.containsKey(index)){
		    		change += Math.pow(newColumn.get(index) - oldColumn.get(index) , 2);
		    		oldColumn.put(index,(float)0.0);
		    	}
		    	else{
		    		change += Math.pow(newColumn.get(index), 2);
		    	}
		    }
		    
		    //go through oldColumn
		    keys = oldColumn.keySet();
		    itr = keys.iterator();
		    
		    while(itr.hasNext()){
		    	index = itr.next();
		    	change += Math.pow(oldColumn.get(index), 2);
		    }
		    
		    StringBuilder sb = new StringBuilder();
		    
		    keys = newColumn.keySet();
		    itr = keys.iterator();
		    
		    while(itr.hasNext()){
		    	index = itr.next();
		    	sb.append(index).append(" ").append(newColumn.get(index)).append(" ");
		    }
		    	
		    if(sb.length() > 0){
		    	sb.deleteCharAt(sb.length() - 1);
		    }
		    sb.append(":").append(adjList).append(":").append(sum1);
		    
		    if(sb.length() > 0){
		    	outputValue.set(sb.toString());
		    	context.write(key,outputValue);
//		    	context.write(key,new Text(sb2.toString()));
		    }
		    
		    //outputValue.set(df.format(change));
		    outputValue.set(Float.toString(change));
//		    outputValue.set(Double.toString(change));
		    
		    if(change > 0.0)
		    	context.write(mc, outputValue);
//		    context.write(ZERO,new Text(Double.toString(change)));
		}
		
		private float computeThreshold(float avg, float max){
		    float ret = MLRMCL_PRUNE_A * avg * (1-MLRMCL_PRUNE_B * (max-avg));
		    ret = (ret > max) ? max : ret;
		    return Math.abs(ret);
		} 
	}
	
	public static class MapStage3 extends Mapper<Text,Text,IntWritable,Text> {
		private IntWritable outputKey = new IntWritable();
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("#################### Map Stage 3 ####################");
		}
		// identity mapper
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			outputKey.set(Integer.parseInt(key.toString()));
			outputValue.set(value);
			context.write(outputKey, outputValue);
		}
	}
	public static class CombineStage3 extends Reducer<IntWritable,Text,IntWritable,Text>{
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("#################### Combine Stage 3 ####################");
		}
		protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			float change = 0;
			
			if(key.get() == -(int)'c'){
				for(Text val:values){
					change += Float.parseFloat(val.toString());
				}
				context.write(key,new Text(Float.toString(change)));
			}
			else{
				for(Text val:values){
					context.write(key,val);
				}
			}
	}
}
	public static class ReduceStage3 extends Reducer<IntWritable,Text,IntWritable,Text> {
		
		private float CONVERGE_THRESHOLD;
		private int NUM_NODES;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("#################### Reduce Stage 3 ####################");
			Configuration conf = context.getConfiguration();
			
			CONVERGE_THRESHOLD = conf.getFloat("CONVERGE_THRESHOLD", CONVERGE_THRESHOLD);
			System.out.println("CONVERGE_THRESHOLD:" + CONVERGE_THRESHOLD);
			
			NUM_NODES = conf.getInt("NUM_NODES", NUM_NODES); 
			System.out.println("NUM_NODES:" + NUM_NODES);
		}
		protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			float change = 0;
			
			if(key.get() == -(int)'c'){
				for(Text val:values){
					change += Float.parseFloat(val.toString());
				}
				change = (float) (Math.sqrt(change) / NUM_NODES);
				System.out.println("From Reduce Stage 3,Total Change:" + change);
				if(change > CONVERGE_THRESHOLD)
					context.getCounter(rmclCounter.CONVERGE_CHECK).increment(1);
			}
			else{
				for(Text val:values){
					context.write(key,val);
				}
			}
		}
	}
	
	public static class MapStage4 extends Mapper<Text,Text,IntWritable,IntWritable>{
		private IntWritable outputKey = new IntWritable();
		private IntWritable outputValue = new IntWritable();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Map Stage 4 ####################");
		}
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			int i;
			int maxIndex = -1;
			float maxValue = 0;
			float tmp;
			String[] columnArr = value.toString().split(":")[0].split(" ");
			for(i = 0;i < columnArr.length;i = i + 2){
				tmp = Float.parseFloat(columnArr[i + 1]); 
				if(tmp > maxValue){
					maxValue = tmp;
					maxIndex = Integer.parseInt(columnArr[i]); 
				}
			}
			
			outputKey.set(Integer.parseInt(key.toString()));
			outputValue.set(maxIndex);
			context.write(outputKey, outputValue);
		}
	}
	
	//identity reducer
	public static class ReduceStage4 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		//private IntWritable outputKey = new IntWritable();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Reduce Stage 4 ####################");
		}
		protected void reduce(IntWritable key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException{
			for(IntWritable index : values){
				context.write(key, index);
			}
		}
	}
	
	//command line arguments
	
	protected int NUM_NODES;
	protected float INFLATE_CONSTANT;
    protected float MLRMCL_PRUNE_A;
    protected float MLRMCL_PRUNE_B;
    protected int NUM_REDUCERS;
    protected float CONVERGE_THRESHOLD;
    protected int MAX_ITER = 25;
	protected Path inputPath;
	protected Path tmpPath = new Path("tmp");
	protected Path outputPath;
	
	/*
	 * Total of 8 command line parameter. INPUT_TYPE takes one of the two values:e or a
   	usage:RMCLU inputPath outputPath NUM_NODES INFLATE_CONSTANT NUM_REDUCERS CONVERGE_THRESHOLD MAX_ITER  
	*/
	public static void main(String[] args) throws Exception{
		int ret = ToolRunner.run(new Configuration(),new RMCL(),args);
		System.exit(ret);
	}
	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length !=7){
			System.out.println("usage:RMCL inputPath outputPath NUM_NODES INFLATE_CONSTANT " +
					"NUM_REDUCERS CONVERGE_THRESHOLD MAX_ITER");
			return -1;
		}
		
		int i;
		inputPath = new Path(args[0]);
		outputPath = new Path(args[1]);
		NUM_NODES = Integer.parseInt(args[2]);
		INFLATE_CONSTANT = Float.parseFloat(args[3]);
		MLRMCL_PRUNE_A = (float) 0.9;
		MLRMCL_PRUNE_B = 2;
		NUM_REDUCERS = Integer.parseInt(args[4]);
		CONVERGE_THRESHOLD = Float.parseFloat(args[5]); 
		MAX_ITER = Integer.parseInt(args[6]);
		
		
		FileSystem fs = FileSystem.get(getConf());
		
		//convert edges to Adj lists
		System.out.println("Converting Edge file to AdjList file . . .");
		EdgeToAdjListConfig().waitForCompletion(true);
		fs.delete(inputPath, true);
		fs.rename(outputPath, inputPath);
		
		//first stage of map reduce
		Job job1 = configStage1();
		job1.waitForCompletion(true);
		fs.delete(inputPath,true);
		fs.rename(outputPath,inputPath);
		
		long startTime = System.currentTimeMillis();
		
		// iterative stages of map reduce
		for(i = 0;i < MAX_ITER;i++){
			Job job2 = configStage2();
			job2.waitForCompletion(true);
			
			Job job3 = configStage3();
			job3.waitForCompletion(true);
			
			Counters counters = job3.getCounters();
			Counter counter = counters.findCounter(rmclCounter.CONVERGE_CHECK);
			long change = counter.getValue();
			
			if(change == 0){
				fs.delete(inputPath,true);
				fs.delete(tmpPath,true);
				fs.rename(outputPath,inputPath);
				break;
			}
			
			fs.delete(inputPath,true);
			fs.delete(tmpPath,true);
			fs.rename(outputPath,inputPath);
			System.out.println("End of Iteration:" + (i + 1));
		}
		
		long endTime = System.currentTimeMillis();
		
		System.out.println("Finding final clusters . . .");
		Job job4 = configStage4();
		job4.waitForCompletion(true);
		
		//fs.delete(inputPath,true);
		//fs.rename(outputPath, inputPath);
		
		System.out.println("#################### RMCL TOTAL TIME ####################");
		System.out.println((double)(endTime - startTime) / 1000 + " Seconds");
		System.out.println("#################### RMCL TOTAL TIME ####################");
	
		return 0;
	}
	
	public Job EdgeToAdjListConfig() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"EDGE_TO_ADJLIST_STAGE");
		job.setJarByClass(RMCL.class);
		job.setMapperClass(EdgeToAdjListMapper.class);
		job.setCombinerClass(EdgeToAdjListCombiner.class);
		job.setReducerClass(EdgeToAdjListReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
	public Job configStage1() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"RMCL_STAGE1");
		job.setJarByClass(RMCL.class);
		job.setMapperClass(MapStage1.class);
//		job.setReducerClass(ReduceStage1.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job,inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		return job;
	}
	
	public Job configStage2() throws Exception{
		Configuration conf = getConf();
		conf.setFloat("INFLATE_CONSTANT", INFLATE_CONSTANT);
		conf.setFloat("MLRMCL_PRUNE_A", MLRMCL_PRUNE_A);
		conf.setFloat("MLRMCL_PRUNE_B", MLRMCL_PRUNE_B);
		Job job = new Job(conf,"RMCL_STAGE2");
		job.setJarByClass(RMCL.class);
		job.setMapperClass(MapStage2.class);
		job.setCombinerClass(CombineStage2.class);
		job.setReducerClass(ReduceStage2.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		//job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatArrayWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job,inputPath);
		FileOutputFormat.setOutputPath(job,tmpPath);
		return job;
	}
	
	public Job configStage3() throws Exception{
		Configuration conf = getConf();
		conf.setFloat("CONVERGE_THRESHOLD", CONVERGE_THRESHOLD);
		conf.setInt("NUM_NODES", NUM_NODES);
		Job job = new Job(conf,"RMCL_STAGE3");
		job.setJarByClass(RMCL.class);
		job.setMapperClass(MapStage3.class);
		job.setCombinerClass(CombineStage3.class);
		job.setReducerClass(ReduceStage3.class);
		job.setNumReduceTasks(NUM_REDUCERS);
//		job.setNumReduceTasks(0);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job,tmpPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		return job;
	}
	
	public Job configStage4() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"RMCL_STAGE4");
		job.setJarByClass(RMCL.class);
		job.setMapperClass(MapStage4.class);
		job.setReducerClass(ReduceStage4.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
//		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);		
		FileInputFormat.addInputPath(job,inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		return job;
	}
}