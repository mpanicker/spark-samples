package com.manoj.spark_test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 *
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * To run this example:
 *   `$ bin/run-example org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \
 *    zoo03 my-consumer-group topic1,topic2 1`
 */
public class JavaKafkaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	  private JavaKafkaWordCount() {
	  }

	  public static void main(String[] args) {
		  String[] arguments = {"192.168.222.129:2181", "manoj-cg", "testtopic1", "1"};
	    if (args.length < 4) {
	      System.err.println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
	      System.exit(1);
	    } else {

		}

	    //StreamingExamples.setStreamingLogLevels();
	    SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("JavaKafkaWordCount");
	    // Create the context with 2 seconds batch size
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));

	    int numThreads = Integer.parseInt(arguments[3]);
	    Map<String, Integer> topicMap = new HashMap<String, Integer>();
	    String[] topics = arguments[2].split(",");
	    for (String topic: topics) {
	      topicMap.put(topic, numThreads);
	    }

	    JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, arguments[0], arguments[1], topicMap);

	    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
	      //@Override
	      public String call(Tuple2<String, String> tuple2) {
	        return tuple2._2();
	      }
	    });

	    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	      //@Override
	      public List<String> call(String x) {
	        return Arrays.asList(SPACE.split(x));
	      }
	    });

	    System.out.println("words are:"+words.toString());
	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
	      new PairFunction<String, String, Integer>() {
	        //@Override
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<String, Integer>(s, 1);
	        }
	      }).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
	        //@Override
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	      }, new Duration(10000));

	    wordCounts.print();
	    
	    wordCounts.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
	    	public Void call(JavaPairRDD<String, Integer> words) {
	    		for(Tuple2<String, Integer> word:words.collect()) {
	    			System.out.println("word is:"+word._1()+" count is :"+word._2());
	    		}
	    		return null;
	    	}
	    });
	    

	    jssc.start();
	    
	    jssc.awaitTerminationOrTimeout(5000);
	  }
}
