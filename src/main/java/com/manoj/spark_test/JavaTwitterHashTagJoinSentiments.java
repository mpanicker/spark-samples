package com.manoj.spark_test;

/**
 * Created by manoj on 6/21/2017.
 */
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import scala.Tuple2;
import twitter4j.Status;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Displays the most positive hash tags by joining the streaming Twitter data with a static RDD of
 * the AFINN word list (http://neuro.imm.dtu.dk/wiki/AFINN)
 */
public class JavaTwitterHashTagJoinSentiments {

    static Logger logger = Logger.getLogger(JavaTwitterHashTagJoinSentiments.class);

    public static void main(String[] args) throws IOException {
        /*if (args.length < 4) {
            System.err.println("Usage: JavaTwitterHashTagJoinSentiments <consumer key> <consumer secret>" +
                    " <access token> <access token secret> [<filters>]");
            System.exit(1);
        }*/

        //StreamingExamples.setStreamingLogLevels();

        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];
        String[] filters = Arrays.copyOfRange(args, 4, args.length);

        // Set the system properties so that Twitter4j library used by Twitter stream
        // can use them to generate OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", "3oEbEvlpsXADUu1oiHcxXD0xU");
        System.setProperty("twitter4j.oauth.consumerSecret", "W0NYhoIYX6MzFhvZX359oMDOp8gbrP6PKnJkHVOAuDlOy0Tja2");
        System.setProperty("twitter4j.oauth.accessToken", "258405377-M2Qz4dORELvF2LsoPIt4GT8VLCE5gQZuWqwwBPXf");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "g2L0ZN4wFEWGW52DTRtNJQBRbsvJzEIHGMArU3verMmEr");

        SparkConf sparkConf = new SparkConf().setAppName("JavaTwitterHashTagJoinSentiments").setMaster("local[4]");
        JavaStreamingContext jssc = new JavaStreamingContext( sparkConf, new Duration(5000));
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, filters);


        JavaDStream<String> words = stream.map(new Function<Status,String>() {
           public String call(Status s) {
               return s.getText();
           }
        });


        /*JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status, String>() {
            //@Override
            public Iterable<String> call(Status s) {
                return Arrays.asList(s.getText().split(" "));
            }
        });*/

        JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
            //@Override
            public Boolean call(String word) throws Exception {
                return word.contains("trump"); //startsWith("#");
                //return word.startsWith("#WIvIND");
            }
        });

        System.out.println("All TD tweets \n");
        hashTags.print();


        JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(
                new PairFunction<String, String, Integer>() {
                    //                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        // leave out the # character
                        return new Tuple2<String, Integer>(s.substring(1), 1);
                    }
                });
        JavaPairDStream<String, Integer> hashTagTotals = hashTagCount.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    //                  @Override
                    public Integer call(Integer a, Integer b) {
                        return a + b;
                    }
                }, new Duration(60000));

        System.out.println("Count \n");
        hashTagTotals.print();
/*
        // Read in the word-sentiment list and create a static RDD from it
        String wordSentimentFilePath = "data/streaming/AFINN-111.txt";
        final JavaPairRDD<String, Double> wordSentiments = jssc.sparkContext().textFile(wordSentimentFilePath)
                .mapToPair(new PairFunction<String, String, Double>(){
              //      @Override
                    public Tuple2<String, Double> call(String line) {
                        String[] columns = line.split("\t");
                        return new Tuple2<String, Double>(columns[0],
                                Double.parseDouble(columns[1]));
                    }
                });

        JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(
                new PairFunction<String, String, Integer>() {
//                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        // leave out the # character
                        return new Tuple2<String, Integer>(s.substring(1), 1);
                    }
                });

        JavaPairDStream<String, Integer> hashTagTotals = hashTagCount.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
  //                  @Override
                    public Integer call(Integer a, Integer b) {
                        return a + b;
                    }
                }, new Duration(10000));

        // Determine the hash tags with the highest sentiment values by joining the streaming RDD
        // with the static RDD inside the transform() method and then multiplying
        // the frequency of the hash tag by its sentiment value
        JavaPairDStream<String, Tuple2<Double, Integer>> joinedTuples =
                hashTagTotals.transformToPair(new Function<JavaPairRDD<String, Integer>,
                        JavaPairRDD<String, Tuple2<Double, Integer>>>() {
    //                @Override
                    public JavaPairRDD<String, Tuple2<Double, Integer>> call(JavaPairRDD<String,
                            Integer> topicCount)
                            throws Exception {
                        return wordSentiments.join(topicCount);
                    }
                });

        JavaPairDStream<String, Double> topicHappiness = joinedTuples.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Double, Integer>>, String, Double>() {
      //              @Override
                    public Tuple2<String, Double> call(Tuple2<String,
                            Tuple2<Double, Integer>> topicAndTuplePair) throws Exception {
                        Tuple2<Double, Integer> happinessAndCount = topicAndTuplePair._2();
                        return new Tuple2<String, Double>(topicAndTuplePair._1(),
                                happinessAndCount._1() * happinessAndCount._2());
                    }
                });

        JavaPairDStream<Double, String> happinessTopicPairs = topicHappiness.mapToPair(
                new PairFunction<Tuple2<String, Double>, Double, String>() {
        //            @Override
                    public Tuple2<Double, String> call(Tuple2<String, Double> topicHappiness)
                            throws Exception {
                        return new Tuple2<Double, String>(topicHappiness._2(),
                                topicHappiness._1());
                    }
                });

        JavaPairDStream<Double, String> happiest10 = happinessTopicPairs.transformToPair(
                new Function<JavaPairRDD<Double, String>, JavaPairRDD<Double, String>>() {
          //          @Override
                    public JavaPairRDD<Double, String> call(JavaPairRDD<Double,
                            String> happinessAndTopics) throws Exception {
                        return happinessAndTopics.sortByKey(false);
                    }
                }
        );

        // Print hash tags with the most positive sentiment values
        happiest10.foreachRDD(new Function<JavaPairRDD<Double, String>, Void>() {
            //@Override
            public Void call(JavaPairRDD<Double, String> happinessTopicPairs) throws Exception {
                List<Tuple2<Double, String>> topList = happinessTopicPairs.take(10);
                System.out.println(
                        String.format("\nHappiest topics in last 10 seconds (%s total):",
                                happinessTopicPairs.count()));
                for (Tuple2<Double, String> pair : topList) {
                    System.out.println(
                            String.format("%s (%s happiness)", pair._2(), pair._1()));
                }
                return null;
            }
        });
*/

        jssc.start();
        jssc.awaitTermination();
    }
}