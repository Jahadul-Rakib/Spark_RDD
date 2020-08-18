package com.rakib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class App {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.INFO);

        List<String> inputStream = new ArrayList<String>();
        inputStream.add("r");
        inputStream.add("a");
        inputStream.add("k");
        inputStream.add("i");
        inputStream.add("b");
        inputStream.add("d");

        List<Tuple2<Integer, String>> inputstreamtuple = new ArrayList<>();
        inputstreamtuple.add(new Tuple2<>(1, "Rakib"));
        inputstreamtuple.add(new Tuple2<>(2, "Rakib1"));
        inputstreamtuple.add(new Tuple2<>(3, "Rakib2"));
        inputstreamtuple.add(new Tuple2<>(4, "Rakib3"));
        inputstreamtuple.add(new Tuple2<>(5, "Rakib"));
        inputstreamtuple.add(new Tuple2<>(6, "Rakib3"));
        inputstreamtuple.add(new Tuple2<>(7, "Rakib2"));

        List<Tuple2<Integer, Integer>> inputStreamTuple2 = new ArrayList<>();
        inputStreamTuple2.add(new Tuple2<>(1, 25));
        inputStreamTuple2.add(new Tuple2<>(2, 34));
        inputStreamTuple2.add(new Tuple2<>(3, 14));
        inputStreamTuple2.add(new Tuple2<>(4, 18));

        SparkConf conf = new SparkConf().setAppName("TestApp").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.parallelize(inputStream);
        rdd.collect().forEach(System.out::println);

        JavaPairRDD<Integer, String> pairRDDs = context.parallelizePairs(inputstreamtuple);
        pairRDDs.collect().forEach(System.out::println);

        JavaPairRDD<Integer, Integer> pairRDDs2 = context.parallelizePairs(inputStreamTuple2);
        pairRDDs2.collect().forEach(System.out::println);

        //Inner Join
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = pairRDDs.join(pairRDDs2);
        join.collect().forEach(System.out::println);

        //Left Join
        JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> LeftJoin = pairRDDs.leftOuterJoin(pairRDDs2);
        LeftJoin.collect().forEach(System.out::println);
        //Looping from different Partions
        LeftJoin.foreach(v -> System.out.println(v._2));

        //Right Join
        JavaPairRDD<Integer, Tuple2<Optional<String>, Integer>> rightJoin = pairRDDs.rightOuterJoin(pairRDDs2);
        rightJoin.collect().forEach(System.out::println);

        //full join
        JavaPairRDD<Tuple2<Integer, String>, Tuple2<Integer, Integer>> fulltJoin = pairRDDs.cartesian(pairRDDs2);
        fulltJoin.collect().forEach(System.out::println);

        context.close();
    }
}
