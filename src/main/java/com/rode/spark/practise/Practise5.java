package com.rode.spark.practise;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static com.rode.spark.practise.PractiseConstants.CLASS_PATH;

/**
 * 5.单表关联
 *     题目：求孙子和祖父母的关系列表
 *     数据：   child        parent
 *                 Tom        Lucy
 *                 Tom        Jack
 *                 Jone        Lucy
 *                 Jone        Jack
 *                 Lucy        Mary
 *                 Lucy        Ben
 *                 Jack        Alice
 *                 Jack        Jesse
 *                 Terry        Alice
 *                 Terry        Jesse
 *                 Philip        Terry
 *                 Philip        Alma
 *                 Mark        Terry
 *                 Mark        Alma
 *      输出：    child     grandparent
 *               Tom       Mary
 *               Tom       Ben
 *               Tom       Alice
 *               Tom       Jesse
 *               Jone       Mary
 *               Jone       Ben
 *               Jone       Alice
 *               Jone       Jesse
 */
public class Practise5 {
    public static void main(String[] args) {
        test1();
    }

    /**
     * 思路：
     * 拿到两个数据集，将其中一个反转，变为parent->child，将两个数据集join，得到key->(child, grandparent)
     */
    private static void test1(){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("practise5");
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
            JavaRDD<String> lineRdd = sc.textFile(CLASS_PATH + "practise/practise5/file1-1.txt");
            JavaPairRDD<String, String> childParentPairRdd = lineRdd.mapToPair(v1 -> new Tuple2<>(v1.split(",")[0], v1.split(",")[1]));
            JavaPairRDD<String, String> parentChildpairRdd = lineRdd.
                    mapToPair(v1 -> new Tuple2<>(v1.split(",")[0], v1.split(",")[1])).mapToPair(v-> new Tuple2<>(v._2, v._1));
            JavaPairRDD<String, Tuple2<String, String>> result = parentChildpairRdd.join(childParentPairRdd);
            JavaPairRDD<String, String> value = result.mapToPair(v-> new Tuple2<>(v._2._1, v._2._2));
            value.sortByKey().collect().forEach(System.out::println);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
