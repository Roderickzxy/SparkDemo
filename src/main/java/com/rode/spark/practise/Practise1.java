package com.rode.spark.practise;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 题目1：
 * 排序：定义三个文件，对所有文件里的数字进行排序
 *
 * 思路：读取三个文件的内容到RDD，合并RDD，对RDD排序
 * @author zhengxinyue
 * @since 2020/10/17
 */
public class Practise1 {
    public static void main(String[] args) {
        practise2();
    }
    
    /**
     * 文件里每一行都是数字
     */
    private static void practise1(){
        SparkConf conf = new SparkConf().setAppName("practise1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines1 = sc.textFile("C:\\workspace-mine\\SparkDemo\\src\\main\\resources\\practise\\practise1\\file1-1.txt");
        JavaRDD<String> lines2 = sc.textFile("C:\\workspace-mine\\SparkDemo\\src\\main\\resources\\practise\\practise1\\file2-1.txt");
        JavaRDD<String> lines3 = sc.textFile("C:\\workspace-mine\\SparkDemo\\src\\main\\resources\\practise\\practise1\\file3-1.txt");
        JavaRDD<String> linesAll = lines1.union(lines2).union(lines3);
    
        //假如是想按首字母排序，不需要做Integer::valueOf的类型转换
        JavaRDD<Integer> numbers = linesAll.map(Integer::valueOf).sortBy(x->x, true, 1).repartition(1);
        numbers.collect().forEach(System.out::println);
    }
    
    /**
     * 文件每一行都是{name} {number}的形式
     */
    private static void practise2(){
        SparkConf conf = new SparkConf().setAppName("practise1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines1 = sc.textFile("C:\\workspace-mine\\SparkDemo\\src\\main\\resources\\practise\\practise1\\file1-2.txt");
        JavaRDD<String> lines2 = sc.textFile("C:\\workspace-mine\\SparkDemo\\src\\main\\resources\\practise\\practise1\\file2-2.txt");
        JavaRDD<String> lines3 = sc.textFile("C:\\workspace-mine\\SparkDemo\\src\\main\\resources\\practise\\practise1\\file3-2.txt");
        JavaRDD<String> linesAll = lines1.union(lines2).union(lines3);
    
        JavaRDD<Integer> numbers = linesAll.map(item->item.trim().split(" ")[1]).map(Integer::valueOf).sortBy(x->x, true, 1).repartition(1);
        numbers.collect().forEach(System.out::println);
    }
}
