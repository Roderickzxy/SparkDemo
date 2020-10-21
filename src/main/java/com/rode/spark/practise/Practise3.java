package com.rode.spark.practise;

import static com.rode.spark.practise.PractiseConstants.CLASS_PATH;

import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.clearspring.analytics.util.Lists;

import scala.Serializable;
import scala.Tuple2;
import scala.math.Ordered;

/**
 * 二次排序
 * 题目：要求先按账户排序，在按金额排序
 * hadoop@apache          200
 * hive@apache            550
 * yarn@apache            580
 * hive@apache            159
 * hadoop@apache          300
 * hive@apache            258
 * hadoop@apache          150
 * yarn@apache            560
 * yarn@apache            260
 * 结果：(hadoop@apache,[150,200,300]),(hive@apache,[159,258,550]),....
 * <p>
 * 思路：
 * 先转为元组，对第一列做排序，再做reducebykey
 */
public class Practise3 {
    public static void main(String[] args) {
        practise1();
//        practise2();
    }
    
    /**
     * 思路：先二次排好序，再相同key聚合出list
     */
    private static void practise1() {
        SparkConf sparkConf = new SparkConf().setAppName("practise3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile(CLASS_PATH + "practise/practise3/file1-1.txt");
        JavaPairRDD<UDFSort, Tuple2<String, Integer>> pairRDD = JavaPairRDD.fromJavaRDD(lines.map(item -> {
            String[] array = item.split(" ");
            return new Tuple2<>(new UDFSort(array[0],
                    Integer.valueOf(array[array.length - 1])),
                    new Tuple2<>(array[0], Integer.valueOf(array[array.length - 1])));
        }));
        JavaPairRDD<String, Integer> sortResult = pairRDD.sortByKey().mapToPair(v1 -> v1._2);
        sortResult.groupByKey().collect().forEach(System.out::println);
    }
    
    /**
     * 思路：先一次排序，再聚合出list，再对list内容做排序
     */
    private static void practise2(){
        SparkConf sparkConf = new SparkConf().setAppName("practise3").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
    
        JavaRDD<String> lines = sc.textFile(CLASS_PATH + "practise/practise3/file1-1.txt");
        JavaPairRDD<String, Integer> pairRdd = JavaPairRDD.fromJavaRDD(lines.map(item->{
            String[] array = item.split(" ");
            return new Tuple2<>(array[0], Integer.valueOf(array[array.length-1]));
        }));
        JavaPairRDD<String, Iterable<Integer>> pairListRdd = pairRdd.sortByKey().groupByKey();
        pairListRdd.mapValues(e -> {
            List<Integer> tmp = Lists.newArrayList(e);
            tmp.sort(Comparator.comparingInt(o -> o));
            return tmp;
        }).collect().forEach(System.out::println);
        
    }
    
    
    /**
     * 自定义排序的类型
     */
    static class UDFSort implements Serializable, Ordered<UDFSort> {
        private String k1;
        private Integer k2;
        
        public UDFSort(String k1, Integer k2) {
            this.k1 = k1;
            this.k2 = k2;
        }
        
        public String getK1() {
            return k1;
        }
        
        public void setK1(String k1) {
            this.k1 = k1;
        }
        
        public Integer getK2() {
            return k2;
        }
        
        public void setK2(Integer k2) {
            this.k2 = k2;
        }
        
        @Override
        public int compare(UDFSort that) {
            if (!k1.equals(that.getK1())) {
                return this.k1.compareTo(that.getK1());
            }
            
            return this.k2.compareTo(that.getK2());
        }
        
        @Override
        public boolean $less(UDFSort that) {
            return compare(that) < 0;
        }
        
        @Override
        public boolean $greater(UDFSort that) {
            return compare(that) > 0;
        }
        
        @Override
        public boolean $less$eq(UDFSort that) {
            return compare(that) <= 0;
        }
        
        @Override
        public boolean $greater$eq(UDFSort that) {
            return compare(that) >= 0;
        }
        
        @Override
        public int compareTo(UDFSort that) {
            return compare(that);
        }
    }

}
