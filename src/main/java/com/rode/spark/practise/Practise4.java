package com.rode.spark.practise;

import static com.rode.spark.practise.PractiseConstants.CLASS_PATH;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 4.TopN
 *     任务描述： orderid(订单ID),userid(用户ID),payment(支付金额),productid(商品ID)
 *        file1.txt
 *         1,1768,50,155
 *         2,1218,600,211
 *         3,2239,788,242
 *         4,3101,28,599
 *         5,4899,290,129
 *         6,3110,54,1201
 *         7,4436,259,877
 *         8,2369,7890,27
 *        file2.txt
 *         100,4287,226,233
 *         101,6562,489,124
 *         102,1124,33,17
 *         103,3267,159,179
 *         104,4569,57,125
 *         105,1438,37,116
 *   求Top N个payment支付金额值
 *
 *  思路： 将文件内容集合到rdd，提取payment做为key，构造(payment, "orderid,userid,payment,productid")的元组
 *  再按key排序，提取._2, 取前n个
 * @author zhengxinyue
 * @since 2020/10/21
 */
public class Practise4 {
    
    public static void main(String[] args) {
        practise1(10);
    }
    
    private static void practise1(int topN){
        SparkConf sparkConf = new SparkConf().setAppName("practise4").setMaster("local");
        JavaRDD<String> lineRdd1;
        JavaRDD<String> lineRdd2;
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf)) {
        
            lineRdd1 = sc.textFile(CLASS_PATH + "practise/practise4/file1-1.txt");
            lineRdd2 = sc.textFile(CLASS_PATH + "practise/practise4/file1-2.txt");
            JavaRDD<String> lineRddAll = lineRdd1.union(lineRdd2);
    
            JavaPairRDD<Integer, String> pairRDD = lineRddAll.mapToPair(line->
                    new Tuple2<>(Integer.valueOf(line.split(",")[2]), line)
            );
            pairRDD.sortByKey().take(topN).forEach(item-> System.out.println(item._2));
        }
    }
}
