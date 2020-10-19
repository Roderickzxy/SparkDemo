package com.rode.spark.practise;

/**
 * 二次排序
 *     题目：要求先按账户排序，在按金额排序
 *     hadoop@apache          200
 *     hive@apache            550
 *     yarn@apache            580
 *     hive@apache            159
 *     hadoop@apache          300
 *     hive@apache            258
 *     hadoop@apache          150
 *     yarn@apache            560
 *     yarn@apache            260
 *       结果：(hadoop@apache,[150,200,300]),(hive@apache,[159,258,550]),....
 *
 *  思路：
 *  先转为元组，对第一列做排序，再做reducebykey
 */
public class Practise3 {
}
