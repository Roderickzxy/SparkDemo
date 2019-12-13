package com.rode.spark.sql;

import org.apache.commons.codec.StringEncoder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * spark sql 处理数据
 */
public class ParseNginxLogMain {

    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("count for nginx")
                .getOrCreate();
        Dataset<Row> df = spark.read().text("/home/roderick/IdeaProjects/SparkDemo/src/main/resources/input/nginx.log");

        Dataset<String> requestDetail = df.map((MapFunction<Row, String>) line->line.toString().split(" ")[5], Encoders.STRING());
        requestDetail.show();
    }
}
