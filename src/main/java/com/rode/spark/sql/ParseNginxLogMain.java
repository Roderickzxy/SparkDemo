package com.rode.spark.sql;

import org.apache.commons.codec.StringEncoder;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * spark sql 处理数据
 *
 * 目的：统计nginx日志里每个url请求的次数
 */
public class ParseNginxLogMain {

    private static final String P_COMM = ".* .* .* \\[.*\\] \"(.*)\" \\d+ \\d+ \".*\" \".*\"";

    // 192.168.201.120 - - [20/Aug/2019:21:16:52 +0800] "GET /ticket/first?email=zengzhenshuo@corp.polyv.net HTTP/1.1" 200 101 "-" "Apache-HttpClient/4.5.2 (Java/1.8.0_77)"
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("count for nginx")
                .getOrCreate();
        Dataset<Row> df = spark.read().text("/home/roderick/IdeaProjects/SparkDemo/src/main/resources/input/nginx.log");

        Dataset<String> requestStr = df.map((MapFunction<Row, String>) line-> {

            String request = line.toString();
            Pattern pattern = Pattern.compile(P_COMM);
            Matcher matcher = pattern.matcher(request);
            matcher.find();//必须要有这句
            return matcher.group(1);
        }, Encoders.STRING());

        Dataset<RequestDetail> requestDetail = requestStr.map((MapFunction<String, RequestDetail>) line-> {
            String[] lineArray = line.split(" ");
            return new RequestDetail(lineArray[0],lineArray[1],lineArray[2]);
        }, Encoders.bean(RequestDetail.class));

        // 使用dataset的方法操作结构化数据
        Dataset<Row> requestCount = requestDetail.groupBy("requestUrl").count();
        requestCount.orderBy(requestCount.col("count").desc()).show();


        // 使用sql的方式操作数据
        requestDetail.createOrReplaceTempView("request_table");
        spark.sql("select requestUrl, count(requestUrl) as count from request_table group by requestUrl order by count desc").show();

//        requestDetail.show();
    }

    // GET /ticket/first?email=zengzhenshuo@corp.polyv.net HTTP/1.1
    public static class RequestDetail{
        private String requestType;
        private String requestUrl;
        private String httpVersion;

        public RequestDetail() {
        }

        public RequestDetail(String requestType, String requestUrl, String httpVersion) {
            this.requestType = requestType;
            this.requestUrl = requestUrl;
            this.httpVersion = httpVersion;
        }

        public String getRequestType() {
            return requestType;
        }

        public void setRequestType(String requestType) {
            this.requestType = requestType;
        }

        public String getRequestUrl() {
            return requestUrl;
        }

        public void setRequestUrl(String requestUrl) {
            this.requestUrl = requestUrl;
        }

        public String getHttpVersion() {
            return httpVersion;
        }

        public void setHttpVersion(String httpVersion) {
            this.httpVersion = httpVersion;
        }

        @Override
        public String toString() {
            return "RequestDetail{" +
                    "requestType='" + requestType + '\'' +
                    ", requestUrl='" + requestUrl + '\'' +
                    ", httpVersion='" + httpVersion + '\'' +
                    '}';
        }
    }
}

