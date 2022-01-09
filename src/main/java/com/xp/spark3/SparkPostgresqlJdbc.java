package com.xp.spark3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkPostgresqlJdbc {

    public static void main (String[] args) {
        System.out.println(args);
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("SparkPostgresqlJdbc")
                .getOrCreate();
        //启动runSparkPostgresqlJdbc程序
        runSparkPostgresqlJdbc(spark);

        spark.stop();

    }

    private static void runSparkPostgresqlJdbc(SparkSession spark){
        //new一个属性
        System.out.println("确保数据库已经开启，并创建了products表和插入了数据");
        Properties connectionProperties = new Properties();


        //增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
        connectionProperties.put("user","postgres");
        connectionProperties.put("password","xiapeng");
        connectionProperties.put("driver","org.postgresql.Driver");



        //SparkJdbc读取Postgresql的products表内容
        System.out.println("SparkJdbc读取Postgresql的products表内容");
        Dataset<Row> jdbcDF = spark.read()
                .jdbc("jdbc:postgresql://192.168.1.12:5432/xiapeng","products",connectionProperties).select("product_no","price");
        //显示jdbcDF数据内容
        jdbcDF.show();

    }
}
