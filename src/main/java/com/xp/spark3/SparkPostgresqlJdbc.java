package com.xp.spark3;

import com.xp.spark3.entity.Product;
import org.apache.spark.sql.*;

import java.util.List;
import java.util.Properties;

public class SparkPostgresqlJdbc {

    public static void main(String[] args) {

        Properties connectionProperties = new Properties();


        //增加数据库的用户名(user)密码(password),指定postgresql驱动(driver)
        connectionProperties.put("user", "postgres");
        connectionProperties.put("password", "xiapeng");
        connectionProperties.put("driver", "org.postgresql.Driver");
        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("SparkPostgresqlJdbc")
                .getOrCreate();
        //启动runSparkPostgresqlJdbc程序
        //query(spark, connectionProperties);
        save(spark, connectionProperties);
        //update(spark, connectionProperties);

        spark.stop();

    }

    private static void query(SparkSession spark, Properties connectionProperties) {
        //SparkJdbc读取Postgresql的products表内容
        Dataset<Row> jdbcDF = spark.read()
                .jdbc("jdbc:postgresql://192.168.1.12:5432/xiapeng", "products", connectionProperties).select("product_no", "price");
        //显示jdbcDF数据内容
        jdbcDF.show();

    }

    private static void save(SparkSession spark, Properties connectionProperties) {
        Product product = Product.builder().product_no(4).price(7.99F).name("Accord").build();

        Dataset<Product> jdbcDF = spark.createDataset(List.of(product), Encoders.bean(Product.class));
        jdbcDF.write().mode(SaveMode.Append).jdbc("jdbc:postgresql://192.168.1.12:5432/xiapeng", "products", connectionProperties);
    }

    /**
     * update暂时不可用
     * @param spark
     * @param connectionProperties
     */
    private static void update(SparkSession spark, Properties connectionProperties) {
        Product product = Product.builder().product_no(4).price(7.99F).name("Accord").build();

        Dataset<Product> jdbcDF = spark.createDataset(List.of(product), Encoders.bean(Product.class));
        // overwrite 会全部删除之间的表数据，然后重新写入一条
        jdbcDF.write().mode(SaveMode.Append).jdbc("jdbc:postgresql://192.168.1.12:5432/xiapeng", "products", connectionProperties);
    }
}
