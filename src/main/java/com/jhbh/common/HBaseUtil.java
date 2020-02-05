/*
package com.jhbh.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.LinkedList;

*/
/**
 * 操作HBase的工具类，主要作用，提供hbase的Connection用于增加和获取数据
 * 作用相当于连接池
 */
/*
public class HBaseUtil {
    //构建一个池子
    private static LinkedList<Connection> pool = new LinkedList<Connection>();
    private static Admin admin;
    static HConnection connection;
    static Configuration newConf;
    private static Configuration conf = HBaseConfiguration.create();

    private HBaseUtil() {

    }
    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "bigdata-01:2181,bigdata-02:2181,bigdata-03:2181");
//            conf.set("hbase.zookeeper.quorum", "service01:2181,service02:2181,service03:2181,service05:2181,service06:2181");

            for (int i = 0; i < 40; i++) {
                Connection connection = ConnectionFactory.createConnection(conf);
                pool.push(connection);
            }
        } catch (IOException e){
            throw new RuntimeException("初始化异常。。。。");
        }
    }

    public static Connection getConnection() {
//        Connection res = pool.poll();
        while(pool.size()<10) {
            try {
                for (int i = 0; i < 10; i++) {
                    Connection connection = ConnectionFactory.createConnection(conf);
                    pool.push(connection);
                }
                System.out.println("连接池小于10，请稍等一下再来");
                Thread.sleep(1000);
            } catch (Exception e) {
                System.out.println("连接池小于10，创建失败");
                e.printStackTrace();
            }
        }
        return pool.poll();
    }
        */
/*if (res == null){
            System.out.println("——————————————————————连接池为空，新创建一个——————————————————");
            try {
                res =  ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                System.out.println("————————————————连接池为空，新创建失败——————————————————————");
                e.printStackTrace();
            }
        }
        return res;*//*


    public static void release(Connection connection) {
        pool.push(connection);
    }
}
    */
/* static{
        try {
//            conf.set("hbase.zookeeper.quorum", "service-01:2181,service-02:2181,service-03:2181,service-05:2181,service-06:2181");
            conf.set("hbase.zookeeper.quorum", "bigdata-01:2181,bigdata-02:2181,bigdata-03:2181");
//            conf.set("hbase.zookeeper.quorum", "service01:2181,service02:2181,service03:2181,service05:2181,service06:2181");


            for (int i = 0; i < 30; i++) {
                connection = ConnectionFactory.createConnection(conf);
                pool.push(connection);
            }
        } catch (IOException e){
            throw new RuntimeException("初始化异常。。。。");
        }
    }*//*


    */
/*public static Connection getConnection(){
        if(pool.size()<20){
            for (int i = 0; i < 30; i++) {
                try {
                    connection = ConnectionFactory.createConnection(conf);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                pool.push(connection);
            }
        }
        return pool.poll();
    }*//*






*/
