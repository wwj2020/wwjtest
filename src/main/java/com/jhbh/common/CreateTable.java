package com.jhbh.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Date;
import java.util.LinkedList;

/**
 * 操作HBase的工具类，主要作用，提供hbase的Connection用于增加和获取数据
 * 作用相当于连接池
 */
public class CreateTable {
    //构建一个池子
    private static LinkedList<Connection> pool = new LinkedList<Connection>();
    private static Admin admin;
    static Connection connection;

    public CreateTable() {
    }

    @Before
    public void getConn() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "bigdata-01:2181,bigdata-02:2181,bigdata-03:2181");
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //创建原始AT表
    @Test
    public void createATTable() {
        try {
            createTable("acceptance");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //创建原始DV表
    @Test
    public void createDVTable() {
        try {
            createTable("delivery");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*//创建AT索引表
    @Test
    public void createAtIndex() {
        try {
            createIndexTable("atIndex");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //创建DV索引表
    @Test
    public void createdvIndex() {
        try {
            createIndexTable("dvIndex");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/

    /*@Test
    public void  createIndex_atcompanycode(){
        try {
            createIndexTable("index_atcompanycode");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_atmancerttype(){
        try {
            createIndexTable("index_atmancerttype");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_atmancertcode(){
        try {
            createIndexTable("index_atmancertcode");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_atmantime(){
        try {
            createIndexTable("index_atmantime");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_atmanname(){
        try {
            createIndexTable("index_atmanname");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_atmanphone(){
        try {
            createIndexTable("index_atmanphone");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_atemployee(){
        try {
            createIndexTable("index_atemployee");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    *//*@Test
    public void  createIndex_atcreatetime(){
        try {
            createIndexTable("index_atcreatetime");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*//*
    @Test
    public void  createIndex_atdestcity(){
        try {
            createIndexTable("index_atdestcity");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_atinterfacee(){
        try {
            createIndexTable("index_atinterfacee");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //创建dv索引表
    @Test
    public void  createIndex_dvcompanycode(){
        try {
            createIndexTable("index_dvcompanycode");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_dvmancerttype(){
        try {
            createIndexTable("index_dvmancerttype");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_dvmancertcode(){
        try {
            createIndexTable("index_dvmancertcode");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_dvmantime(){
        try {
            createIndexTable("index_dvmantime");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_dvmanname(){
        try {
            createIndexTable("index_dvmanname");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_dvemployee(){
        try {
            createIndexTable("index_dvemployee");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    *//*@Test
    public void  createIndex_dvcreatetime(){
        try {
            createIndexTable("index_dvcreatetime");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*//*
    @Test
    public void  createIndex_dvtype(){
        try {
            createIndexTable("index_dvtype");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  createIndex_dvinterface(){
        try {
            createIndexTable("index_dvinterface");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //查询
    @Test
    public void getResult(){
        try {
            String billcode="75149046114289";
            String tableName="basic_acceptance";
            Table table = connection.getTable(TableName.valueOf(tableName));
            int startRand = 0;
            int endRand = 100;
            int startTime = 1500000000;
            int endTime = 1600000000;
            Scan scan = new Scan();
            scan.setStartRow((startRand+"_"+billcode+"_"+startTime).getBytes());
            scan.setStopRow((endRand+"_"+billcode+"_"+endTime).getBytes());
            ResultScanner scanner = table.getScanner(scan);
            for(Result result:scanner){
                System.out.println(result);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
*/
    //查询测试
    @Test
    public void getResult() {
        try {
            // 全表扫描
            Scan scan = new Scan();
//			scan.addFamily(CF_BASE.getBytes());
//			scan.addColumn(CF_BASE.getBytes(), "name".getBytes());
//			scan.setStartRow("baiyc_20150716_0004".getBytes());
//			scan.setStopRow("rk0001".getBytes());

            // ValueFilter(=, 'substring:0004')
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator("375"));
//			scan.setFilter(filter);

            // 列： Qualifier
            Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator("A01610100000"));
//			scan.setFilter(filter2);

            Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filter,filter2);
            scan.setFilter(filterList);
            Table atIndex = connection.getTable(TableName.valueOf("acceptance"));
            ResultScanner rs = atIndex.getScanner(scan);
            for(Result res:rs){
                System.out.println(res);
            }
        } catch (IOException e) {
            System.out.println("scan异常");
        }
    }



  //创建表
    public void createTable(String tname) throws Exception {

        //1、要创建的 表 描述信息
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tname));

        //2、要创建的 表 列族信息描述
        HColumnDescriptor f = new HColumnDescriptor("cf");
//        HColumnDescriptor f2 = new HColumnDescriptor("cf2");

        //3、将要创建的 表 列族信息添加进表描述信息里面
        tableDescriptor.addFamily(f);
//        tableDescriptor.addFamily(f2);

        //4、使用Hbase的DDL对象创建表
        admin.createTable(tableDescriptor);
        System.out.println(tname + " created successfully！");
    }


   /* public void createIndexTable(String tname) throws Exception {

        //1、要创建的 表 描述信息
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tname));

        //2、要创建的 表 列族信息描述
        HColumnDescriptor f1 = new HColumnDescriptor("cf1");

        //3、将要创建的 表 列族信息添加进表描述信息里面
        tableDescriptor.addFamily(f1);

        //4、使用Hbase的DDL对象创建表
        admin.createTable(tableDescriptor);
        System.out.println(tname + " created successfully！");
    }*/


}
