package com.jhbh.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;

/**
 * 操作HBase的工具类，主要作用，提供hbase的Connection用于增加和获取数据
 * 作用相当于连接池
 */
public class DeleteTable {
    //构建一个池子
    private static LinkedList<Connection> pool = new LinkedList<Connection>();
    private static Admin admin;
    static Connection connection;

    public DeleteTable() {
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

    //删除AT表
    @Test
    public void  deleteATTable(){
        try {
            deleteTable("acceptance");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //删除DV表
    @Test
    public void  deleteDVTable(){
        try {
            deleteTable("delivery");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
   /* //删除AT索引表
    @Test
    public void  deleteIndex_atcompanycode(){
        try {
            deleteTable("index_atcompanycode");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  deleteIndex_atmancerttype(){
        try {
            deleteTable("index_atmancerttype");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  deleteIndex_atmancertcode(){
        try {
            deleteTable("index_atmancertcode");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  deleteIndex_atmantime(){
        try {
            deleteTable("index_atmantime");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  deleteIndex_atmanname(){
        try {
            deleteTable("index_atmanname");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  deleteIndex_atmanphone(){
        try {
            deleteTable("index_atmanphone");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }@Test
    public void  deleteIndex_atemployee(){
        try {
            deleteTable("index_atemployee");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
   *//* @Test
    public void  deleteIndex_atcreatetime(){
        try {
            deleteTable("index_atcreatetime");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*//*
    @Test
    public void  deleteIndex_atdestcity(){
        try {
            deleteTable("index_atdestcity");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void  deleteIndex_atinterfacee(){
        try {
            deleteTable("index_atinterfacee");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
   *//* @Test
    public void  deleteIndex_atsource(){
        try {
            deleteTable("index_atsource");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*//*
    //删除dv索引表
    @Test
    public void deleteIndex_dvcompanycode() {
        try {
            deleteTable("index_dvcompanycode");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteIndex_dvmancerttype() {
        try {
            deleteTable("index_dvmancerttype");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteIndex_dvmancertcode() {
        try {
            deleteTable("index_dvmancertcode");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteIndex_dvmantime() {
        try {
            deleteTable("index_dvmantime");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteIndex_dvmanname() {
        try {
            deleteTable("index_dvmanname");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteIndex_dvemployee() {
        try {
            deleteTable("index_dvemployee");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
*/
    /*@Test
    public void deleteIndex_dvcreatetime() {
        try {
            deleteTable("index_dvcreatetime");
        } catch (Exception e) {
            e.printStackTrace();
            ;
        }
    }*/

    /*@Test
    public void deleteIndex_dvtype() {
        try {
            deleteTable("index_dvtype");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteIndex_dvinterface() {
        try {
            deleteTable("index_dvinterface");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
*/
    public void deleteTable(String tname) throws Exception {

        //使用Hbase的DDL对象 删除表
        //1、先禁用表
        admin.disableTable(TableName.valueOf(tname));
        //2、删除表
        admin.deleteTable(TableName.valueOf(tname));
        System.out.println(tname + " deleted successfully！");
    }
}