package com.bigdata.dis.sdk.demo.other;

import java.sql.*;
import java.util.TimeZone;

public class MysqlInsert {

    //驱动程序名
    static String driver = "com.mysql.jdbc.Driver";
    //URL指向要访问的数据库名mydata
    static String url = "jdbc:mysql://192.168.0.156:3306/test";
    //MySQL配置时的用户名
    static String user = "root";
    //MySQL配置时的密码
    static String password = "Bigdata@12345";

    public static void main(String[] args) {
        insert();
    }

    public static void insert() {
        //声明Connection对象
        Connection con;

        //遍历查询结果集
        try {
            //加载驱动程序
            Class.forName(driver);
            //1.getConnection()方法，连接MySQL数据库！！
            con = DriverManager.getConnection(url, user, password);
            if (!con.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            //2.创建statement类对象，用来执行SQL语句！！
            // create table test.t2(name varchar(50), addr varchar(100), t1 timestamp);
            con.setAutoCommit(false); // 设置手动提交

            String sql = "insert into t2(name, addr, t1) values('11','11',?)";
            int count = 0;
            PreparedStatement psts = con.prepareStatement(sql);
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1; i++) {
                for (int j = 0; j < 10; j++) {
                    //psts.setTimestamp(1, new Timestamp(new java.util.Date().getTime()));
                    psts.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                    psts.addBatch();
                }
                psts.executeBatch(); // 执行批量处理
                con.commit();  // 提交
                System.out.println("commit " + i);
                Thread.sleep(1000);
            }

            System.out.println("All down : " + count + ", cost " + (System.currentTimeMillis() - start) + " ms");
            con.close();

        } catch (ClassNotFoundException e) {
            //数据库驱动类异常处理
            System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        } catch (SQLException e) {
            //数据库连接失败异常处理
            e.printStackTrace();
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        } finally {
            System.out.println("数据库数据成功获取！！");
        }
    }

    public static void select() {
        //声明Connection对象
        Connection con;
        //遍历查询结果集
        try {
            //加载驱动程序
            Class.forName(driver);
            //1.getConnection()方法，连接MySQL数据库！！
            con = DriverManager.getConnection(url, user, password);
            if (!con.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            //2.创建statement类对象，用来执行SQL语句！！
            Statement statement = con.createStatement();
            //要执行的SQL语句
            String sql = "select * from t2";
            //3.ResultSet类，用来存放获取的结果集！！
            ResultSet rs = statement.executeQuery(sql);
            System.out.println("-----------------");
            System.out.println("执行结果如下所示:");
            System.out.println("-----------------");
            System.out.println("姓名" + "\t" + "职称");
            System.out.println("-----------------");

            String job = null;
            String id = null;
            while (rs.next()) {
                //获取stuname这列数据
                job = rs.getString("id");
                //获取stuid这列数据
                id = rs.getString("name");

                //输出结果
                System.out.println(id + "\t" + job);
            }
            rs.close();
            con.close();
        } catch (ClassNotFoundException e) {
            //数据库驱动类异常处理
            System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        } catch (SQLException e) {
            //数据库连接失败异常处理
            e.printStackTrace();
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        } finally {
            System.out.println("数据库数据成功获取！！");
        }
    }
}
