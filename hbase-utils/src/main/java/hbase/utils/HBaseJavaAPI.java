package hbase.utils;

import commons.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.rmi.CORBA.Util;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by sponge on 2017/6/2.
 */

public class HBaseJavaAPI {
    private static Configuration conf = null;
    private static Connection conn = null;

    private static  final String tableName = "sefon:student";



    private static boolean isExist(String tableName) throws IOException {
        Admin admin = Utils.getConn().getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void createTable(String tableName, String[] colFamilys) throws IOException {

        if (isExist(tableName)) {
            System.out.println("表 " + tableName + " 已存在！");
            return;
        }
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        for (String colFamily : colFamilys) {
            tableDesc.addFamily(new HColumnDescriptor(colFamily));
        }
        Admin admin = Utils.getConn().getAdmin();
        admin.createTable(tableDesc);
        admin.close();
    }

    public static void deleteTable(String tableName) throws IOException {
        Admin admin = Utils.getConn().getAdmin();
        TableName tbName = TableName.valueOf(tableName);
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("删除的表 " + tableName + " 不存在！");
        } else {
            admin.disableTable(tbName);
            admin.deleteTable(tbName);
            System.out.println("删除表 " + tableName + " 成功！");
        }
        admin.close();
    }

    public static Boolean renameTable(String oldTableName, String newTableName) throws IOException {
        Admin admin = Utils.getConn().getAdmin();
        TableName oldtb = TableName.valueOf(oldTableName);
        TableName newtb = TableName.valueOf(newTableName);
        if (admin.tableExists(oldtb)) {
            String snapshotName = getRandomString(10);
            HTableDescriptor ht = admin.getTableDescriptor(oldtb);
            admin.disableTable(oldtb);
            try {
                admin.snapshot(snapshotName, oldtb);
                admin.cloneSnapshot(snapshotName, newtb);
            } catch (Exception e) {
                e.printStackTrace();
                admin.enableTable(oldtb);
                admin.close();
                return false;
            }

            admin.deleteSnapshot(snapshotName);
            admin.deleteTable(oldtb);
            admin.createTable(ht);
            admin.close();
            return true;
        } else {
            admin.close();
            return false;
        }
    }


    public static String getRandomString(int length) { //length表示生成字符串的长度
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

    public static void addRow(Table table, String rowKey, String colFamily, String column, String value) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }

    public static void addRow(String table, String rowKey, String colFamily, String column, String value) throws IOException {
        Table htable = Utils.getConn().getTable(TableName.valueOf(table));
        addRow(htable, rowKey, colFamily, column, value);
        htable.close();
    }

    public static void delRow(String table, String rowKey) throws IOException {
        Table htable = Utils.getConn().getTable(TableName.valueOf(table));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        htable.delete(delete);
        htable.close();
    }

    public static void delMultiRows(String table, String[] rowKeys) throws IOException {
        Table htable = Utils.getConn().getTable(TableName.valueOf(table));
        List<Delete> delList = new ArrayList<Delete>();
        for (String rowkey : rowKeys) {
            delList.add(new Delete(Bytes.toBytes(rowkey)));
        }
        htable.delete(delList);
        htable.close();
    }

    public static void getRow(String table, String rowKey) throws IOException {
        Table htable = Utils.getConn().getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = htable.get(get);
        Utils.printResult(result);
    }

    public static void getAllRows(String table) throws IOException {
        Table htable = Utils.getConn().getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        ResultScanner results = htable.getScanner(scan);
        // 输出结果
        Utils.printResult(results);
    }

    public static void getAllRows(String table, String[] colFamilys) throws IOException {
        Table htable = Utils.getConn().getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        for (String colFamily : colFamilys) {
            scan.addFamily(Bytes.toBytes(colFamily));
        }
        ResultScanner results = htable.getScanner(scan);
        Utils.printResult(results);
    }

    public static void getAllRows(String table, String[] colFamilys, Filter filter) throws IOException {
        Table htable = Utils.getConn().getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        for (String colFamily : colFamilys) {
            scan.addFamily(Bytes.toBytes(colFamily));
        }
        if (filter != null) {
            scan.setFilter(filter);
        }
        ResultScanner results = htable.getScanner(scan);
        Utils.printResult(results);
    }

    public static void getAllRows(String table,  Filter filter) throws IOException {
        Table htable = Utils.getConn().getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        if (filter != null) {
            scan.setFilter(filter);
        }
        ResultScanner results = htable.getScanner(scan);
        Utils.printResult(results);
    }
    private static void addStudentRow(
            String rowKey, String ch, String math, String en, String sex, String age) throws IOException {

        HBaseJavaAPI.addRow(tableName, rowKey, "info", "age", age);
        HBaseJavaAPI.addRow(tableName, rowKey, "info", "sex", sex);
        HBaseJavaAPI.addRow(tableName, rowKey, "course", "china", ch);
        HBaseJavaAPI.addRow(tableName, rowKey, "course", "math", math);
        HBaseJavaAPI.addRow(tableName, rowKey, "course", "english", en);
    }

    public static void main(String[] args) throws IOException {

        // 第一步：创建数据库表：“student”
        String[] columnFamilys = {"info", "course"};
        HBaseJavaAPI.createTable(tableName, columnFamilys);
        // 第二步：向数据表的添加数据
        if (isExist(tableName)) {
            addStudentRow("zpc","97","128","85","boy","20");
            addStudentRow("henjun","90","120","90","boy","19");
            addStudentRow("wangjun","100","100","99","girl","18");
            addStudentRow("lijie","75","99","78","girl","19");
            addStudentRow("lishuang","80","98","78","boy","19");
            addStudentRow("zhaoyun","75","99","78","girl","2");

            // 第三步：获取一条数据
            System.out.println("**************获取一条(zpc)数据*************");
            HBaseJavaAPI.getRow(tableName, "zpc");
            // 第四步：获取所有数据
            System.out.println("**************获取所有数据***************");
            HBaseJavaAPI.getAllRows(tableName);
            System.out.println("**************获取所有数据 info 列簇***************");
            HBaseJavaAPI.getAllRows(tableName, new String[]{"info"});
            System.out.println("**************获取filter sex 为girl数据***************");
            Filter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("info"), Bytes.toBytes("sex"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("girl"));
            HBaseJavaAPI.getAllRows(tableName, new String[]{"info","course"}, filter);


            System.out.println("**************获取filter sex 为girl 且 age > 18数据***************");
            FilterList filterList=new FilterList();
            filterList.addFilter(filter);
            Filter filterScore = new SingleColumnValueFilter(
                    Bytes.toBytes("info"), Bytes.toBytes("age"), CompareFilter.CompareOp.GREATER, Bytes.toBytes("18"));
            filterList.addFilter(filterScore);
            HBaseJavaAPI.getAllRows(tableName, new String[]{"info","course"}, filterList);
            System.out.println("**************获取filter sex 为girl 或 age > 18数据***************");
            filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            filterList.addFilter(filter);
            filterList.addFilter(filterScore);
            HBaseJavaAPI.getAllRows(tableName, filterList);
            System.out.println("**************获取filter sex 为girl 或 age > 18 且 family 为info数据***************");
            Filter  filtefamily
                    = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info")));
            FilterList filterFamily = new FilterList();
            filterFamily.addFilter(filterList);
            filterFamily.addFilter(filtefamily);
            HBaseJavaAPI.getAllRows(tableName, filterFamily);

            System.out.println("**************获取filter rowkey 以l开始数据***************");
            //new SubstringComparator("li") 包涵li的查询
            Filter rowfilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryPrefixComparator(Bytes.toBytes("li")));
            HBaseJavaAPI.getAllRows(tableName, new String[]{"info","course"}, rowfilter);


            // 第五步：删除一条数据
            System.out.println("************删除一条(zpc)数据************");
            HBaseJavaAPI.delRow(tableName, "zpc");
            HBaseJavaAPI.getAllRows(tableName);
            // 第六步：删除多条数据
            System.out.println("**************删除多条数据***************");
            String rows[] = new String[]{"qingqing", "xiaoxue"};
            HBaseJavaAPI.delMultiRows(tableName, rows);
            HBaseJavaAPI.getAllRows(tableName);
            // 第七步：删除数据库
            System.out.println("***************删除数据库表**************");
            HBaseJavaAPI.deleteTable(tableName);
            System.out.println("表" + tableName + "存在吗？" + isExist(tableName));
        } else {
            System.out.println(tableName + "此数据库表不存在！");
        }
    }
}


