package commons;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by sponge on 2017/6/15.
 */
public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);
    private static Connection con;
    private static Random random = new Random();

    public static final String tableName = "sefon:student";
    private static final int BUFFER_SIZE = 1024;

    public static void printResult(Result result) {
        for (Cell cell : result.rawCells()) {
            printCell(cell);
        }
    }

    public static void printResult(ResultScanner results) {
        for (Result result : results) {
            printResult(result);
        }
    }

    public static void printCell(Cell cell) {
        System.out.print("行名:" + Bytes.toString(CellUtil.cloneRow(cell)) + " ");
        System.out.print("时间戳:" + cell.getTimestamp() + " ");
        System.out.print("列族名:" + Bytes.toString(CellUtil.cloneFamily(cell)) + " ");
        System.out.print("列名:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + " ");
        System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
    }

    public  static Connection getConn() throws IOException {
        if (con == null) {
            Configuration conf = HBaseConfiguration.create();
            Properties props = Config.getHbaseProperties();
            for (Map.Entry<Object, Object> entry : props.entrySet()) {
                String key = (String) entry.getKey();
                String value = (String) entry.getValue();
                logger.debug("key : " + key + " value : " + value);
                conf.set(key, value);
            }
            con = ConnectionFactory.createConnection(conf);
        }
        return con;
    }

    public static void testData(int maxPuts, int menoSize) throws  IOException {
        Admin admin = Utils.getConn().getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            String[] cols = {"info", "course"};
            for (String colFamily : cols) {
                tableDesc.addFamily(new HColumnDescriptor(colFamily));
            }
            admin.createTable(tableDesc, genSplitKey());
            admin.close();
        }
        Table table = getConn().getTable(TableName.valueOf(tableName));

        int messageNum = 0;
        List<Put> puts = new ArrayList<Put>(BUFFER_SIZE);
        for (int i = 0; i < maxPuts; i++) {
            Put put = Student(randString(5) + Integer.toString(i), random.nextInt(100), random.nextInt(2),
                    random.nextInt(100), random.nextInt(100), random.nextInt(100),
                    randString(menoSize));
            puts.add(put);
            if (puts.size() == BUFFER_SIZE) {
                table.put(puts);
                messageNum += puts.size();
                logger.info("Commit message " + puts.size() + " Total message " + messageNum);

                puts.clear();
            }
        }
        if (puts.size() > 0) {
            table.put(puts);
            messageNum += puts.size();
            logger.info("Commit message " + puts.size() + " Total message " + messageNum);

        }

    }
    private static Put Student(String rowKey, int ch, int math, int en, int sex, int age, String meno){

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(sex));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("meno"), Bytes.toBytes(meno));
        put.addColumn(Bytes.toBytes("course"), Bytes.toBytes("math"), Bytes.toBytes(math));
        put.addColumn(Bytes.toBytes("course"), Bytes.toBytes("china"), Bytes.toBytes(ch));
        put.addColumn(Bytes.toBytes("course"), Bytes.toBytes("english"), Bytes.toBytes(en));

        return put;
    }

    private static byte[][] genSplitKey() {
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        int splitLen = base.length();
        byte[][] splits = new byte[splitLen][];
        for(int i=0; i < splitLen; i ++){
            byte[] key = new byte[1];
            key[0] = (byte)base.charAt(i);
            splits[i] = key ;
        }
        return  splits;
    }

    public static String randString(int length) {
        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        int charLength = base.length();
        StringBuilder sb = new StringBuilder();
        for(int i =0; i < length; i++){
            sb.append(base.charAt(random.nextInt(charLength)));
        }
        return sb.toString();
    }

}
