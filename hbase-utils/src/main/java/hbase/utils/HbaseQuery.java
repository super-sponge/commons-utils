package hbase.utils;

import commons.Utils;
import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by sponge on 2017/6/15.
 */
public class HbaseQuery {
    private static final Logger logger = LoggerFactory.getLogger(HbaseQuery.class);
    private String tableName;
    private String rowKey;
    private Connection con = null;

    public HbaseQuery(String tableName, String rowKey) {
        this.tableName = tableName;
        this.rowKey = rowKey;
    }

    public ResultScanner Query(Filter filter) throws IOException {
        Table table = Utils.getConn().getTable(TableName.valueOf(this.tableName));
        Scan scan = new Scan();
        scan.setFilter(filter);
        return table.getScanner(scan);
    }
    public Result Query() throws IOException {
        Table table = Utils.getConn().getTable(TableName.valueOf(this.tableName));
        Get get = new Get(Bytes.toBytes(this.rowKey));
        return table.get(get);
    }

    public static void main(String[] args ) throws IOException {
        Options opts = new Options();
        opts.addOption("h", false, "Help message");
        opts.addOption("n", true, "Number messages");
        opts.addOption("s", true, "Message size");

        CommandLineParser parse = new DefaultParser();
        CommandLine cli = null;
        try {
            cli = parse.parse(opts, args);
            if (cli.hasOption("h")) {
                HelpFormatter hf = new HelpFormatter();
                hf.printHelp("May optioins", opts );
                return;
            }

            int msgCount = Integer.parseInt(cli.getOptionValue("n", "50000"));
            int msgSize = Integer.parseInt(cli.getOptionValue("s", "1024"));
            Utils.testData(msgCount, msgSize);

        } catch (ParseException e) {
            logger.error("Parse Error " + e.getMessage());
        }

        Utils.testData(5000000, 512);

    }

}
