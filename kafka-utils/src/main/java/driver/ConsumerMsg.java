package driver;


import work.ConsumerThread;
import org.apache.commons.cli.*;

import java.util.Properties;


/**
 * Created by sponge on 2017/3/25 0025.
 */
public class ConsumerMsg {

    public static void main(String[] args) {


        Options opts = new Options();
        opts.addOption("t",  true, "Topic name");
        opts.addOption("h", false, "Help message");
        CommandLineParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(opts, args);
            if (cl.getOptions().length > 0 ) {
                if (cl.hasOption('h')) {
                    HelpFormatter hf = new HelpFormatter();
                    hf.printHelp("May Options", opts);
                } else {
                    String topic = cl.getOptionValue("t");
                    consumerMsg(topic);
                }
            } else {
                HelpFormatter hf = new HelpFormatter();
                hf.printHelp("May Options", opts);
            }
        } catch (ParseException e) {
            e.printStackTrace();
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("May Options", opts);
        }
    }
    private static void consumerMsg(String topic) {

        ConsumerThread consumerThread1 = new ConsumerThread(topic,  "myThread-1");
//        ConsumerThread consumerThread2 = new ConsumerThread(topic,  "myThread-2");

        consumerThread1.start();
//        consumerThread2.start();
    }
}