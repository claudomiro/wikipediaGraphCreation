package com.claudomiro.wiki.links;

import lombok.extern.java.Log;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

@Log
public class WikiLinksGraphSparkJob implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient String inputFileName;
    private transient String jobOutputDirectory;

    private final SparkConf conf;

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        new WikiLinksGraphSparkJob(args[0], args[1]).run();
    }

    public WikiLinksGraphSparkJob(String inputFileName, String outputBaseDir) {
        this.inputFileName = inputFileName;
        log.info("Opening path '" + inputFileName + "'");
        conf = new SparkConf().setAppName("WikiLinksExtractionSparkJob").setMaster("local[*]");
        SimpleDateFormat outputFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        jobOutputDirectory = outputBaseDir + "job_" + outputFormat.format(new Date());
        log.info("Saving OUTPUT in '" + jobOutputDirectory + "'");
    }

    private void run() {
        try(JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<String> inputRDD = sc.textFile(inputFileName);
            inputRDD.map(line -> line.split("\t")[1])
                    .filter(str -> str.equalsIgnoreCase("RECIFE"))
                    .saveAsTextFile(jobOutputDirectory);
        }
    }
}
