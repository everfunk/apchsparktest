package ru.mindb8.rnd.sparktest;

import io.github.cgi.zabbix.api.DefaultZabbixApi;
import io.github.cgi.zabbix.api.RequestBuilder;
import io.github.cgi.zabbix.api.ZabbixApi;
import lombok.val;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.hadoop.batch.spark.SparkYarnTasklet;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import scala.Tuple2;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.SPACE;

public class SparkMain {
    public static void main(String[] args) {
//        val zabbixUrl = "http://xxx.xxx.xxx.xxx:443/zabbix/api_jsonrpc.php";
//        val zabbixUser = "";
//        val zabbixPass = "";
//        ZabbixApi zabbixApi = new DefaultZabbixApi(zabbixUrl);
//        zabbixApi.init();
//        zabbixApi.login(zabbixUser, zabbixPass);
//        zabbixApi.apiVersion();

//        val request =
//                RequestBuilder.newBuilder()
//                .method("host.get")
//                .paramEntry("filter", filter)
//                .build();
//
//        request = RequestBuilder.newBuilder()
//                .method("host.update")
//                .paramEntry("hostid", hostid)
//                .paramEntry("host", "localhost”)
//                        .paramEntry("name", "localhost")
//                        .build();
//
//        zabbixApi.call(request);

        try (val context = new AnnotationConfigApplicationContext();){
            context.register(SparkConfiguration.class);
            context.scan("ru.mindb8.rnd.sparktest");
            context.refresh();
            context.registerShutdownHook();
            val filler = context.getBean(FillDBData.class);
            filler.fill(100);
            val j = context.getBean(JdbcTemplate.class);
            val q = j.query("SELECT ID, VAL FROM SAMPLE", new RowMapper<Pair<Long, BigDecimal>>() {
                @Override
                public Pair<Long, BigDecimal> mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return new Pair<>(rs.getLong("ID"), rs.getBigDecimal("VAL"));
                }
            });
            val spark = context.getBean(SparkSession.class);
            val ds = spark.read()
                .format("jdbc")
                .option("driver", "org.h2.Driver")
                .option("url", "jdbc:h2:mem:testdb")
                .option("query", "SELECT ID, VAL FROM SAMPLE")
                .option("user", "sa")
                .option("password", "password")
                .option("batchsize", 100)
                .load();

            val rdd = ds.rdd();
            System.out.println("+++++++++" + rdd.count());
        }
    }

    public static void main1(String[] args) {
        SparkYarnTasklet tasklet;
        System.setProperty("hadoop.home.dir", "/C:/###/hadoop/hadoop-2.8.3/");
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }
        SparkConf sparkConf =
                new SparkConf()
                        .setAppName("JavaWordCount")
                        .setMaster("local");
                        //.setSparkHome(".");

        /*SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();*/
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = ctx.parallelize(data);

        JavaRDD<String> words
                = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones
                = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts
                = ones.reduceByKey((Integer i1, Integer i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        List<Row> dataTraining = Arrays.asList(
                RowFactory.create(1.0, Vectors.dense(0.0, 1.1, 0.1)),
                RowFactory.create(0.0, Vectors.dense(2.0, 1.0, -1.0)),
                RowFactory.create(0.0, Vectors.dense(2.0, 1.3, 1.0)),
                RowFactory.create(1.0, Vectors.dense(0.0, 1.2, -0.5))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        SQLContext sqlContext = new SQLContext(ctx);
        Dataset<Row> training = sqlContext.createDataFrame(dataTraining, schema);

// Create a LogisticRegression instance. This instance is an Estimator.
        LogisticRegression lr = new LogisticRegression();
// Print out the parameters, documentation, and any default values.
        System.out.println("LogisticRegression parameters:\n" + lr.explainParams() + "\n");

// We may set parameters using setter methods.
        lr.setMaxIter(10).setRegParam(0.01);

// Learn a LogisticRegression model. This uses the parameters stored in lr.
        LogisticRegressionModel model1 = lr.fit(training);
// Since model1 is a Model (i.e., a Transformer produced by an Estimator),
// we can view the parameters it used during fit().
// This prints the parameter (name: value) pairs, where names are unique IDs for this
// LogisticRegression instance.
        System.out.println("Model 1 was fit using parameters: " + model1.parent().extractParamMap());

// We may alternatively specify parameters using a ParamMap.
        ParamMap paramMap = new ParamMap()
                .put(lr.maxIter().w(20))  // Specify 1 Param.
                .put(lr.maxIter(), 30)  // This overwrites the original maxIter.
                .put(lr.regParam().w(0.1), lr.threshold().w(0.55));  // Specify multiple Params.

// One can also combine ParamMaps.
        ParamMap paramMap2 = new ParamMap()
                .put(lr.probabilityCol().w("myProbability"));  // Change output column name
        ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);

// Now learn a new model using the paramMapCombined parameters.
// paramMapCombined overrides all parameters set earlier via lr.set* methods.
        LogisticRegressionModel model2 = lr.fit(training, paramMapCombined);
        System.out.println("Model 2 was fit using parameters: " + model2.parent().extractParamMap());


        StructType schema1 = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });


// Prepare test documents.
        List<Row> dataTest = Arrays.asList(
                RowFactory.create(1.0, Vectors.dense(-1.0, 1.5, 1.3)),
                RowFactory.create(0.0, Vectors.dense(3.0, 2.0, -0.1)),
                RowFactory.create(1.0, Vectors.dense(0.0, 2.2, -1.5))
        );
        //Dataset<Row> test = sqlContext.createDataFrame(distData, schema1);

// Make predictions on test documents using the Transformer.transform() method.
// LogisticRegression.transform will only use the 'features' column.
// Note that model2.transform() outputs a 'myProbability' column instead of the usual
// 'probability' column since we renamed the lr.probabilityCol parameter previously.
        //Dataset<Row> results = model2.transform(test);
        //Dataset<Row> rows = results.select("features", "label", "myProbability", "prediction");
        //for (Row r: rows.collectAsList()) {
        //    System.out.println("(" + r.get(0) + ", " + r.get(1) + ") -> prob=" + r.get(2)
        //            + ", prediction=" + r.get(3));
        //}
        ctx.stop();
    }

}
