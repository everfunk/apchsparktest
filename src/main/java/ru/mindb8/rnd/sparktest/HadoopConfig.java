package ru.mindb8.rnd.sparktest;

import lombok.val;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HadoopConfig {

   @Value("${empty:\"/C:/###/hadoop/hadoop-2.8.3/\"}")
   private String hadoopPath;

   @Bean
   public org.apache.hadoop.conf.Configuration hadoopConfiguration() {
       val hadoopConf = new org.apache.hadoop.conf.Configuration(true);
       //hadoopConf.addResource(new Path(hadoopPath, "core-site.xml"));
       //hadoopConf.addResource(new Path(hadoopPath, "hdfs-site.xml"));
       return hadoopConf;
   }
}
