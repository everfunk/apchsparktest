package ru.mindb8.rnd.sparktest;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import liquibase.integration.spring.SpringLiquibase;
import lombok.val;
import org.apache.spark.sql.SparkSession;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.batch.spark.SparkYarnTasklet;

import org.springframework.data.hadoop.scripting.HdfsScriptRunner;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scripting.ScriptSource;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;



@Configuration
@EnableTransactionManagement
@Import({HadoopConfig.class})
public class SparkConfiguration {
    @Autowired
    private org.apache.hadoop.conf.Configuration hadoopConfiguration;

    @Value("${example.inputDir}")
    String inputDir;

    @Value("${example.inputFileName}")
    String inputFileName;

    @Value("${example.inputLocalDir}")
    String inputLocalDir;

    @Value("${example.outputDir}")
    String outputDir;

    @Value("${example.sparkAssembly}")
    String sparkAssembly;

    private final String liquibaseChangelog = "classpath:db.changelog.xml";

//    @Bean
//    Step initScript(StepBuilderFactory steps, Tasklet scriptTasklet) throws Exception {
//        return steps.get("initScript")
//                .tasklet(scriptTasklet)
//                .build();
//    }

//    @Bean
//    Tasklet scriptTasklet(HdfsScriptRunner scriptRunner) {
//        ScriptTasklet scriptTasklet = new ScriptTasklet();
//        scriptTasklet.setScriptCallback(scriptRunner);
//        return scriptTasklet;
//        return null;
//    }

    @Bean
    SparkSession spark() {
        return SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();
    }

    @Bean
    HdfsScriptRunner scriptRunner() {
        ScriptSource script = new ResourceScriptSource(new ClassPathResource("fileCopy.js"));
        HdfsScriptRunner scriptRunner = new HdfsScriptRunner();
        scriptRunner.setConfiguration(hadoopConfiguration);
        scriptRunner.setLanguage("javascript");
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("source", inputLocalDir);
        arguments.put("file", inputFileName);
        arguments.put("indir", inputDir);
        arguments.put("outdir", outputDir);
        scriptRunner.setArguments(arguments);
        scriptRunner.setScriptSource(script);
        return scriptRunner;
    }

//    @Bean
//    Step sparkTopHashtags(StepBuilderFactory steps, Tasklet sparkTopHashtagsTasklet) throws Exception {
//        return steps.get("sparkTopHashtags")
//                .tasklet(sparkTopHashtagsTasklet)
//                .build();
//    }

//    @Bean
//    SparkYarnTasklet sparkTopHashtagsTasklet() throws Exception {
//        SparkYarnTasklet sparkTasklet = new SparkYarnTasklet();
//        sparkTasklet.setSparkAssemblyJar(sparkAssembly);
//        sparkTasklet.setHadoopConfiguration(hadoopConfiguration);
//        sparkTasklet.setAppClass("Hashtags");
//        File jarFile = new File(System.getProperty("user.dir") + "/app/spark-hashtags_2.10-0.1.0.jar");
//        sparkTasklet.setAppJar(jarFile.toURI().toString());
//        sparkTasklet.setExecutorMemory("1G");
//        sparkTasklet.setNumExecutors(1);
//        sparkTasklet.setArguments(new String[]{
//                hadoopConfiguration.get("fs.defaultFS") + inputDir + "/" + inputFileName,
//                hadoopConfiguration.get("fs.defaultFS") + outputDir});
//        return sparkTasklet;
//    }

    @Bean
    JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(dataSource());
    }


    @Bean
    SimpleJdbcInsert insertStatement() {
        final SimpleJdbcInsert statement = new SimpleJdbcInsert(dataSource())
                .withTableName("SAMPLE")
                .usingGeneratedKeyColumns("ID")
                .usingColumns("VAL");
        return statement;
    }

    @Bean
    FillDBData fillDBData() {
        return new FillDBData(insertStatement());
    }


    @Bean
    DataSource dataSource() {

        final String jdbcUrl = "jdbc:h2:mem:testdb";
        final String driverClassName = "org.h2.Driver";
        final String dbUsername = "sa";
        final String dbPassword = "password";
        //spring.jpa.database-platform=org.hibernate.dialect.H2Dialect

        // DataSourceBuider есть в SpringBoot
        val config = new HikariConfig();
        config.setDriverClassName(driverClassName);
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(dbUsername);
        config.setPassword(dbPassword);
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        config.setAutoCommit(false);
        val ds = new HikariDataSource( config );
        return ds;
    }

    @Bean
    SpringLiquibase liquibase(DataSource dataSource) {
        val liquibase = new SpringLiquibase();
        liquibase.setDataSource(dataSource);
        liquibase.setChangeLog(liquibaseChangelog);
        return liquibase;
    }

//    @Bean
//    Properties jpaProperties() {
//        val props = new Properties();
///*
//    "org.hibernate.dialect.MySQLDialect"
//    "org.hibernate.dialect.SQLiteDialect"
//    "org.hibernate.dialect.MySQL5Dialect"
//    "org.hibernate.dialect.H2Dialect"
//    "org.hibernate.dialect.Oracle10gDialect"
//    "org.hibernate.dialect.PostgreSQL9Dialect"
// */
//        props.put("hibernate.dialect", hibernateDialect);
///*
//        validate: проверить схему, не вносить изменения в базу данных.
//        update: обновить схему.
//        create: создает схему, уничтожая предыдущие данные.
//        create-drop: отказаться от схемы, когда SessionFactory закрывается явно, как правило, когда приложение остановлено
//*/
//        props.put("hibernate.hbm2ddl.auto", "validate");
//        props.put("hibernate.default_schema", defaultDbSchema);
//        props.put("hibernate.show_sql", "true");
//        props.put("hibernate.connection.release_mode", "auto");
//        props.put("hibernate.connection.autoReconnect", "true");
//        props.put("hibernate.current_session_context_class", "thread");
//        return props;
//    }

    @Bean
    DataSourceTransactionManager txManager() {
        return new DataSourceTransactionManager(dataSource());
    }

//    @Bean
//    PlatformTransactionManager transactionManager(LocalContainerEntityManagerFactoryBean factoryBean) {
//        val transactionManager = new JpaTransactionManager();
//        transactionManager.setEntityManagerFactory(factoryBean.getObject());
//        return transactionManager;
//    }

}
