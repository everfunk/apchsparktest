package ru.mindb8.rnd.sparktest;

import avro.shaded.com.google.common.collect.Iterators;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class FillDBData {
    private final SimpleJdbcInsert statement;

    @Transactional
    public void fill(int n) {
        val batchSize = 100;
        val records = new Random()
                                                        .doubles(n)
                                                        .mapToObj(x -> new MapSqlParameterSource()
                                                                            .addValue("ID", x));

        Iterators.partition(records.iterator(), batchSize).forEachRemaining(
                x -> statement.executeBatch(SqlParameterSourceUtils.createBatch(x))
        );
    }
}
