package ru.mindb8.rnd.sparktest;

import lombok.Value;

@Value
public class Pair<T, S> {
    T first;
    S second;
}
