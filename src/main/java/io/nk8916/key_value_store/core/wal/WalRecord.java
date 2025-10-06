package io.nk8916.key_value_store.core.wal;

import jakarta.annotation.Nullable;
import lombok.Getter;

import java.util.Base64;

public class WalRecord {
    @Getter
    private String operation;
    @Getter
    private String key;
    private @Nullable Base64 value;
    @Getter
    private long timestamp;
    @Getter
    private long sequenceNumber;

    public WalRecord(String operation, String key, @Nullable Base64 value, long timestamp, long sequenceNumber) {
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
    }

    public @Nullable Base64 getValue() {
        return value;
    }

}


