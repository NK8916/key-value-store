package io.nk8916.key_value_store.core.wal;

public final class WalConstants {
    private WalConstants() {
        // Prevent instantiation
    }

    public static final String WAL_FILE_PATH = "data/wal/wal.log";
    public static final String WAL_DIRECTORY = "data/wal";
}
