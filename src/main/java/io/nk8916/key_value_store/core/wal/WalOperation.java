package io.nk8916.key_value_store.core.wal;

public enum WalOperation {
    PUT(1, "PUT"),
    DELETE(2, "DELETE");

    private final int code;
    private final String description;
    WalOperation(int code, String description) {
        this.code = code;
        this.description = description;
    }
    public int getCode() {
        return code;
    }
    public String getDescription() {
        return description;
    }
}
