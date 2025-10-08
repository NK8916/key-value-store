package io.nk8916.key_value_store.core.wal;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.file.StandardOpenOption.*;

public class WalOperation implements AutoCloseable{
    private final Path walDir;
    private final boolean fSyncWrite;
    private final ObjectMapper mapper;
    private Path activeWalPath;
    private FileChannel channel;
    private BufferedWriter writer;
    private BufferedReader reader;
    private final AtomicBoolean closed=new AtomicBoolean(false);

    public WalOperation(Path walDir,boolean fSyncWrite) throws IOException {
        this.walDir=walDir;
        this.fSyncWrite=fSyncWrite;
        this.mapper=new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        Files.createDirectories(walDir);
        openActive();
    }
    private void openActive() throws IOException {
        this.activeWalPath = walDir.resolve("wal.log");
        this.channel = FileChannel.open(activeWalPath, CREATE, WRITE, APPEND,READ);
        this.writer = new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(channel), StandardCharsets.UTF_8));
        this.reader=new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8));
    }

    public synchronized void append(WalRecord record) throws JsonProcessingException {
        String json = mapper.writeValueAsString(record);
        try {
            writer.write(json);
            writer.newLine();
            writer.flush();
            if (fSyncWrite) {
                channel.force(true);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write WAL record", e);
        }
    }

    public synchronized Path rotate() throws IOException {
        ensureOpen();
        try {
            writer.flush();
            channel.force(true);
        } finally {
            writer.close();
            channel.close();
        }
        String epoch=String.valueOf(Instant.now().toEpochMilli());
        String rotatedFileName = "wal_" + epoch + ".log";
        Path rotatedPath = walDir.resolve(rotatedFileName);
        Files.move(activeWalPath, rotatedPath, StandardCopyOption.ATOMIC_MOVE);
        openActive();
        return rotatedPath;
    }


    private void ensureOpen() {
        if (closed.get()) throw new IllegalStateException("WAL writer is closed");
    }


    @Override
    public synchronized void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                if (writer != null) writer.flush();
            } finally {
                try {
                    if (channel != null) channel.force(true);
                } finally {
                    if (writer != null) writer.close();
                    if (channel != null) channel.close();
                }
            }
        }
    }
}
