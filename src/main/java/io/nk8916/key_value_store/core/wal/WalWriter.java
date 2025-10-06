package io.nk8916.key_value_store.core.wal;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public class WalWriter {

    private void writeRecord(WalRecord record) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(record);
        String walFilePath = WalConstants.WAL_FILE_PATH;
        try (FileChannel channel = FileChannel.open(Paths.get(walFilePath),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {

            ByteBuffer buffer = ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
            channel.write(buffer);
            channel.force(true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
