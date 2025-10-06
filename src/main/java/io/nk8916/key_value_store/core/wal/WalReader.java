package io.nk8916.key_value_store.core.wal;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class WalReader {

    public WalRecord[] readWalRecord() {
        Path walPath = Paths.get(WalConstants.WAL_FILE_PATH);

        if (!Files.exists(walPath)) {
            return new WalRecord[0];
        }
        List<WalRecord> records = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();

        try{
            List<String> lines = Files.readAllLines(walPath);
            for (String line : lines) {
                WalRecord record = mapper.readValue(line, WalRecord.class);
                records.add(record);
            }

        }catch (IOException e) {
            e.printStackTrace();
        }
        return records.toArray(new WalRecord[0]);
    }
}
