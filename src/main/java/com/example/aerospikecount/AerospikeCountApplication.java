package com.example.aerospikecount;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AerospikeCountApplication implements CommandLineRunner {

    @Override
    public void run(String... args) throws Exception {
        AerospikeClient client = new AerospikeClient(args[0], 3000);
        Statement statement = new Statement();
        statement.setNamespace(args[1]);
        statement.setSetName(args[2]);

        long count = 0L;
        try (RecordSet records = client.query(null, statement)) {
            while (records != null && records.next()) {
                count++;
            }
        }

        System.out.println("count = " + count);
    }

    public static void main(String[] args) {
        SpringApplication.run(AerospikeCountApplication.class, args);
    }

}
