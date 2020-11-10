package com.example.avro.generic;


import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import static org.junit.Assert.*;

@Slf4j
@RunWith(SpringRunner.class)
public class GenericCustomerTest {

    /** looks up and returns the Avro schema file
     *
     * @param resource as {@link String}
     * @return Schema as {@link Schema}
     * @throws Exception
     */
    public static Schema getSchema(String resource) throws Exception {
        //log.info("getting schema for " + resource);
        Path filepath = Paths.get(ClassLoader.getSystemResource(resource).toURI());
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(Files.readString(filepath, StandardCharsets.US_ASCII));
    }

    @Test
    /**
     * Create a Customer
     */
    public void testSimpleCustomer() {
        log.debug("Begin");
        try {
            GenericRecordBuilder customerBuilder = new GenericRecordBuilder(
                    getSchema("avro/Customer.avsc"));

            customerBuilder.set("first_name", "John")
                    .set("uid", UUID.randomUUID().toString())
                    .set("last_name", "Smith")
                    .set("age", 50)
                    .set("height", 170f)
                    .set("weight", 80.5f)
                    .set("automated_email", false)
                    .set("emails", "joh.smith@gmail.com")
                    .set("creation_ts", System.currentTimeMillis());

            GenericData.Record john = customerBuilder.build();
            log.info("John record: " + john);
        } catch (Exception e ) {
            log.warn("Error: " + e);
            fail();
        }
    }

    /**
     * Create Customer with defaults only
     */
    @Test
    public void testSimpleCustomerWithDefaults() {
        log.debug("Begin");
        try {
            GenericRecordBuilder customerBuilder = new GenericRecordBuilder(
                    getSchema("avro/Customer.avsc"));

            customerBuilder.set("first_name", "John")
                    .set("uid", UUID.randomUUID().toString())
                    .set("last_name", "Smith")
                    .set("age", 50)
                    .set("height", 170f)
                    .set("weight", 80.5f)
                    .set("creation_ts", LogicalTypes.timestampMillis());
            GenericData.Record john = customerBuilder.build();
            log.info("John default record: " + john);
        } catch (Exception e ) {
            log.warn("Error: " + e);
            fail();
        }
    }

    /**
     * Build Customer purposefully missing a field that has no default.
     */
    @Test
    public void testRuntimeValidationError() {
        log.debug("Begin");
        try {
            GenericRecordBuilder customerBuilder = new GenericRecordBuilder(
                    getSchema("avro/Customer.avsc"));

            customerBuilder.set("first_name", "John")
                    .set("uid", UUID.randomUUID().toString())
                    .set("age", 50)
                    .set("height", 170f)
                    .set("weight", 80.5f)
                    .set("creation_ts", System.currentTimeMillis());
            GenericData.Record john = customerBuilder.build();
            log.info("John default record: " + john);
            fail("Should validate with an error.  No last name provided.");
        } catch (Exception e ) {
            log.warn("Error: " + e);
        }
    }

    @Test
    public void testWriteCustomerToFile() {
        List<GenericData.Record> records = new ArrayList<>();
        try {
            GenericRecordBuilder customerBuilder = new GenericRecordBuilder(
                    getSchema("avro/Customer.avsc"));
            customerBuilder
                    .set("uid", UUID.randomUUID().toString())
                    .set("first_name", "John")
                    .set("last_name", "Smith")
                    .set("age", 50)
                    .set("height", 170f)
                    .set("weight", 80.5f)
                    .set("creation_ts", System.currentTimeMillis());
            records.add(customerBuilder.build());
            records.add(customerBuilder.build());
            writeToFile("customer-generic.avro",records);

            List<GenericRecord> r  = readFromfile("customer-generic.avro");
            assertEquals(r.size(),records.size());
        } catch (Exception e ) {
            log.warn("Error: " + e);
            fail();
        }
    }

    public static void writeToFile(String filename, List<GenericData.Record> records) throws IOException {
        log.debug("Begin");
        //@todo should assert list not length 0...
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(records.get(0).getSchema());
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(records.get(0).getSchema(), new File(filename));
        for(GenericData.Record record : records) {
            dataFileWriter.append(record);
        }
        dataFileWriter.flush();
        dataFileWriter.close();
        log.debug("Done");
    }

    public List<GenericRecord> readFromfile(String filename) throws IOException {
        log.debug("Begin");
        final File file = new File(filename);
        List<GenericRecord> records = new ArrayList<>();
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

        DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(file,datumReader);
        while(dataFileReader.hasNext()) {
            GenericRecord r = dataFileReader.next();
            log.info("Added: " + r);
            records.add(r);
        }
        dataFileReader.close();
        return records;
    }
}
