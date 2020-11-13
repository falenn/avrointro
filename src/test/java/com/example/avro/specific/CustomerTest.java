package com.example.avro.specific;

import com.example.Customer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
@RunWith(SpringRunner.class)
public class CustomerTest {

    /**
     * Create an instance of customer using the generated Avro model
     */
    @Test
    public void testCreateCustomer() {

        Customer.Builder builder = Customer.newBuilder();
        builder.setFirstName("John");
        builder.setLastName("Smith");
        builder.setAge(47);
        builder.setAutomatedEmail(false);
        List<String> emails = new ArrayList<>();
        emails.add("me@gmail.com");
        builder.setEmails(emails);
        builder.setCreationTs(System.currentTimeMillis());
        builder.setUid(UUID.randomUUID().toString());
        builder.setHeight(79.4f);
        builder.setWeight(120.2f);
        Customer curtis = builder.build();

        log.info("Customer: " + curtis);
    }

    @Test
    public void testWriteCustomerToFile() {
        List<Customer> records = new ArrayList<>();
        try {
            records.add(buildCustomer("John","Doe"));
            records.add(buildCustomer("Jane","Doe"));
            writeToFile("customer-specific.avro",records);

            List<Customer> r  = readFromfile("customer-specific.avro");
            assertEquals(r.size(),records.size());

            //Print the first name
            for(GenericRecord rec : r) {
                log.info("first name: " + rec.get("first_name"));
            }

            //print the schema carried by the object
            log.info("Schema: " + r.get(0).getSchema().toString());
        } catch (Exception e ) {
            log.warn("Error: " + e);
            fail();
        }
    }

    protected static void writeToFile(String filename, List<Customer> records) throws IOException {
        log.debug("Begin");
        //@todo should assert list not length 0...
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(records.get(0).getSchema(), new File(filename));
        for(Customer record : records) {
            dataFileWriter.append(record);
        }
        dataFileWriter.flush();
        dataFileWriter.close();
        log.debug("Done");
    }

    protected static List<Customer> readFromfile(String filename) throws IOException {
        log.debug("Begin");
        final File file = new File(filename);
        List<Customer> records = new ArrayList<>();
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);

        DataFileReader<Customer> dataFileReader =
                new DataFileReader<>(file,datumReader);
        while(dataFileReader.hasNext()) {
            Customer c = dataFileReader.next();
            log.info("Added: " + c);
            records.add(c);
        }
        dataFileReader.close();
        return records;
    }

    protected static Customer buildCustomer(String firstname, String lastName) {
        Customer.Builder builder = Customer.newBuilder();
        builder.setFirstName(firstname);
        builder.setLastName(lastName);
        builder.setAge(47);
        builder.setAutomatedEmail(false);
        List<String> emails = new ArrayList<>();
        emails.add("me@gmail.com");
        builder.setEmails(emails);
        builder.setCreationTs(System.currentTimeMillis());
        builder.setUid(UUID.randomUUID().toString());
        builder.setHeight(79.4f);
        builder.setWeight(120.2f);
        return builder.build();
    }

}
