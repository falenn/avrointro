package com.example.avro;

import com.example.Customer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@RunWith(SpringRunner.class)
public class CustomerTest {

    /**
     * Create an instance of customer using the generated Avro model
     */
    @Test
    public void testCreateCustomer() {

        Customer.Builder builder = Customer.newBuilder();
        builder.setFirstName("Curtis");
        builder.setLastName("Bates");
        builder.setAge(47);
        builder.setAutomatedEmail(false);
        List<String> emails = new ArrayList<>();
        emails.add("me@gmail.com");
        builder.setEmails(emails);
        builder.setCreationTs(System.currentTimeMillis());
        builder.setUid(UUID.randomUUID().toString());
        builder.setHeight(79);
        builder.setWeight(120);
        Customer curtis = builder.build();

        log.info("Curtis: " + curtis);
    }

}
