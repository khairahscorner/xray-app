package com.cloudacademy.xray;


import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.xray.entities.Subsegment;
import com.amazonaws.xray.entities.Segment;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;


@RestController
public class AppController {

    /*Initiate the DynamoDB Client*/

    DynamoDB dynamoDB = new DynamoDB(client);
    String tableName = "employee_details";
    Item item;

    @RequestMapping("/post")
    public Segment post() {


        /* Add the Begin Segment here for post method */


        Table table = dynamoDB.getTable(tableName);
        try {

            item = new Item().withPrimaryKey("ID", 001).withString("Employee_Name", "Martha")
                .withString("Employee_Role", "Software Developer")
                .withStringSet("Address", new HashSet < String > (Arrays.asList("#14832", "Street 12", "New York")))
                .withString("Currency", "USD")
                .withNumber("Salary", 100000)
                .withString("Date_of_Joining", "2020-01-31")
                .withBoolean("Is_Employee_Active", true);
            table.putItem(item);
            System.out.print(item.toJSONPretty());

            item = new Item().withPrimaryKey("ID", 002).withString("Employee_Name", "Jennifer")
                .withString("Employee_Role", "HR Manager")
                .withStringSet("Address", new HashSet < String > (Arrays.asList("#1232", "Street 1", "California")))
                .withString("Currency", "USD")
                .withNumber("Salary", 60000)
                .withString("Date_of_Joining", "2019-04-23")
                .withBoolean("Is_Employee_Active", true);

            table.putItem(item);
            System.out.print(item.toJSONPretty());



        } catch (RuntimeException e) {

            /* Add the Segment add exception here for post method */
            System.err.println("Something went wrong");
            System.err.println(e.getMessage());


        } finally {
            /* Add the end segment code here for post method */

        }
        return segment;
    }

    @RequestMapping("/get")
    public Segment get() {

        /* **Add the Begin Segment here for get method***/

        Table table = dynamoDB.getTable(tableName);
        try {

            Item item = table.getItem("ID", 001);
            System.out.println(item.toJSONPretty());

            item = table.getItem("ID", 002);
            System.out.println(item.toJSONPretty());

        } catch (Exception e) {

            /* Add the Segment add exception here for get method */
            System.err.println("get failed.");
            System.err.println(e.getMessage());
        } finally {
            /* Add the end segment code here for get method */
        }

        return segment;
    }
}