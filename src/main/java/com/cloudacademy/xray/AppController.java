package com.cloudacademy.xray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import com.amazonaws.xray.AWSXRay;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

import com.amazonaws.xray.entities.Segment;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.springframework.web.server.ResponseStatusException;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@RestController
public class AppController {
    private static final Logger logger = LoggerFactory.getLogger(AppController.class);

    private AmazonDynamoDB client;
    private DynamoDB dynamoDB;

    @Value("${aws.region}")
    private String region;

    @Value("${dynamodb.table}")
    String tableName;

    @PostConstruct
    public void init() {
        this.client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(region)
                .build();
        this.dynamoDB = new DynamoDB(client);
    }

    @RequestMapping("/post")
    public List<Map<String, Object>> post() {

        /* Add the Begin Segment here for post method */
        Segment segment = AWSXRay.beginSegment("Beginning segment: Inserting data to " + tableName);
        logger.info("Started segment: {}", segment.getName());

        /* ensure table already exists in the region */
        Table table = dynamoDB.getTable(tableName);
        ArrayList<Item> items = new ArrayList<>();

        try {
            Item a = new Item().withPrimaryKey("ID", 1).withString("User_Name", "SuperKingCowBaby")
                    .withString("Nationality", "Korean")
                    .withString("DOB", "2005-02");
            table.putItem(a);
            items.add(a);
            logger.info("Successfully added item with ID: {}", a.getString("ID"));

            Item b = new Item().withPrimaryKey("ID", 2).withString("User_Name", "JeongJeongie")
                    .withString("Nationality", "Korean")
                    .withString("DOB", "2004-09");
            table.putItem(b);
            items.add(b);
            logger.info("Successfully added item with ID: {}", b.getString("ID"));

            Item c = new Item().withPrimaryKey("ID", 3).withString("User_Name", "Ruto")
                .withString("Nationality", "Japanese")
                .withString("DOB", "2004-04");
            table.putItem(c);
            items.add(c);
            logger.info("Successfully added item with ID: {}", c.getString("ID"));

            Item d = new Item().withPrimaryKey("ID", 4).withString("User_Name", "PpirroTongTong")
                    .withString("Nationality", "Korean")
                    .withString("DOB", "2003-12");
            table.putItem(d);
            items.add(d);
            logger.info("Successfully added item with ID: {}", d.getString("ID"));

        } catch (RuntimeException e) {

            /* Add the Segment add exception here for post method */
		    segment.addException(e);
            logger.error("Error for /post: {}", e.getMessage());

        } finally {
            /* Add the end segment code here for post method */
		    AWSXRay.endSegment();
        }
        return items.stream()
                .map(Item::asMap)
                .collect(Collectors.toList());
    }

    @RequestMapping("/get")
    public Map<String, Object> get(@RequestParam("id") int id) {

        /* **Add the Begin Segment here for get method***/
        Segment segment = AWSXRay.beginSegment("Beginning segment: Fetching data from " + tableName);
        Table table = dynamoDB.getTable(tableName);
        Item item = null;

        try {
            item = table.getItem("ID", id);
            if (item == null) {
                logger.warn("Item with ID: {} not found.", id);
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No item found with ID: " + id);
            }
            logger.info("Successfully fetched item with ID: {}", item.getString("ID"));

        } catch (Exception e) {

            /* Add the Segment add exception here for get method */
            segment.addException(e);
            logger.error("Error for /get?id={}: {}", id, e.getMessage());
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);

        } finally {
            /* Add the end segment code here for get method */
            AWSXRay.endSegment();
        }

        return item.asMap();
    }

}
