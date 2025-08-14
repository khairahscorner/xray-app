package com.cloudacademy.xray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.entities.Segment;
import org.springframework.web.server.ResponseStatusException;

import javax.annotation.PostConstruct;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.*;
import java.util.stream.Collectors;

@RestController
public class AppController {
    private static final Logger logger = LoggerFactory.getLogger(AppController.class);

    private DynamoDbClient dynamoDbClient;

    @Value("${aws.region}")
    private String region;

    @Value("${dynamodb.table}")
    private String tableName;

    @PostConstruct
    public void init() {
        this.dynamoDbClient = DynamoDbClient.builder()
                .region(Region.of(region))
                .build();
    }

    @RequestMapping("/post")
    public List<Map<String, Object>> post() {

        /* Add the Begin Segment here for post method */
        Segment segment = AWSXRay.beginSegment("Beginning segment: Inserting data to " + tableName);
        logger.info("Started segment: {}", segment.getName());

        List<Map<String, Object>> insertedItems = new ArrayList<>();

        try {
            insertedItems.add(putItem(1, "SuperKingCowBaby", "Korean", "2005-02"));
            insertedItems.add(putItem(2, "JeongJeongie", "Korean", "2004-09"));
            insertedItems.add(putItem(3, "Ruto", "Japanese", "2004-04"));
            insertedItems.add(putItem(4, "PpirroTongTong", "Korean", "2003-12"));

        } catch (RuntimeException e) {

            /* Add the Segment add exception here for post method */
		    segment.addException(e);
            logger.error("Error for /post: {}", e.getMessage());
        } finally {
            /* Add the end segment code here for post method */
		    AWSXRay.endSegment();
        }

        return insertedItems;
    }

    @RequestMapping("/get")
    public Map<String, Object> get(@RequestParam("id") int id) {

        /* **Add the Begin Segment here for get method***/
        Segment segment = AWSXRay.beginSegment("Beginning segment: Fetching data from " + tableName);

        try {
            GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("ID", AttributeValue.builder().n(String.valueOf(id)).build()))
                    .build());

            if (!response.hasItem() || response.item().isEmpty()) {
                logger.warn("Item with ID: {} not found.", id);
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No item found with ID: " + id);
            }

            logger.info("Successfully fetched item with ID: {}", id);
            return attributeValueMapToPlainMap(response.item());

        } catch (Exception e) {

            /* Add the Segment add exception here for get method */
            segment.addException(e);
            logger.error("Error for /get?id={}: {}", id, e.getMessage());
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        } finally {
            /* Add the end segment code here for get method */
            AWSXRay.endSegment();
        }
    }

    // Helper to insert item
    private Map<String, Object> putItem(int id, String userName, String nationality, String dob) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ID", AttributeValue.builder().n(String.valueOf(id)).build());
        item.put("User_Name", AttributeValue.builder().s(userName).build());
        item.put("Nationality", AttributeValue.builder().s(nationality).build());
        item.put("DOB", AttributeValue.builder().s(dob).build());

        dynamoDbClient.putItem(PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build());

        logger.info("Successfully added item with ID: {}", id);
        return attributeValueMapToPlainMap(item);
    }

    // Convert AttributeValue map to plain Java map
    private Map<String, Object> attributeValueMapToPlainMap(Map<String, AttributeValue> attributeMap) {
        return attributeMap.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            AttributeValue v = e.getValue();
                            if (v.s() != null) return v.s();
                            if (v.n() != null) return v.n();
                            if (v.bool() != null) return v.bool();
                            return null;
                        }
                ));
    }
}
