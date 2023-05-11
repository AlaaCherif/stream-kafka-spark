package tn.insat.tp3;

import com.google.gson.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;;
import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class SparkKafkaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Gson gson = new GsonBuilder().create();

    private SparkKafkaWordCount() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: SparkKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }
        MongoClientURI uri = new MongoClientURI("mongodb://alaa:alaajebcar@ac-eo6n1um-shard-00-00.nxtedqb.mongodb.net:27017,ac-eo6n1um-shard-00-01.nxtedqb.mongodb.net:27017,ac-eo6n1um-shard-00-02.nxtedqb.mongodb.net:27017/?ssl=true&replicaSet=atlas-tli1u5-shard-0&authSource=admin&retryWrites=true&w=majority");
        MongoClient mongoClient = new MongoClient(uri);
        MongoDatabase database = mongoClient.getDatabase("accidents");
        MongoCollection<Document> collection = database.getCollection("locations");

        SparkConf sparkConf = new SparkConf().setAppName("SparkKafkaWordCount");
        // Create the context with a batch size of 2 seconds
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                new Duration(2000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");
        for (String topic : topics) {
            topicMap.put(topic, numThreads);
        }

        // Create a Kafka stream of key-value pairs, where the value is a JSON string
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);


        messages.foreachRDD(rdd -> {
            rdd.map(record -> {
                Gson gson = new Gson();
                JsonObject jsonObject = gson.fromJson(record._2(), JsonObject.class);
                String location = jsonObject.get("borough").getAsString();
                String date = jsonObject.get("date").getAsString();
                Double lat = jsonObject.get("lat").getAsDouble();
                Double lon = jsonObject.get("lon").getAsDouble();
                MongoUtils.saveToMongoDB(lon, lat);
                JsonArray casualties = jsonObject.get("casualties").getAsJsonArray();
                List<Integer> ageList = new ArrayList<>();
                for (JsonElement element : casualties) {
                    Casualty casualty = gson.fromJson(element, Casualty.class);
                    ageList.add(casualty.getAge());
                }
                String time = date.split("T")[1];
                String timeHour = time.split(":")[0];
                String period;
                if (Integer.parseInt(timeHour) > 18 || Integer.parseInt(timeHour) < 6)
                    period = "night";
                else period = "day";

                System.out.println(location + ";" + period + ";" + ageList.get(0).toString());
                return location + ";" + period + ";" + ageList.get(0).toString();
            }).saveAsTextFile("/stream-results-2");
        });


        jssc.start();
        jssc.awaitTermination();
    }
}


class MongoUtils {
    private static final String DB_URI = "mongodb://alaa:alaajebcar@ac-eo6n1um-shard-00-00.nxtedqb.mongodb.net:27017,ac-eo6n1um-shard-00-01.nxtedqb.mongodb.net:27017,ac-eo6n1um-shard-00-02.nxtedqb.mongodb.net:27017/?ssl=true&replicaSet=atlas-tli1u5-shard-0&authSource=admin&retryWrites=true&w=majority";
    private static final String DB_NAME = "Accidents";
    private static final String COLLECTION_NAME = "locations";

    public static void saveToMongoDB(double lon, double lat) {
        // Create a MongoDB client instance
        MongoClientURI uri = new MongoClientURI(DB_URI);
        MongoClient mongoClient = new MongoClient(uri);

        // Get a handle to the database and collection
        MongoDatabase database = mongoClient.getDatabase(DB_NAME);
        MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);

        // Create a document with the longitude and latitude fields
        Document document = new Document();
        document.put("lon", lon);
        document.put("lat", lat);
        // Save the document to the collection
        collection.insertOne(document);

        // Close the MongoDB client
        mongoClient.close();
    }
}


class Casualty {
    private int age;

    public int getAge() {
        return age;
    }
}
