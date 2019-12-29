package io.millesabords.mongo2els;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.jongo.Jongo;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Simple tool for exporting data from MongoDB to Elasticsearch.
 *
 * @author Bruno Bonnin
 */
public class Mongo2Els {
    private static Logger LOGGER = LoggerFactory.getLogger(Mongo2Els.class);
    //    @Option(name = "-m", usage = "Processing mode", required = true, metaVar = "<bulk |
    // realtime>")
    private String mode = "bulk";
    //    @Option(name = "-c", usage = "Configuration file", required = true, metaVar = "<file name>")
    private String configFileName = "mongo2els-example.properties";
    private MongoClient mongoClient;
    private Jongo jongo;
    private Client elsClient;
    public static void main(final String[] args) {
        new Mongo2Els().doMain(args);
    }
    private void doMain(final String[] args) {
        final CmdLineParser parser = new CmdLineParser(this);
        try {
            parser.parseArgument(args);
            final Config cfg = Config.get();
            cfg.load(new FileInputStream(configFileName));
            LOGGER.info("Config : {}", cfg);
            initMongoClient();
            initElsClient();
            LOGGER.info("Start : {}", new Date());
            if ("bulk".equalsIgnoreCase(mode)) {
                new BulkIndexing(jongo, elsClient, cfg.getInt(Config.ELS_BULK_THREADS)).indexData();
            } else if ("realtime".equalsIgnoreCase(mode)) {
                final RealtimeIndexing indexing = new RealtimeIndexing(elsClient);
                final OplogTailer tailer = new OplogTailer(mongoClient);
                tailer.addListener(indexing);
                final ExecutorService exec = Executors.newSingleThreadExecutor();
                exec.submit(tailer);
            } else {
                System.err.println("Bad mode : " + mode);
                parser.printUsage(System.err);
            }
        } catch (final CmdLineException e) {
            System.err.println("Bad argument");
            parser.printUsage(System.err);
        } catch (final Exception e) {
            LOGGER.error("Problem during processing", e);
        }
        LOGGER.info("End : {}", new Date());
    }
    @SuppressWarnings("deprecation")
    private void initMongoClient() {
        final Config cfg = Config.get();
        mongoClient =
                new MongoClient(
                        new ServerAddress("35.247.238.219", 27017),
                        Arrays.asList(
                                MongoCredential.createCredential("rw", "orders", "chapolin@30".toCharArray())));
        mongoClient.getServerAddressList(); // To check that the connection is ok
        jongo = new Jongo(mongoClient.getDB(cfg.get(Config.MONGO_DB)));
    }
    private void initElsClient() throws IOException {
        Map<String, String> properties = new HashMap<>();
        properties.put("cluster.name", "ordersearch-cluster");
        elsClient =
                new PreBuiltTransportClient(Settings.builder().putProperties(properties, key -> key)
                        .build())
                        .addTransportAddress(
                                new TransportAddress(
                                        InetAddress.getByName("10.158.0.8"), 9300))
                        .addTransportAddress(
                                new TransportAddress(
                                        InetAddress.getByName("10.158.0.7"), 9300))
                        .addTransportAddress(
                                new TransportAddress(
                                        InetAddress.getByName("10.158.0.6"), 9300));
    }
}

