package helloworld;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.tests.EventLoader;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;

public class AppTest {
    @Mock
    private Context context;
    private App app;
    private static DynamoDbClient client;
    private AutoCloseable closeable;

    @BeforeAll
    public static void setupDynamoLocal() {
        int port = getFreePort();
        try {
            DynamoDBProxyServer dynamoProxy = ServerRunner.createServerFromCommandLineArgs(new String[]{
                    "-inMemory",
                    "-port",
                    Integer.toString(port)
            });
            dynamoProxy.start();
        } catch (Exception e) {
            throw new RuntimeException();
        }

        client = DynamoDbClient.builder()
                .httpClient(UrlConnectionHttpClient.builder().build())
                .region(Region.EU_WEST_1)
                .endpointOverride(URI.create("http://localhost:" + port))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("FAKE", "FAKE")))
                .build();

        client.createTable(CreateTableRequest.builder()
                .tableName("idempotency")
                .keySchema(KeySchemaElement.builder().keyType(KeyType.HASH).attributeName("id").build())
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build()
                )
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
    }

    private static int getFreePort() {
        try {
            ServerSocket socket = new ServerSocket(0);
            int port = socket.getLocalPort();
            socket.close();
            return port;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        app = new App(client);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    void testApp() {
        APIGatewayProxyResponseEvent response = app.handleRequest(EventLoader.loadApiGatewayRestEvent("event.json"), context);
        Assertions.assertNotNull(response);
        Assertions.assertTrue(response.getBody().contains("hello world"));
    }
}
