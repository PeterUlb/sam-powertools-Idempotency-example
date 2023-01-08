package helloworld;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.lambda.powertools.idempotency.Idempotency;
import software.amazon.lambda.powertools.idempotency.IdempotencyConfig;
import software.amazon.lambda.powertools.idempotency.IdempotencyKey;
import software.amazon.lambda.powertools.idempotency.Idempotent;
import software.amazon.lambda.powertools.idempotency.exceptions.IdempotencyAlreadyInProgressException;
import software.amazon.lambda.powertools.idempotency.persistence.DynamoDBPersistenceStore;
import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.utilities.JsonConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class App implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    private static final Logger log = LogManager.getLogger(App.class);

    public App() {
        this(null);
    }

    public App(DynamoDbClient client) {
        Idempotency.config().withConfig(
                        IdempotencyConfig.builder()
                                .withEventKeyJMESPath("powertools_json(body).address") // will retrieve the address field in the body which is a string transformed to json with `powertools_json`
                                .build())
                .withPersistenceStore(
                        DynamoDBPersistenceStore.builder()
                                .withDynamoDbClient(client)
                                .withTableName(System.getenv("IDEMPOTENCY_TABLE"))
                                .build()
                ).configure();
    }

    /**
     * Try with:
     * <pre>
     *     curl -X POST https://[REST-API-ID].execute-api.[REGION].amazonaws.com/Prod/helloidem/ -H "Content-Type: application/json" -d '{"address": "https://checkip.amazonaws.com", "delay": 8}'
     * </pre>
     * <ul>
     *     <li>First call will execute the handleRequest normally, and store the response in the idempotency table (Look into DynamoDB)</li>
     *     <li>Second call (and next ones) will retrieve from the cache (if cache is enabled, which is by default) or from the store, the handler won't be called. Until the expiration happens (by default 1 hour).</li>
     * </ul>
     */
    @Logging(logEvent = true)
    // We could also put the @Idempotent annotation here, using it on the handler avoids executing the handler (cost reduction).
    // Use it on other methods to handle multiple items (with SQS batch processing for example)
    // On the handler @IdempotencyKey and Idempotency.registerLambdaContext(context) isn't required
    public APIGatewayProxyResponseEvent handleRequest(final APIGatewayProxyRequestEvent input, final Context context) {
        // If you are using the @Idempotent annotation on another method to guard isolated parts of your code,
        // you must use registerLambdaContext method available in the Idempotency object to benefit from the
        // protection against extended failed retries when a Lambda function times out
        Idempotency.registerLambdaContext(context);
        try {
            return extracted(input, context);
        } catch (IdempotencyAlreadyInProgressException e) {
            return new APIGatewayProxyResponseEvent()
                    .withHeaders(getHeaders())
                    .withStatusCode(409)
                    .withBody("{ \"message\": \"IdempotencyAlreadyInProgress\" }");
        }
    }

    @Idempotent
    public APIGatewayProxyResponseEvent extracted(@IdempotencyKey final APIGatewayProxyRequestEvent input, final Context context) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent()
                .withHeaders(getHeaders());
        try {
            JsonNode tree = JsonConfig.get().getObjectMapper().readTree(input.getBody());
            String address = tree.get("address").asText();
            int delay = tree.get("delay").asInt();
            final String pageContents = this.getPageContents(address);
            String output = String.format("{ \"message\": \"hello world\", \"location\": \"%s\" }", pageContents);

            log.info("ip is {}", pageContents);
            delay(delay);
            return response
                    .withStatusCode(200)
                    .withBody(output);

        } catch (IOException e) {
            return response
                    .withBody("{ \"message\": \"IO error occurred\" }")
                    .withStatusCode(500);
        }
    }

    private void delay(int delay) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(Integer.max(delay, 0)));
        } catch (InterruptedException e) {
            log.warn("Interrupted!", e);
            Thread.currentThread().interrupt();
        }
    }

    private String getPageContents(String address) throws IOException {
        URL url = new URL(address);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
            return br.lines().collect(Collectors.joining(System.lineSeparator()));
        }
    }

    private Map<String, String> getHeaders() {
        return Map.of(
                "Content-Type", "application/json",
                "Access-Control-Allow-Origin", "*",
                "Access-Control-Allow-Methods", "GET, OPTIONS",
                "Access-Control-Allow-Headers", "*"
        );
    }
}