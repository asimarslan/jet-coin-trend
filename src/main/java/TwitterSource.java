import com.google.common.collect.Lists;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSource implements ProcessorSupplier {

    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String secret;

    public TwitterSource(String consumerKey, String consumerSecret,
                         String token, String secret) throws InterruptedException {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.token = token;
        this.secret = secret;
    }

    @Override
    public Collection<? extends Processor> get(int count) {
        AbstractProcessor abstractProcessor = new AbstractProcessor() {

            private BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);


            private BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(10000);

            @Override
            protected void init(Context context) throws Exception {
                super.init(context);
                StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

                // add some track terms
                endpoint.trackTerms(Lists.newArrayList("bitcoin", "#btc"));

                Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

                Client client = new ClientBuilder()
                        .hosts(Constants.STREAM_HOST)
                        .endpoint(endpoint)
                        .authentication(auth)
                        .processor(new StringDelimitedProcessor(queue))
                        .eventMessageQueue(eventQueue)                          // optional: use this if you want to process client events
                        .build();

                client.connect();

                Thread thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        while (true) {
                            try {
                                Event take = eventQueue.take();
                                System.out.println(take.getMessage());
                                System.out.println(take.getEventType());
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
                thread.start();


            }

            @Override
            public boolean complete() {
                for (int i = 0; i < 100; i++) {
                    String message = queue.poll();
                    if (message != null) {
                        if (!tryEmit(message)) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                return false;
            }

        };
        return Collections.singleton(abstractProcessor);

    }
}
