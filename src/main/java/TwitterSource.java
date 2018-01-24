import com.google.common.collect.Lists;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSource implements ProcessorSupplier {

    private BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);

    public TwitterSource(String consumerKey, String consumerSecret, String token, String secret) {


        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        // add some track terms
        endpoint.trackTerms(Lists.newArrayList("bitcoin", "#btc"));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();


    }

    @Override
    public Collection<? extends Processor> get(int count) {
        AbstractProcessor abstractProcessor = new AbstractProcessor() {

            @Override
            protected void init(Context context) throws Exception {
                super.init(context);
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
