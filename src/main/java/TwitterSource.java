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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;

public class TwitterSource implements ProcessorSupplier {

    public TwitterSource(String consumerKey, String consumerSecret, String token, String secret) {

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
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

        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            String msg = null;
            try {
                msg = queue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(msg);
        }

    }

    @Override
    public Collection<? extends Processor> get(int count) {
        ArrayList<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            AbstractProcessor abstractProcessor = new AbstractProcessor() {

                private Stream<String> stream;

                @Override
                protected void init(Context context) throws Exception {
                    super.init(context);
                    stream = Stream.generate(() -> "test1");
                }


                @Override
                public boolean complete() {

                    return emitFromTraverser(traverseStream(stream));
                }

            };
            processors.add(abstractProcessor);
        }
        return processors;

    }
}
