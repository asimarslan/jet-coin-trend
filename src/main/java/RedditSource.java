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
import net.dean.jraw.RedditClient;
import net.dean.jraw.http.NetworkAdapter;
import net.dean.jraw.http.OkHttpNetworkAdapter;
import net.dean.jraw.http.UserAgent;
import net.dean.jraw.models.Submission;
import net.dean.jraw.oauth.Credentials;
import net.dean.jraw.oauth.OAuthHelper;
import net.dean.jraw.pagination.DefaultPaginator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hazelcast on 24/01/2018.
 */
public class RedditSource implements ProcessorSupplier {

    private Properties secret;

    public RedditSource() {
        try {
            secret = new Properties();
            secret.load(this.getClass().getResourceAsStream("reddit-security.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public Collection<? extends Processor> get(int count) {
        AbstractProcessor abstractProcessor = new AbstractProcessor() {
            Set<String> ids = Collections.newSetFromMap(new ConcurrentHashMap<>());
            RedditClient reddit;
            List<String> coins;
            public List<String> getSubmissionListForSubreddit(String name) {
                DefaultPaginator<Submission> paginator = getPaginatorForSubreddit(name);
                List<Submission> submissions = paginator.accumulateMerged(-1);
                return analyzeTitleAndContents(submissions);
            }

            public DefaultPaginator<Submission> getPaginatorForSubreddit(String subredditName) {
                return reddit
                        .subreddit(subredditName)
                        .posts()
                        .limit(20)
                        .build();

            }

            public List<String> analyzeTitleAndContents(List<Submission> submissions) {
                List<String> texts = new ArrayList<String>();
                for (Submission submission : submissions) {
                    if (ids.contains(submission.getId())) continue;
                    ids.add(submission.getId());
                    texts.add(submission.getTitle() + "." + submission.getSelfText());
                }
                ids.clear();
                return texts;
            }

            @Override
            protected void init(Context context) throws Exception {
                super.init(context);
                coins = new ArrayList<String>(){{
                    add("Bitcoin");
                    add("Etherum");
                    add("Bitcoincash");
                    add("Cardano");
                    add("Litecoin");
                    add("Stellar");
                    add("NEM");
                    add("EOS");
                }};
                UserAgent userAgent = new UserAgent("bot", "com.example.usefulbot", "v0.1", "mattbdean");
                // Create our credentials
                String username = secret.getProperty("username");
                String password = secret.getProperty("password");
                String clientId = secret.getProperty("clientId");
                String clientSecret = secret.getProperty("clientSecret");
                Credentials credentials = Credentials.script(username, password, clientId, clientSecret);

                // This is what really sends HTTP requests
                NetworkAdapter adapter = new OkHttpNetworkAdapter(userAgent);

                // Authenticate and get a RedditClient instance
                reddit = OAuthHelper.automatic(adapter, credentials);
                System.out.println("authenticated to reddit ");

            }

            @Override
            public boolean complete() {
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for(String coin:coins){
                    List<String> strings = getSubmissionListForSubreddit(coin);
                    for (String string : strings) {
                        if (!tryEmit(string)) {
                            return false;
                        }
                    }
                }
                return false;

            }

        };
        return Collections.singleton(abstractProcessor);

    }

}
