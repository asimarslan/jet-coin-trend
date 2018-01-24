import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.JSONObject;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

public class SentimentProcessor extends AbstractProcessor {

    private SentimentAnalyzer analyzer;

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);
        analyzer = new SentimentAnalyzer();
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        TimestampedEntry<String, String> entry = (TimestampedEntry<String, String>) item;

        String coinType = entry.getKey();
        String tweetJson = entry.getValue();

        System.out.println(tweetJson);
        JSONObject jsonObject = new JSONObject(tweetJson);
        String tweetText = jsonObject.getString("text");

        List<CoreMap> annotations = analyzer.getAnnotations(tweetText);

        double sentimentType = analyzer.getSentimentClass(annotations);
        double sentimentScore = analyzer.getScore(annotations, sentimentType);

        double score=sentimentType * sentimentScore;

        TimestampedEntry<String, Double> result = new TimestampedEntry<>(entry.getTimestamp(), coinType, score);
        return tryEmit(result);
    }
}
