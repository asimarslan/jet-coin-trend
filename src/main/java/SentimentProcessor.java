import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;
import java.util.Map;

public class SentimentProcessor extends AbstractProcessor {
    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        Map.Entry<String, String> entry = (Map.Entry<String, String>) item;
        String coinType = entry.getValue();

        double score=0.0; //calculateCoinScore(entry)
        return tryEmit(Util.entry(coinType, score));
    }
}
