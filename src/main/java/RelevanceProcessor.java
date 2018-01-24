import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.TimestampedEntry;

import javax.annotation.Nonnull;

public class RelevanceProcessor extends AbstractProcessor{


    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        String content = (String)item;

        for (String cointype: CoinDefs.coinMap.keySet()) {
            for (String keyword : CoinDefs.coinMap.get(cointype)) {
                if (content.contains(keyword)) {
                    TimestampedEntry<String, String> entry = new TimestampedEntry<>(1, cointype, content);
                    tryEmit(entry);
                }
            }
        }
        return true;
    }
}
