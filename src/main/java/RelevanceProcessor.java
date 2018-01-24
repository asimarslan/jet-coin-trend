import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.TimestampedEntry;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

public class RelevanceProcessor extends AbstractProcessor{

    private Map<String, List<String>> coinMap;

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        super.init(context);
        CoinTypes types = new CoinTypes();

        coinMap = types.getCoinTypeMap();
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        String content = (String)item;

        for (String cointype: coinMap.keySet()) {
            for (String keyword : coinMap.get(cointype)) {
                if (content.contains(keyword)) {
                    TimestampedEntry<String, String> entry = new TimestampedEntry<>(1, cointype, content);
                    tryEmit(entry);
                }
            }
        }
        return true;
    }
}
