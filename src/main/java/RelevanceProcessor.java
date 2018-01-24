import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.AbstractProcessor;

import javax.annotation.Nonnull;
import java.util.List;

public class RelevanceProcessor extends AbstractProcessor{
    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        String tweetJson = (String)item;
        //TODO find relevancey

        List<String> coinTypeList;

        for (String coin : coinTypeList) {
            tryEmit(Util.entry(tweetJson, coin));
        }
        return true;
    }
}
