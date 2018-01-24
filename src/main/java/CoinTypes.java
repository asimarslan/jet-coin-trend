import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoinTypes {

    public Map<String, List<String>> getCoinTypeMap() {
        Map<String, List<String>> map = new HashMap<>();
        List<String> list =new ArrayList<>();
        list.add("btc");
        list.add("bitcoin");
        map.put("btc", list);
        return map;
    }
}
