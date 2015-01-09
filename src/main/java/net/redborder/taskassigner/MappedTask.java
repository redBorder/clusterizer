package net.redborder.taskassigner;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by andresgomez on 8/1/15.
 */
public class MappedTask implements Task {
    Map<String, Object> map;

    public MappedTask() {
        map = new HashMap<>();
    }

    public MappedTask(Map<? extends String, ? extends Object> m){
        map = new HashMap<>();
        initialize(m);
    }

    @Override
    public void initialize(Map<? extends String, ? extends Object> m) {
        map.putAll(m);
    }

    public void setData(String dataId, Object dataValue) {
        map.put(dataId, dataValue);
    }

    public <T> T getData(String dataId) {
        T value = (T) map.get(dataId);
        return value;
    }

    @Override
    public Map<String, Object> asMap() {
        return map;
    }
}
