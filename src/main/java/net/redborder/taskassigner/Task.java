package net.redborder.taskassigner;

import java.util.Map;

/**
 * Created by andresgomez on 8/1/15.
 */
public interface Task {
    public Map<String, Object> asMap();
    public void initialize(Map<? extends String, ? extends Object> m);
}
