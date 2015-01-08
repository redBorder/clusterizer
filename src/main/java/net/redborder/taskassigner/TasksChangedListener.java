package net.redborder.taskassigner;

import java.util.List;
import java.util.Map;

/**
 * Created by crodriguez on 9/12/14.
 */
public interface TasksChangedListener {
    public void updateTasks(List<Task> tasks);
}
