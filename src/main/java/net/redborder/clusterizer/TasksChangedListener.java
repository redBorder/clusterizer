package net.redborder.clusterizer;

import java.util.List;

/**
 * Created by crodriguez on 9/12/14.
 */
public interface TasksChangedListener {
    public void updateTasks(List<Task> tasks);
}
