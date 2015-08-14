package net.redborder.clusterizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by crodriguez on 9/12/14.
 */
public abstract class TasksHandler {

    List<TasksChangedListener> listenersTask;
    List<NotifyListener> listenersNotify;
    List<Task> tasks;
    Random random;


    public TasksHandler() {
        listenersTask = new ArrayList<>();
        listenersNotify = new ArrayList<>();
        random = new Random();
    }

    // Adds a listener that will be notified when the tasks of
    // this instance change.
    public void addListener(TasksChangedListener listener) {
        listenersTask.add(listener);
    }

    public void addListener(NotifyListener listener) {
        listenersNotify.add(listener);
    }

    public List<Task> getTasks() {
        return tasks;
    }


    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    public void mustWork() {
        for (NotifyListener listener : listenersNotify)
            listener.time2Work();
    }


    public void setAssignedTasks(List<Task> tasks) {
        String taskStr = "[ ";

        for (Task task : tasks) {
            taskStr = taskStr + task.asMap() + " ";
        }
        taskStr = taskStr + "]";

        System.out.println("Updated node tasks: " + taskStr);

        for (TasksChangedListener listener : listenersTask) {
            listener.updateTasks(tasks);
        }
    }

    public abstract void wakeup();

    public abstract void goToWork(boolean waitWorkers);

    public abstract Integer numWorkers();

    public abstract void end();

    public abstract void reload();
}
