package net.redborder.taskassigner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by crodriguez on 9/12/14.
 */
public abstract class TasksHandler {

    List<TasksChangedListener> listeners;
    List<Task> tasks;
    List<Task> assignedTasks;


    public TasksHandler() {
        listeners = new ArrayList<>();
    }

    // Adds a listener that will be notified when the tasks of
    // this instance change.
    public void addListener(TasksChangedListener listener) {
        listeners.add(listener);
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public List<Task> getAssignedTasks() {
        return assignedTasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    public void setAssignedTasks(List<Task> tasks) {
        String taskStr = "[ ";

        for(Task task : tasks){
            taskStr = taskStr + task.asMap() + " ";
        }
        taskStr = taskStr + "]";

        System.out.println("Updated node tasks: " + taskStr);

        this.assignedTasks = tasks;

        for (TasksChangedListener listener : listeners) {
            listener.updateTasks(tasks);
        }
    }

    public abstract void wakeup();

    public abstract void end();

    public abstract void reload();
}
