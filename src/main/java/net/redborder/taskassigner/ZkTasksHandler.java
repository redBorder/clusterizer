package net.redborder.taskassigner;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.jackson.map.ObjectMapper;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by crodriguez on 9/12/14.
 */
public class ZkTasksHandler extends TasksHandler {
    RetryPolicy retryPolicy;
    CuratorFramework client;
    LeaderLatch latch;
    InterProcessSemaphoreMutex mutex;
    ObjectMapper mapper;
    String hostname;
    String zkHosts;

    WorkersWatcher workersWatcher;
    TasksWatcher tasksWatcher;
    TasksAssigner tasksAssigner;

    private static final String TASKS_ZK_PATH = "/rBtask";
    private String zk_path;

    public ZkTasksHandler(String zkHosts) {
        this(zkHosts, TASKS_ZK_PATH);
    }

    public ZkTasksHandler(String zkHosts, String task_zk_path) {
        retryPolicy = new ExponentialBackoffRetry(1000, 3);
        mapper = new ObjectMapper();
        listeners = new ArrayList<>();
        workersWatcher = new WorkersWatcher();
        tasksWatcher = new TasksWatcher();
        tasksAssigner = new TasksAssigner();
        client = null;
        this.zkHosts = zkHosts;
        this.zk_path = task_zk_path;

        tasksAssigner.init();
        tasksAssigner.start();

        try {
            hostname = InetAddress.getLocalHost().getHostName();
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void end() {
        if (client != null) {
            try {
                latch.close();
                tasksAssigner.close();
                tasksAssigner.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
            client.close();
            client = null;
        }
    }

    @Override
    public void reload() {
        try {
            latch.close();
            client.close();
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init() throws Exception {
        // Connect to ZK
        client = CuratorFrameworkFactory.newClient(zkHosts, retryPolicy);
        client.start();

        // Create the leader latch and the barrier
        latch = new LeaderLatch(client, zk_path + "/latch");
        mutex = new InterProcessSemaphoreMutex(client, zk_path + "/mutex");

        // First, if the barrier is up, we must wait because
        // a leader is currently setting up the tasks for other workers
        mutex.acquire();

        // Create nmsp section if it doesnt exists
        if (client.checkExists().forPath(zk_path) == null) {
            client.create().forPath(zk_path);
        }

        if (client.checkExists().forPath(zk_path + "/workers") == null) {
            client.create().forPath(zk_path + "/workers");
        }


        // Write myself to the workers register
        // Writing itself to the workers path will make the leader to assign this
        // instance a number of tasks to process
        if (client.checkExists().forPath(zk_path + "/workers/" + hostname) == null) {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/workers/" + hostname);
            client.getData().usingWatcher(tasksWatcher).forPath(zk_path + "/workers/" + hostname);
        } else {
            byte[] zkData = client.getData().usingWatcher(tasksWatcher).forPath(zk_path + "/workers/" + hostname);
            client.delete().forPath(zk_path + "/workers/" + hostname);
            System.out.println("Exists old state, I recovery and create again!");
            client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/workers/" + hostname, zkData);
            client.getData().usingWatcher(tasksWatcher).forPath(zk_path + "/workers/" + hostname);

            // Read data from ZK and assign those tasks
            List<Map<String, Object>> maps = (List<Map<String, Object>>) mapper.readValue(zkData, List.class);
            List<Task> tasks = new ArrayList<>();

            for(Map<String, Object> map : maps){
                MappedTask task = new MappedTask(map);
                tasks.add(task);
            }

            setAssignedTasks(tasks);
        }

        // Set a watch over workers nodes
        client.getChildren().usingWatcher(workersWatcher).forPath(zk_path + "/workers");
        mutex.release();

        // Start latch so a leader can be selected
        latch.start();

        // Leader latch listeners.
        // When a new leader is assigned (because the leader has fallen) then the task
        // must be assigned again.
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                System.out.println("Im leading now, so lets assign the tasks again");

                try {
                    tasksAssigner.assign();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void notLeader() {
                System.out.println("Im not the leader anymore");
            }
        });

    }

    private class TasksAssigner extends Thread {

        private boolean running;
        private Object monitor;

        public TasksAssigner() {
            monitor = new Object();
        }

        // This method reassign the tasks to the workers
        // This method should only be called if this instance is the leader
        private void assignTasks() throws Exception {
            // Raise the barrier so no one registers right now
            mutex.acquire();

            // Lets get the sensors and the workers available
            List<Task> tasks = getTasks();
            List<String> workers = client.getChildren().forPath(zk_path + "/workers");

            HashMap<String, Object> assignments = new HashMap<>();
            int workers_length = workers.size();
            int current = 0;

            for (Task task : tasks) {
                String worker = workers.get(current);
                List<Map<String, Object>> taskList = (List<Map<String, Object>>) assignments.get(worker);

                if (taskList == null) {
                    taskList = new ArrayList<>();
                    assignments.put(worker, taskList);
                }

                taskList.add(task.asMap());

                current++;
                if (current >= workers_length) {
                    current = 0;
                }
            }

            // Write the assigned sensors on ZK
            String task_assigned = "Assigning tasks: ";
            for (Map.Entry<String, Object> assignedTasks : assignments.entrySet()) {
                List<Map<String, Object>> task_list = (List<Map<String, Object>>) assignedTasks.getValue();
                String worker_name = assignedTasks.getKey();

                task_assigned += worker_name + " (" + task_list + ") ";
                client.setData().forPath(zk_path + "/workers/" + worker_name, mapper.writeValueAsBytes(task_list));
            }
            System.out.println(task_assigned);

            // Finishing, release the barrier
            mutex.release();
        }

        @Override
        public void run() {
            try {
                while (running) {
                    synchronized (monitor) {
                        monitor.wait();
                        if (running)
                            assignTasks();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void close() {
            running = false;
            monitorNotify();
        }

        public void init() {
            running = true;
        }

        public void assign() {
            monitorNotify();
        }

        private void monitorNotify(){
            synchronized (monitor) {
                monitor.notifyAll();
            }
        }
    }

    private class WorkersWatcher implements CuratorWatcher {

        @Override
        public void process(WatchedEvent watchedEvent) throws Exception {
            System.out.println(Thread.currentThread().getId() + " [WATCH] WorkersWatcher: " + watchedEvent);
            Watcher.Event.EventType type = watchedEvent.getType();

            if (type.equals(Watcher.Event.EventType.NodeChildrenChanged)) {
                // Someone just joined. Lets assign the tasks!
                System.out.print("Nodes changed!");
                if (latch.hasLeadership()) {
                    System.out.println(" Im leading, so lets reassign the tasks...");
                    tasksAssigner.assign();
                } else {
                    System.out.println(" But im not the leader, so lets ignore it.");
                }
            }

            if (client.getState().equals(CuratorFrameworkState.STARTED)) {
                client.getChildren().usingWatcher(workersWatcher).forPath(zk_path + "/workers");
            }
        }
    }

    private class TasksWatcher implements CuratorWatcher {

        @Override
        public void process(WatchedEvent watchedEvent) throws Exception {
            System.out.println(Thread.currentThread().getId() + " [WATCH] MyTasksWatcher: " + watchedEvent);
            Watcher.Event.EventType type = watchedEvent.getType();

            if (type.equals(Watcher.Event.EventType.NodeDataChanged)) {
                System.out.println("New tasks data!");
                // The leader just assigned us new tasks! Lets process them!
                byte[] data = client.getData().forPath(watchedEvent.getPath());
                List<Map<String, Object>> maps = (List<Map<String, Object>>) mapper.readValue(data, List.class);
                List<Task> tasks = new ArrayList<>();

                for(Map<String, Object> map : maps){
                    MappedTask task = new MappedTask();
                    task.initialize(map);
                    tasks.add(task);
                }
                setAssignedTasks(tasks);
            }

            if (client.getState().equals(CuratorFrameworkState.STARTED)) {
                client.getData().usingWatcher(tasksWatcher).forPath(zk_path + "/workers/" + hostname);
            }
        }
    }
}
