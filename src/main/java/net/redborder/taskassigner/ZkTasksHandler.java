package net.redborder.taskassigner;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.jackson.map.ObjectMapper;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

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
    NotifyWatcher notifyWatcher;

    Object notWorkers;

    Long job_id;

    private static final String TASKS_ZK_PATH = "/rBtask";
    private String zk_path;

    public ZkTasksHandler(String zkHosts) {
        this(zkHosts, TASKS_ZK_PATH + "/clusterizer");
    }

    public ZkTasksHandler(String zkHosts, String task_zk_path) {
        retryPolicy = new RetryNTimes(Integer.MAX_VALUE, 30000);
        mapper = new ObjectMapper();

        listenersTask = new ArrayList<>();
        listenersNotify = new ArrayList<>();
        random = new Random();
        job_id = 0L;

        notWorkers = new Object();

        workersWatcher = new WorkersWatcher();
        tasksWatcher = new TasksWatcher();
        notifyWatcher = new NotifyWatcher();

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
        System.out.println("Initialite ZkTasksHandler ....");
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

        mutex.release();

        initWorkers();
        initTasks();
        initNotifies();

        // Start latch so a leader can be selected
        latch.start();
        System.out.println("ZkTasksHandler done!");
    }

    private void initWorkers() throws Exception {
        System.out.print("Initialite ZkTasksHandler[Workers] .... ");
        mutex.acquire();

        if (client.checkExists().forPath(zk_path + "/workers") == null) {
            client.create().forPath(zk_path + "/workers");
        }

        // Write myself to the workers register
        // Writing itself to the workers path will make the leader to assign this
        // instance a number of tasks to process
        if (client.checkExists().forPath(zk_path + "/workers/" + hostname) == null) {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/workers/" + hostname);
        }

        // Set a watch over workers nodes
        client.getChildren().usingWatcher(workersWatcher).forPath(zk_path + "/workers");
        mutex.release();
        System.out.println(" Done!");
    }

    private void initTasks() throws Exception {
        System.out.print("Initialite ZkTasksHandler[Tasks] .... ");
        mutex.acquire();

        tasks = new ArrayList<>();

        if (client.checkExists().forPath(zk_path + "/tasks") == null) {
            client.create().forPath(zk_path + "/tasks");
        }

        // Write myself to the workers register
        // Writing itself to the workers path will make the leader to assign this
        // instance a number of tasks to process
        if (client.checkExists().forPath(zk_path + "/tasks/" + hostname) == null) {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/tasks/" + hostname);
            client.getData().usingWatcher(tasksWatcher).forPath(zk_path + "/tasks/" + hostname);
        } else {
            byte[] zkData = client.getData().usingWatcher(tasksWatcher).forPath(zk_path + "/tasks/" + hostname);
            client.delete().forPath(zk_path + "/tasks/" + hostname);
            System.out.println("Exists old state, I recovery and create again!");
            client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/tasks/" + hostname, zkData);
            client.getData().usingWatcher(tasksWatcher).forPath(zk_path + "/tasks/" + hostname);

            // Read data from ZK and assign those tasks
            List<Map<String, Object>> maps = (List<Map<String, Object>>) mapper.readValue(zkData, List.class);
            List<Task> tasks = new ArrayList<>();

            for (Map<String, Object> map : maps) {
                MappedTask task = new MappedTask(map);
                tasks.add(task);
            }

            setAssignedTasks(tasks);
        }

        mutex.release();

        // Leader latch listeners.
        // When a new leader is assigned (because the leader has fallen) then the task
        // must be assigned again.
        latch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                System.out.print("Im leading now, so lets assign the tasks again");

                try {
                    if (tasks.size() > 0) {
                        wakeup();
                        System.out.println("");
                    } else {
                        System.out.println(", but I haven't tasks to assign.");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void notLeader() {
                System.out.println("Im not the leader anymore");
            }
        });

        System.out.println(" Done!");
    }

    private void initNotifies() throws Exception {
        System.out.print("Initialite ZkTasksHandler[Notifies] .... ");
        mutex.acquire();

        if (client.checkExists().forPath(zk_path + "/notifies") == null) {
            client.create().forPath(zk_path + "/notifies");
        }

        // Write myself to the workers register
        // Writing itself to the workers path will make the leader to assign this
        // instance a number of tasks to process
        if (client.checkExists().forPath(zk_path + "/notifies/" + hostname) == null) {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/notifies/" + hostname);
            client.getData().usingWatcher(notifyWatcher).forPath(zk_path + "/notifies/" + hostname);
        } else {
            client.getData().usingWatcher(notifyWatcher).forPath(zk_path + "/notifies/" + hostname);
        }

        mutex.release();
        System.out.println(" Done! ");
    }

    @Override
    public void wakeup() {
        tasksAssigner.assign();
    }

    @Override
    public void goToWork(boolean waitWorkers) {
        try {
            tasksAssigner.go2Work(waitWorkers);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Integer numWorkers() {
        Integer num;
        try {
            num = client.getChildren().forPath(zk_path + "/workers").size();
        } catch (Exception e) {
            e.printStackTrace();
            num = 0;
        }
        return num;
    }

    public boolean isLeader() {
        return latch.hasLeadership();
    }

    private class TasksAssigner extends Thread {

        private boolean running;
        private Object monitor;

        public TasksAssigner() {
            monitor = new Object();
        }

        public void go2Work(boolean waitWorkers) throws Exception {
            mutex.acquire();
            System.out.print("Is time to work! ");
            List<String> workers = client.getChildren().forPath(zk_path + "/notifies");
            boolean someone = false;

            if (waitWorkers) {
                if (workers.size() == 0) {
                    System.out.println("Nobody wants work, I'm waiting");
                    synchronized (notWorkers) {
                        notWorkers.wait();
                    }
                }
                workers = client.getChildren().forPath(zk_path + "/notifies");
                someone = true;
            } else {
                if (workers.size() == 0) {
                    System.out.println("Nobody wants work, I will forget this job");
                }else{
                    someone = true;
                }
            }

            if (someone) {
                Random r = new Random();
                String worker = workers.get(r.nextInt(workers.size()));
                System.out.println(" --> Today must work [" + worker + "]");
                System.out.println("SIZE: " + workers.size());
                client.delete().forPath(zk_path + "/notifies/" + worker);
            }

            mutex.release();
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
                if (workers.size() > 0) {
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
            }

            while (assignments.size() < workers_length) {
                String worker = workers.get(current);
                List<Map<String, Object>> taskList = new ArrayList<>();
                assignments.put(worker, taskList);
                current++;
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

        private void monitorNotify() {
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
            System.out.println(Thread.currentThread().getId() + " [WATCH] TasksWatcher: " + watchedEvent);
            Watcher.Event.EventType type = watchedEvent.getType();

            if (type.equals(Watcher.Event.EventType.NodeDataChanged)) {
                System.out.println("New tasks data!");
                // The leader just assigned us new tasks! Lets process them!
                byte[] data = client.getData().forPath(watchedEvent.getPath());
                List<Map<String, Object>> maps = (List<Map<String, Object>>) mapper.readValue(data, List.class);
                List<Task> tasks = new ArrayList<>();

                for (Map<String, Object> map : maps) {
                    MappedTask task = new MappedTask();
                    task.initialize(map);
                    tasks.add(task);
                }
                setAssignedTasks(tasks);
            }

            if (client.getState().equals(CuratorFrameworkState.STARTED)) {
                client.getData().usingWatcher(tasksWatcher).forPath(zk_path + "/tasks/" + hostname);
            }
        }
    }

    private class NotifyWatcher implements CuratorWatcher {

        @Override
        public void process(WatchedEvent watchedEvent) throws Exception {
            System.out.println(Thread.currentThread().getId() + " [WATCH] NotifyWacther: " + watchedEvent);
            Watcher.Event.EventType type = watchedEvent.getType();

            System.out.println(watchedEvent.getType().getIntValue() + "      " + Watcher.Event.EventType.NodeDeleted.getIntValue());
            if (type.equals(Watcher.Event.EventType.NodeDeleted)) {
                System.out.println("New notify! Running job id: " + job_id);

                mustWork();

                if (client.getState().equals(CuratorFrameworkState.STARTED)) {
                    client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/notifies/" + hostname, String.valueOf("Job ID: " + job_id).getBytes());
                    client.getData().usingWatcher(notifyWatcher).forPath(zk_path + "/notifies/" + hostname);
                }

                job_id++;
                synchronized (notWorkers) {
                    notWorkers.notifyAll();
                }
            }


        }
    }
}
