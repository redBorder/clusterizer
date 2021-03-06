package net.redborder.clusterizer;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.InetAddress;
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
    RecoverFromFailed recoverFromFailed;

    private static final String TASKS_ZK_PATH = "";
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
        this.zk_path = task_zk_path + "/clusterizer";

        tasksAssigner.init();
        tasksAssigner.start();

        try {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setRecoverFromFailed(RecoverFromFailed recoverFromFailed){
        this.recoverFromFailed = recoverFromFailed;
    }

    @Override
    public void end() {
        if (client.getState().equals(CuratorFrameworkState.STARTED)) {
            try {
                latch.close();
                tasksAssigner.close();
                tasksAssigner.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
            client.close();
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

    private void prepare() throws Exception {
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

        System.out.println("ZkTasksHandler done!");
    }
    private void init() throws Exception {
        System.out.println("Initialite ZkTasksHandler ....");
        // Connect to ZK
        client = CuratorFrameworkFactory.newClient(zkHosts, 5000, 60000, retryPolicy);

        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            @Override
            public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                System.out.println("Changed state to: " + connectionState.name());
                try {
                    if (connectionState.equals(ConnectionState.CONNECTED) || connectionState.equals(ConnectionState.RECONNECTED)) {

                        try {
                            prepare();
                        } catch (Exception ex) {
                            System.out.println("Error working with zookeeper, shutting down ...");
                            end();
                        }

                        if (connectionState.equals(ConnectionState.RECONNECTED)) {
                            List<Task> tasks = recoverFromFailed.recover();
                            setTasks(tasks);
                            wakeup();
                        }
                    }
                }catch (Exception ex){
                    ex.printStackTrace();
                }
            }
        });

        // Create the leader latch and the barrier
        latch = new LeaderLatch(client, zk_path + "/latch");

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

        mutex = new InterProcessSemaphoreMutex(client, zk_path + "/mutex");

        client.start();
        latch.start();
    }

    private void initWorkers() throws Exception {
        System.out.print("Initialite ZkTasksHandler[Workers] .... ");

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
        System.out.println(" Done!");
    }

    private void initTasks() throws Exception {
        System.out.print("Initialite ZkTasksHandler[Tasks] .... ");

        if (client.checkExists().forPath(zk_path + "/tasks") == null) {
            client.create().forPath(zk_path + "/tasks");
        }

        // Write myself to the workers register
        // Writing itself to the workers path will make the leader to assign this
        // instance a number of tasks to process
        if (client.checkExists().forPath(zk_path + "/tasks/" + hostname) == null) {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/tasks/" + hostname, "[]".getBytes());
            client.getData().usingWatcher(tasksWatcher).forPath(zk_path + "/tasks/" + hostname);
        } else {
            //byte[] zkData = client.getData().forPath(zk_path + "/tasks/" + hostname);
            client.delete().forPath(zk_path + "/tasks/" + hostname);
            //System.out.println("Exists old state, I try recovery and create again!");

            // Read data from ZK and assign those tasks
            List<Map<String, Object>> maps = null;

            try {
                //maps = (List<Map<String, Object>>) mapper.readValue(zkData, List.class);

                //Not recover!!!!!
                client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/tasks/" + hostname, "[]".getBytes());
            } catch (IOException e) {
                System.out.println("I can't recover old state, remove it and start again!");
                client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/tasks/" + hostname, "[]".getBytes());
            }

            client.getData().usingWatcher(tasksWatcher).forPath(zk_path + "/tasks/" + hostname);
/*
            if (maps != null) {
                List<Task> tasks = new ArrayList<>();

                for (Map<String, Object> map : maps) {
                    MappedTask task = new MappedTask(map);
                    tasks.add(task);
                }

                setAssignedTasks(tasks);
            }
            */
        }

        System.out.println(" Done!");
    }

    private void initNotifies() throws Exception {
        System.out.print("Initialite ZkTasksHandler[Notifies] .... ");

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

        System.out.println(" Done! ");
    }

    @Override
    public void wakeup() {
        if(isLeader()) {
            tasksAssigner.assign();
        }
    }

    @Override
    public void goToWork(boolean waitWorkers) {
        try {
            tasksAssigner.go2Work(waitWorkers);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CuratorFramework getCuratorClient(){
        return client;
    }

    @Override
    public Integer numWorkers() {
        Integer num = 1;
        try {
            if (client.getState().equals(CuratorFrameworkState.STARTED)) {
                num = client.getChildren().forPath(zk_path + "/workers").size();
            }
        } catch (Exception e) {
            e.printStackTrace();
            num = 1;
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
                } else {
                    someone = true;
                }
            }

            if (someone) {
                Random r = new Random();
                String worker = workers.get(r.nextInt(workers.size()));
                System.out.println(" --> Today must work [" + worker + "]");
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
            List<String> workers = client.getChildren().forPath(zk_path + "/tasks");

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
                client.setData().forPath(zk_path + "/tasks/" + worker_name, mapper.writeValueAsBytes(task_list));
            }
            System.out.println(task_assigned);

            // Finishing, release the barrier
            tasks.clear();
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

            if (type.equals(Watcher.Event.EventType.NodeDeleted)) {
                System.out.println("New notify! Running job id: " + job_id);

                mustWork();

                if (client.getState().equals(CuratorFrameworkState.STARTED)) {
                    if (client.checkExists().forPath(zk_path + "/notifies/" + hostname) == null) {
                        client.create().withMode(CreateMode.EPHEMERAL).forPath(zk_path + "/notifies/" + hostname, String.valueOf("Job ID: " + job_id).getBytes());
                        client.getData().usingWatcher(notifyWatcher).forPath(zk_path + "/notifies/" + hostname);
                    }
                }

                job_id++;
                synchronized (notWorkers) {
                    notWorkers.notifyAll();
                }
            }


        }
    }
}
