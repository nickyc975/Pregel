package framework;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Master {
    /**
     * Current superstep.
     */
    private long superstep = 0;

    private long numActiveWorkers;

    /**
     * Workers registered on this master.
     */
    private final Map<Long, Worker> workers;

    private Class<Vertex> vertexClass;

    private Class<Edge> edgeClass;

    private Path workPath;

    private Path graphPartsPath = null;

    private Path verticesPartsPath = null;

    private int numPartitions;

    public Master() {
        workers = new HashMap<>();
    }

    long getSuperstep() {
        return this.superstep;
    }

    public Master setVertexClass(Class<Vertex> vertexClass) {
        this.vertexClass = vertexClass;
        return this;
    }

    public Master setEdgeClass(Class<Edge> edgeClass) {
        this.edgeClass = edgeClass;
        return this;
    }

    public Master setWorkPath(String path) {
        this.workPath = FileSystems.getDefault().getPath(path);
        if (Files.exists(this.workPath)) {
            System.out.println("File \"" + this.workPath + "\" already exists!");
            System.exit(-1);
        }
        
        try {
            Files.createDirectories(workPath);
            
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return this;
    }

    public Master setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
        return this;
    }

    public Master setCombiner(Combiner combiner) {
        return this;
    }

    public Master setAggregator(Aggregator aggregator) {
        return this;
    }

    public Worker getWorkerFromWorkerId(long workerId) {
        return workers.get(workerId);
    }

    public Worker getWorkerFromVertexId(long vertexId) {
        return workers.get(getWorkerIdFromVertexId(vertexId));
    }

    public long getWorkerIdFromVertexId(long vertexId) {
        return vertexId % numPartitions;
    }

    public void loadGraph(String path) {
        try {
            graphPartsPath = workPath.resolve("graph").resolve("parts");
            Files.createDirectories(graphPartsPath);
            BufferedReader reader = new BufferedReader(new FileReader(path));
            BufferedWriter[] writers = new BufferedWriter[numPartitions];
            for (int i = 0; i < numPartitions; i++) {
                String partPath = graphPartsPath.resolve(i + ".txt").toString();
                writers[i] = new BufferedWriter(new FileWriter(partPath));
            }

            String line = reader.readLine();
            while (line != null) {
                String[] parts = line.split(" ", 2);
                int index = (int) Long.parseLong(parts[0]) % numPartitions;
                writers[index].write(line);
                writers[index].newLine();
                line = reader.readLine();
            }
            for (BufferedWriter writer : writers) {
                writer.close();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void loadVertexProperties(String path) {
        try {
            verticesPartsPath = workPath.resolve("vertices").resolve("parts");
            Files.createDirectories(verticesPartsPath);
            BufferedReader reader = new BufferedReader(new FileReader(path));
            BufferedWriter[] writers = new BufferedWriter[numPartitions];
            for (int i = 0; i < numPartitions; i++) {
                String partPath = verticesPartsPath.resolve(i + ".txt").toString();
                writers[i] = new BufferedWriter(new FileWriter(partPath));
            }

            String line = reader.readLine();
            while (line != null) {
                String[] parts = line.split(" ", 2);
                int index = (int) Long.parseLong(parts[0]) % numPartitions;
                writers[index].write(line);
                writers[index].newLine();
                line = reader.readLine();
            }
            for (BufferedWriter writer : writers) {
                writer.close();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    synchronized void setDone(long workerId) {
        numActiveWorkers--;
    }

    public void run() {
        for (int i = 0; i < numPartitions; i++) {
            Worker worker = new Worker(i, this);
            worker.setVertexClass(vertexClass).setEdgeClass(edgeClass);
            if (graphPartsPath != null) {
                worker.setGraphPath(graphPartsPath.resolve(i + ".txt").toString());
            }
            if (verticesPartsPath != null) {
                worker.setVerticesPath(verticesPartsPath.resolve(i + ".txt").toString());
            }
            workers.put((long) i, worker);
        }

        List<Thread> threads = new ArrayList<>();
        while (numActiveWorkers > 0) {
            numActiveWorkers = workers.size();
            for (Entry<Long, Worker> entry: workers.entrySet()) {
                Thread thread = new Thread(entry.getValue());
                thread.start();
            }
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException ignored) {

                }
            }
            threads.clear();
        }
    }
}
