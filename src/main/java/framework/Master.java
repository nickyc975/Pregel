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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;

import framework.api.VertexValue;
import framework.api.EdgeValue;
import framework.utils.Combiner;
import framework.utils.Aggregator;

public class Master<V extends VertexValue, E extends EdgeValue, M> {
    /**
     * Current superstep.
     */
    private long superstep = 0;

    private int numPartitions;

    private long numActiveWorkers;

    /**
     * Workers registered on this master.
     */
    private final Map<Long, Worker<V, E, M>> workers;

    private Path workPath = null;

    private Path graphPartsPath = null;

    private Path verticesPartsPath = null;

    private Function<String, E> edgeParser = null;

    private Function<String, V> vertexParser = null;

    private Consumer<Vertex<V, E, M>> computeFunction = null;

    public Master() {
        workers = new HashMap<>();
    }

    long getSuperstep() {
        return this.superstep;
    }

    long getNumVertices() {
        long sum = 0;
        for (Worker<V, E, M> worker : workers.values()) {
            sum += worker.getNumVertices();
        }
        return sum;
    }

    public Master<V, E, M> setEdgeParser(Function<String, E> edgeParser) {
        this.edgeParser = edgeParser;
        return this;
    }

    public Master<V, E, M> setVertexParser(Function<String, V> vertexParser) {
        this.vertexParser = vertexParser;
        return this;
    }

    public Master<V, E, M> setComputeFunction(Consumer<Vertex<V, E, M>> computeFunction) {
        this.computeFunction = computeFunction;
        return this;
    }

    public Master<V, E, M> setWorkPath(String path) {
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

    public Master<V, E, M> setNumPartitions(int numPartitions) {
        this.numPartitions = numPartitions;
        return this;
    }

    public Master<V, E, M> setCombiner(Combiner combiner) {
        return this;
    }

    public Master<V, E, M> setAggregator(Aggregator aggregator) {
        return this;
    }

    public Worker<V, E, M> getWorkerFromWorkerId(long workerId) {
        return workers.get(workerId);
    }

    public Worker<V, E, M> getWorkerFromVertexId(long vertexId) {
        return workers.get(getWorkerIdFromVertexId(vertexId));
    }

    public long getWorkerIdFromVertexId(long vertexId) {
        return vertexId % numPartitions;
    }

    private void partition(String inputFile, String outputDir, Function<String, Integer> calIndexFunc) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        BufferedWriter[] writers = new BufferedWriter[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            String partPath = outputDir + "/" + i + ".txt";
            writers[i] = new BufferedWriter(new FileWriter(partPath));
        }

        String line = reader.readLine();
        while (line != null) {
            int index = calIndexFunc.apply(line);
            writers[index].write(line);
            writers[index].newLine();
            line = reader.readLine();
        }
        for (BufferedWriter writer : writers) {
            writer.close();
        }
        reader.close();
    }

    public void loadEdges(String path) {
        try {
            graphPartsPath = workPath.resolve("graph").resolve("parts");
            Files.createDirectories(graphPartsPath);
            partition(path, graphPartsPath.toString(), 
                s -> (int) edgeParser.apply(s).source() % numPartitions
            );
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void loadVertices(String path) {
        try {
            verticesPartsPath = workPath.resolve("vertices").resolve("parts");
            Files.createDirectories(verticesPartsPath);
            partition(path, verticesPartsPath.toString(), 
                s -> (int) vertexParser.apply(s).id() % numPartitions
            );
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public Iterator<Vertex<V, E, M>> getVertices() {
        return new VertexIterator(this);
    }

    synchronized void setDone(long workerId) {
        numActiveWorkers--;
    }

    public void run() {
        for (int i = 0; i < numPartitions; i++) {
            Worker<V, E, M> worker = new Worker<>(i, this);
            worker.setEdgeParser(edgeParser)
                  .setVertexParser(vertexParser)
                  .setComputeFunction(computeFunction);
            if (graphPartsPath != null) {
                worker.setGraphPath(graphPartsPath.resolve(i + ".txt").toString());
            }
            if (verticesPartsPath != null) {
                worker.setVerticesPath(verticesPartsPath.resolve(i + ".txt").toString());
            }
            workers.put((long) i, worker);
        }

        numActiveWorkers = workers.size();
        List<Thread> threads = new ArrayList<>();
        while (numActiveWorkers > 0) {
            numActiveWorkers = workers.size();
            for (Entry<Long, Worker<V, E, M>> entry: workers.entrySet()) {
                Thread thread = new Thread(entry.getValue());
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException ignored) {

                }
            }
            threads.clear();
            System.out.println("Superstep: " + superstep);
            superstep++;
        }
    }

    private class VertexIterator implements Iterator<Vertex<V, E, M>> {
        private Iterator<Worker<V, E, M>> workers = null;

        private Iterator<Vertex<V, E, M>> vertices = null;

        VertexIterator(Master<V, E, M> master) {
            this.workers = master.workers.values().iterator();
            if (this.workers.hasNext()) {
                this.vertices = this.workers.next().getVertices();
            }
        }

        @Override
        public boolean hasNext() {
            return vertices != null && (vertices.hasNext() || workers.hasNext());
        }

        @Override
        public Vertex<V, E, M> next() {
            if (vertices != null) {
                if (vertices.hasNext()) {
                    return vertices.next();
                } else if (workers.hasNext()) {
                    vertices = workers.next().getVertices();
                    return vertices.next();
                } else {
                    return null;
                }
            }
            return null;
        }
    }
}
