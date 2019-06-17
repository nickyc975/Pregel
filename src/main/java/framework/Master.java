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
import java.util.function.Consumer;
import java.util.function.Function;

import framework.utils.Tuple2;
import framework.utils.Tuple3;

public class Master<V, E, M> implements Context<V, E, M> {
    private State state;

    /**
     * Current superstep.
     */
    private long superstep = 0;

    /**
     * Number of graph partitions.
     */
    private int numPartitions;

    /**
     * Number of active workers.
     */
    private long numActiveWorkers;

    /**
     * Workers registered on this master.
     */
    private final Map<Long, Worker<V, E, M>> workers;

    /**
     * The root output path. All outputs will be under this path.
     */
    private Path workPath = null;

    /**
     * The directory of saving edges partitions.
     */
    private Path edgesPartsPath = null;

    /**
     * The directory of saving vertices partitions.
     */
    private Path verticesPartsPath = null;

    /**
     * User defined function to parse edges from strings.
     * 
     * The param of the function is one line from the input file.
     * The returned value of the function is a 3 elements tuple with the first 
     * element as the source of the edge, the second element as the target of 
     * the edge and the third element as the user defined properties of the edge.
     */
    private Function<String, Tuple3<Long, Long, E>> edgeParser = null;

    /**
     * User defined function to parse vertices from strings.
     * 
     * The param of the function is one line from the input file.
     * The returned value of the function is a 2 elements tuple with the first 
     * element as the the id of the vertex and the second value as user defined 
     * properties of the vertex.
     */
    private Function<String, Tuple2<Long, V>> vertexParser = null;

    /**
     * User defined computing function that do the actual computing job.
     * 
     * The param of the function is one of the vertices.
     */
    private Consumer<Vertex<V, E, M>> computeFunction = null;

    /**
     * User defined message combiner.
     */
    private Combiner<M> combiner = null;

    /**
     * Aggregators.
     */
    private Map<String, Aggregator<Vertex<V, E, M>, ?>> aggregators = null;

    /**
     * Aggregated values.
     */
    private Map<String, ?> aggregatedValues = null;

    public Master() {
        workers = new HashMap<>();
        aggregators = new HashMap<>();
        aggregatedValues = new HashMap<>();
        this.addAggregator("numVertices", new NumVerticesAggregator());
        this.addAggregator("numEdges", new NumEdgesAggregator());
        this.state = State.Initialized;
    }

    @Override
    public State state() {
        return this.state;
    }

    @Override
    public long superstep() {
        return this.superstep;
    }

    /**
     * Get the total number of vertices on all workers.
     * 
     * @return total number of vertices.
     */
    @Override
    public long getNumVertices() {
        return this.getAggregatedValue("numVertices");
    }

    /**
     * Get the total number of edges on all vertices.
     * 
     * @return total number of edges.
     */
    @Override
    public long getNumEdges() {
        return this.getAggregatedValue("numEdges");
    }

    @Override
    public void addVertex(long id) {
        Worker<V, E, M> worker = workers.get(id % numPartitions);
        worker.getOrCreateVertex(id);
    }

    @Override
    public void sendMessage(Message<M> message) {
        Worker<V, E, M> worker = workers.get(message.getReceiver() % numPartitions);
        worker.receiveMessage(message);
    }

    /**
     * Get an iterator of all vertices.
     * 
     * @return an iterator of all vertices.
     */
    public Iterator<Vertex<V, E, M>> getVertices() {
        return new VertexIterator(this);
    }

    public Master<V, E, M> setEdgeParser(Function<String, Tuple3<Long, Long, E>> edgeParser) {
        this.edgeParser = edgeParser;
        return this;
    }

    public Master<V, E, M> setVertexParser(Function<String, Tuple2<Long, V>> vertexParser) {
        this.vertexParser = vertexParser;
        return this;
    }

    public Master<V, E, M> setComputeFunction(Consumer<Vertex<V, E, M>> computeFunction) {
        this.computeFunction = computeFunction;
        return this;
    }

    /**
     * Set and create working directory.
     * 
     * @param path path of working directory.
     * @return the master itself.
     */
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

    public Master<V, E, M> setCombiner(Combiner<M> combiner) {
        this.combiner = combiner;
        return this;
    }

    public Master<V, E, M> addAggregator(String valueName, Aggregator<Vertex<V, E, M>, ?> aggregator) {
        if (!this.aggregators.containsKey(valueName)) {
            this.aggregators.put(valueName, aggregator);
        } else {
            System.out.println("Aggregator with name " + valueName + "has existed! It will not be added.");
        }
        return this;
    }

    Worker<V, E, M> getWorkerFromWorkerId(long workerId) {
        return workers.get(workerId);
    }

    Worker<V, E, M> getWorkerFromVertexId(long vertexId) {
        return workers.get(getWorkerIdFromVertexId(vertexId));
    }

    long getWorkerIdFromVertexId(long vertexId) {
        return vertexId % numPartitions;
    }

    /**
     * Partition the given inputFile to the outputDir with partition index 
     * calculating function calIndexFunc.
     * 
     * @param inputFile the file to be partitioned.
     * @param outputDir the output directory.
     * @param calIndexFunc the index calculating function, the param is one 
     * line from the input file, the returned value is the partition index 
     * of that line.
     */
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

    /**
     * Read and partition the edges file.
     * 
     * @param path file path.
     */
    public void loadEdges(String path) {
        try {
            edgesPartsPath = workPath.resolve("graph").resolve("parts");
            Files.createDirectories(edgesPartsPath);
            partition(path, edgesPartsPath.toString(), 
                s -> (int) (edgeParser.apply(s)._1 % numPartitions)
            );
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Read and partition the vertices file.
     * 
     * @param path file path.
     */
    public void loadVertices(String path) {
        try {
            verticesPartsPath = workPath.resolve("vertices").resolve("parts");
            Files.createDirectories(verticesPartsPath);
            partition(path, verticesPartsPath.toString(), 
                s -> (int) (vertexParser.apply(s)._1 % numPartitions)
            );
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Mark worker with workerId as done.
     * 
     * A worker is done means all vertices on that worker is inactive in a superstep.
     * 
     * @param workerId worker id.
     */
    @Override
    public synchronized void markAsDone(long workerId) {
        numActiveWorkers--;
    }

    /**
     * Aggregate values from workers.
     * 
     * @param valueName name of value.
     * @param value value
     */
    @SuppressWarnings("unchecked")
    synchronized <A> void aggregate(String valueName, A value) {
        Map<String, A> values = (Map<String, A>)aggregatedValues;
        A initial = values.get(valueName);
        if (initial == null) {
            initial = value;
        } else {
            Aggregator<Vertex<V, E, M>, A> aggregator = 
                    (Aggregator<Vertex<V, E, M>, A>)aggregators.get(valueName);
            initial = aggregator.aggregate(initial, value);
        }
        values.put(valueName, initial);
    }

    /**
     * Get the aggregated value with given name.
     * 
     * @return the aggregated value with given name.
     */
    @SuppressWarnings("unchecked")
    public <A> A getAggregatedValue(String valueName) {
        return (A) aggregatedValues.get(valueName);
    }

    /**
     * Update global state.
     * 
     * Initialized ---> Loaded ---> Cleaned ---> Computed
     *                                 ^            |
     *                                 |            |
     *                                  ------------
     */
    private void updateState() {
        switch (this.state) {
            case Initialized:
                this.state = State.Loaded;
                break;
            case Loaded:
                this.state = State.Cleaned;
                break;
            case Cleaned:
                this.state = State.Computed;
                aggregatedValues.clear();
                System.out.println("Superstep: " + superstep);
                for (Worker<V, E, M> worker : workers.values()) {
                    System.out.println(String.format(
                        "worker id: %d, number of vertices: %d, number of edges: %d, " + 
                        "message sent: %d, message received: %d, time cost: %d ms", 
                        worker.id(), worker.getLocalNumVertices(), worker.getLocalNumEdges(), 
                        worker.getNumMessageSent(), worker.getNumMessageReceived(), worker.getTimeCost()
                    ));

                    for (String valueName : aggregators.keySet()) {
                        aggregate(valueName, worker.report(valueName));
                    }
                }
                superstep++;
                break;
            case Computed:
                this.state = State.Cleaned;
                break;
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * Start calculating.
     */
    public void run() {
        for (int i = 0; i < numPartitions; i++) {
            Worker<V, E, M> worker = new Worker<>(i, this);
            worker.setEdgeParser(edgeParser)
                  .setVertexParser(vertexParser)
                  .setComputeFunction(computeFunction)
                  .setCombiner(combiner)
                  .setAggregators(aggregators);
            if (edgesPartsPath != null) {
                worker.setEdgesPath(edgesPartsPath.resolve(i + ".txt").toString());
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

            for (Worker<V, E, M> worker: workers.values()) {
                Thread thread = new Thread(worker);
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
            updateState();
        }
    }

    /**
     * An implementation of Iterator interface. Used to iterate all vertices.
     */
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

    /**
     * Use this to monitor the number of vertices.
     */
    private class NumVerticesAggregator implements Aggregator<Vertex<V, E, M>, Long> {
        @Override
        public Long report(Vertex<V, E, M> vertex) {
            return 1L;
        }

        @Override
        public Long aggregate(Long a, Long b) {
            return a + b;
        }
    }

    /**
     * Use this to monitor the number of edges.
     */
    private class NumEdgesAggregator implements Aggregator<Vertex<V, E, M>, Long> {
        @Override
        public Long report(Vertex<V, E, M> vertex) {
            return (long) vertex.getOuterEdges().size();
        }

        @Override
        public Long aggregate(Long a, Long b) {
            return a + b;
        }
    }
}
