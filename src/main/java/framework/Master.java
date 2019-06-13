package framework;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Master implements Runnable {
    /**
     * Current superstep.
     */
    private long superstep = 0;

    /**
     * Workers registered on this master.
     */
    private final Map<Long, Worker> workers;

    private Class<Vertex> vertexClass;

    private Class<Edge> edgeClass;

    private String dataPath;

    private int numPartitions;

    private List<String> partitions;

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

    public Master setDataPath(String path) {
        this.dataPath = path;
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

    private void partition() {

    }

    @Override
    public void run() {
        partition();
    }
}
