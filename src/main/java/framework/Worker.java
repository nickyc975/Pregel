package framework;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;

import framework.utils.Tuple2;
import framework.utils.Tuple3;

public class Worker<V, E, M> implements Runnable {
    /**
     * Sending threshold. When the number of messages in queue is larger
     * than the threshold, the messages will be sent and the queue will 
     * be cleared.
     */
    private static final int sendThreshold = 10;

    /**
     * Worker id.
     */
    private final long id;

    /**
     * The master that this worker belongs to.
     */
    private final Master<V, E, M> context;

    /**
     * Number of active vertices.
     */
    private long numActiveVertices;

    /**
     * Vertices on this worker.
     * 
     * The key of the map is the id of the corresponding vertex.
     */
    private final Map<Long, Vertex<V, E, M>> vertices;

    /**
     * The path of the edges partition file that is assigned to this worker.
     */
    private String edgesPath = null;

    /**
     * Indicate whether the edges are loaded.
     */
    private boolean edgesLoaded = false;

    /**
     * The path of the vertices partition file that is assigned to this worker.
     */
    private String verticesPath = null;

    /**
     * Indicate whether the vertices are loaded.
     */
    private boolean verticesLoaded = false;

    /**
     * Same as Master.edgeParser.
     */
    private Function<String, Tuple3<Long, Long, E>> edgeParser = null;

    /**
     * Same as Master.vertexParser.
     */
    private Function<String, Tuple2<Long, V>> vertexParser = null;

    /**
     * Same as Master.computeFunction.
     */
    private Consumer<Vertex<V, E, M>> computeFunction = null;

    /**
     * User defined message combiner.
     */
    private Combiner<M> combiner = null;

    /**
     * Send queues for target vertices.
     */
    private Map<Long, Queue<Message<M>>> sendQueues = null;

    /**
     * Aggregators.
     */
    private Map<String, Aggregator<V, ?>> aggregators = null;

    /**
     * Aggregated values.
     */
    private Map<String, ?> aggregatedValues = null;

    Worker(long id, Master<V, E, M> context) {
        this.id = id;
        this.context = context;
        this.vertices = new HashMap<>();
        this.sendQueues = new HashMap<>();
        this.aggregators = new HashMap<>();
        this.aggregatedValues = new HashMap<>();
    }

    long id() {
        return this.id;
    }

    /**
     * Get superstep from master.
     * 
     * @return superstep.
     */
    public long getSuperstep() {
        return context.getSuperstep();
    }

    public long getNumVertices() {
        return vertices.size();
    }

    public long getTotalNumVertices() {
        return context.getNumVertices();
    }

    public Worker<V, E, M> setEdgeParser(Function<String, Tuple3<Long, Long, E>> edgeParser) {
        this.edgeParser = edgeParser;
        return this;
    }

    public Worker<V, E, M> setVertexParser(Function<String, Tuple2<Long, V>> vertexParser) {
        this.vertexParser = vertexParser;
        return this;
    }

    public Worker<V, E, M> setComputeFunction(Consumer<Vertex<V, E, M>> computeFunction) {
        this.computeFunction = computeFunction;
        return this;
    }

    public Worker<V, E, M> setCombiner(Combiner<M> combiner) {
        this.combiner = combiner;
        return this;
    }

    public Worker<V, E, M> setAggregators(Map<String, Aggregator<V, ?>> aggregators) {
        this.aggregators.putAll(aggregators);
        return this;
    }

    Worker<V, E, M> setEdgesPath(String path) {
        this.edgesPath = path;
        return this;
    }

    Worker<V, E, M> setVerticesPath(String path) {
        this.verticesPath = path;
        return this;
    }

    /**
     * Send messages to the given vertex.
     * 
     * @param vertexId id of the target vertex.
     * @param sendQueue message queue.
     */
    private void sendMessagesTo(long vertexId, Queue<Message<M>> sendQueue) {
        Worker<V, E, M> receiver;
        if (vertices.containsKey(vertexId)) {
            receiver = this;
        } else {
            receiver = context.getWorkerFromVertexId(vertexId);
        }

        while (!sendQueue.isEmpty()) {
            receiver.receiveMessage(sendQueue.poll());
        }
    }

    /**
     * Vertices will invoke this to send message to other vertices.
     * 
     * @param message message to send.
     */
    public void sendMessage(Message<M> message) {
        long vertexId = message.getReceiver();
        Queue<Message<M>> sendQueue = sendQueues.get(vertexId);
        if (sendQueue == null) {
            sendQueue = new LinkedList<>();
            sendQueues.put(vertexId, sendQueue);
        }

        Message<M> initial = sendQueue.poll();
        if (this.combiner != null) {
            if (initial == null) {
                initial = message;
            } else {
                M value = combiner.combine(initial.getValue(), message.getValue());
                initial.setValue(value);
            }
        }
        sendQueue.offer(initial);

        if (sendQueue.size() > sendThreshold) {
            sendMessagesTo(vertexId, sendQueue);
        }
    }

    /**
     * Master will invoke this method to deliver messages to workers.
     * 
     * @param message message sent to vertices on this worker.
     */
    public void receiveMessage(Message<M> message) {
        long id = message.getReceiver();
        Vertex<V, E, M> receiver = vertices.get(id);
        if (receiver != null) {
            M value = message.getValue();
            synchronized (receiver) {
                if (combiner != null && receiver.hasNextStepMessage()) {
                    value = combiner.combine(receiver.readNextStepMessage(), value);
                }
                receiver.receiveMessage(value);
            }
        }
    }

    /**
     * Get iterator of vertices on this worker.
     * 
     * @return iterator of vertices on this worker.
     */
    public Iterator<Vertex<V, E, M>> getVertices() {
        return this.vertices.values().iterator();
    }

    /**
     * Load edges partition.
     */
    public void loadEdges() {
        try {
            Vertex<V, E, M> source;
            Tuple3<Long, Long, E> edge;
            BufferedReader reader = new BufferedReader(new FileReader(edgesPath));
            String line = reader.readLine();
            while (line != null) {
                edge = edgeParser.apply(line);
                if (!vertices.containsKey(edge._1)) {
                    source = new Vertex<>(edge._1, this);
                    this.vertices.put(edge._1, source);
                } else {
                    source = vertices.get(edge._1);
                }

                if (!source.hasOuterEdgeTo(edge._2)) {
                    source.addOuterEdge(edge);
                } else {
                    System.out.println(
                        String.format("Warning: duplicate edge from %d to %d!", edge._1, edge._2)
                    );
                }

                if (context.getWorkerIdFromVertexId(edge._2) == this.id()) {
                    if (!vertices.containsKey(edge._2)) {
                        this.vertices.put(edge._2, new Vertex<>(edge._2, this));
                    }
                }
                line = reader.readLine();
            }
            edgesLoaded = true;
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load vertices partition.
     */
    public void loadVertices() {
        try {
            Vertex<V, E, M> vertex;
            Tuple2<Long, V> vertexValue;
            BufferedReader reader = new BufferedReader(new FileReader(verticesPath));
            String line = reader.readLine();
            while (line != null) {
                vertexValue = vertexParser.apply(line);
                long vertexId = vertexValue._1;
                if (vertices.containsKey(vertexId)) {
                    vertex = vertices.get(vertexId);
                } else {
                    vertex = new Vertex<>(vertexId, this);
                }
                vertex.setValue(vertexValue._2);
                vertices.put(vertexId, vertex);
                line = reader.readLine();
            }
            verticesLoaded = true;
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Tell master that this worker is done.
     */
    void voteToHalt() {
        context.markAsDone(id());
    }

    /**
     * Mark vertex with vertexId as done.
     * 
     * A vertex is done means the vertex has nothing to do in a superstep.
     * 
     * @param vertexId vertex id.
     */
    void markAsDone(long vertexId) {
        numActiveVertices--;
    }

    /**
     * Do the computing.
     */
    @Override
    public void run() {
        if (edgesPath != null && !edgesLoaded) {
            loadEdges();
        }

        if (verticesPath != null && !verticesLoaded) {
            loadVertices();
        }

        numActiveVertices = vertices.size();
        for (Vertex<V, E, M> vertex : vertices.values()) {
            computeFunction.accept(vertex);
        }
        
        for (Entry<Long, Queue<Message<M>>> entry : sendQueues.entrySet()) {
            sendMessagesTo(entry.getKey(), entry.getValue());
        }

        if (numActiveVertices == 0) {
            voteToHalt();
        }
    }
}