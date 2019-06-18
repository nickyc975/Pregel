package pregel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import pregel.utils.Tuple2;
import pregel.utils.Tuple3;

class Worker<V, E, M> implements Callable<Void>, Context<V, E, M> {
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
    private final Context<V, E, M> context;

    /**
     * Number of active vertices.
     */
    private long numActiveVertices;

    private long timeCost;

    private long numMessageSent;

    private long numMessageReceived;

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
     * The path of the vertices partition file that is assigned to this worker.
     */
    private String verticesPath = null;

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
    private Map<String, Aggregator<Vertex<V, E, M>, ?>> aggregators = null;

    /**
     * Aggregated values.
     */
    private Map<String, ?> aggregatedValues = null;

    Worker(long id, Context<V, E, M> context) {
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

    @Override
    public State state() {
        return context.state();
    }

    /**
     * Get superstep from master.
     * 
     * @return superstep.
     */
    @Override
    public long superstep() {
        return context.superstep();
    }

    long getLocalNumVertices() {
        return vertices.size();
    }

    @Override
    public long getNumVertices() {
        return context.getNumVertices();
    }

    long getLocalNumEdges() {
        return vertices.values()
                       .stream()
                       .mapToLong(vertex -> (long) vertex.getOuterEdges().size())
                       .sum();
    }

    @Override
    public long getNumEdges() {
        return context.getNumEdges();
    }

    long getTimeCost() {
        return this.timeCost;
    }

    long getNumMessageSent() {
        return this.numMessageSent;
    }

    long getNumMessageReceived() {
        return this.numMessageReceived;
    }

    @Override
    public void addVertex(long id) {
        context.addVertex(id);
    }

    @Override
    public <A> A getAggregatedValue(String valueName) {
        return context.getAggregatedValue(valueName);
    }

    /**
     * Get iterator of vertices on this worker.
     * 
     * @return iterator of vertices on this worker.
     */
    Iterator<Vertex<V, E, M>> getVertices() {
        return this.vertices.values().iterator();
    }

    Worker<V, E, M> setEdgeParser(Function<String, Tuple3<Long, Long, E>> edgeParser) {
        this.edgeParser = edgeParser;
        return this;
    }

    Worker<V, E, M> setVertexParser(Function<String, Tuple2<Long, V>> vertexParser) {
        this.vertexParser = vertexParser;
        return this;
    }

    Worker<V, E, M> setComputeFunction(Consumer<Vertex<V, E, M>> computeFunction) {
        this.computeFunction = computeFunction;
        return this;
    }

    Worker<V, E, M> setCombiner(Combiner<M> combiner) {
        this.combiner = combiner;
        return this;
    }

    Worker<V, E, M> setAggregators(Map<String, Aggregator<Vertex<V, E, M>, ?>> aggregators) {
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
     * @param sendQueue message queue.
     */
    private void sendMessages(Queue<Message<M>> sendQueue) {
        while (!sendQueue.isEmpty()) {
            context.sendMessage(sendQueue.poll());
            numMessageSent++;
        }
    }

    /**
     * Vertices will invoke this to send message to other vertices.
     * 
     * @param message message to send.
     */
    @Override
    public void sendMessage(Message<M> message) {
        long vertexId = message.getReceiver();
        Queue<Message<M>> sendQueue = sendQueues.get(vertexId);
        if (sendQueue == null) {
            sendQueue = new LinkedList<>();
            sendQueues.put(vertexId, sendQueue);
        }

        Message<M> initial = sendQueue.peek();
        if (combiner != null && initial != null) {
            initial = sendQueue.poll();
            M value = combiner.combine(initial.getValue(), message.getValue());
            message.setValue(value);
        }
        sendQueue.offer(message);

        if (sendQueue.size() > sendThreshold) {
            sendMessages(sendQueue);
        }
    }

    /**
     * Master will invoke this method to deliver messages to workers.
     * 
     * @param message message sent to vertices on this worker.
     */
    void receiveMessage(Message<M> message) {
        long id = message.getReceiver();
        Vertex<V, E, M> receiver = vertices.get(id);
        if (receiver != null) {
            M value = message.getValue();
            synchronized (receiver) {
                numMessageReceived++;
                if (combiner != null && receiver.hasNextStepMessage()) {
                    value = combiner.combine(receiver.readNextStepMessage(), value);
                }
                receiver.receiveMessage(value);
            }
        }
    }

    /**
     * Get or create a vertex with given id.
     * 
     * @param id vertex id.
     * @return the vertex with given id.
     */
    synchronized Vertex<V, E, M> getOrCreateVertex(long id) {
        Vertex<V, E, M> vertex = vertices.get(id);
        if (vertex == null) {
            vertex = new Vertex<>(id, this);
            this.vertices.put(id, vertex);
        }

        return vertex;
    }

    /**
     * Load edges partition.
     */
    private void loadEdges() {
        try {
            Vertex<V, E, M> source;
            Tuple3<Long, Long, E> edge;
            BufferedReader reader = new BufferedReader(new FileReader(edgesPath));
            String line = reader.readLine();
            while (line != null) {
                edge = edgeParser.apply(line);
                source = getOrCreateVertex(edge._1);
                if (!source.hasOuterEdgeTo(edge._2)) {
                    source.addOuterEdge(edge);
                } else {
                    System.out.println(
                        String.format("Warning: duplicate edge from %d to %d!", edge._1, edge._2)
                    );
                }
                context.addVertex(edge._2);
                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load vertices partition.
     */
    private void loadVertices() {
        try {
            Vertex<V, E, M> vertex;
            Tuple2<Long, V> vertexValue;
            BufferedReader reader = new BufferedReader(new FileReader(verticesPath));
            String line = reader.readLine();
            while (line != null) {
                vertexValue = vertexParser.apply(line);
                vertex = getOrCreateVertex(vertexValue._1);
                vertex.setValue(vertexValue._2);
                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Mark vertex with vertexId as done.
     * 
     * A vertex is done means the vertex has nothing to do in a superstep.
     * 
     * @param vertexId vertex id.
     */
    @Override
    public void markAsDone(long vertexId) {
        numActiveVertices--;
    }

    /**
     * Aggregate vertex with the aggregator with name valueName.
     * 
     * @param <A> return type of the aggregator's report() method.
     * @param valueName aggregator name.
     * @param vertex vertex to perform aggregator.
     */
    @SuppressWarnings("unchecked")
    private <A> void aggregate(String valueName, Vertex<V, E, M> vertex) {
        Map<String, A> values = (Map<String, A>) aggregatedValues;
        Aggregator<Vertex<V, E, M>, A> aggregator = 
                (Aggregator<Vertex<V, E, M>, A>) aggregators.get(valueName);
        
        A initial = values.get(valueName);
        A newValue = aggregator.report(vertex);
        if (initial == null) {
            initial = newValue;
        } else {
            initial = aggregator.aggregate(initial, newValue);
        }
        values.put(valueName, initial);
    }

    /**
     * Load data.
     */
    private void load() {
        if (edgesPath != null) {
            loadEdges();
        }

        if (verticesPath != null) {
            loadVertices();
        }
    }

    /**
     * Do some clean up before run.
     */
    private void clean() {
        aggregatedValues.clear();
        this.numMessageSent = 0;
        this.numMessageReceived = 0;
    }

    /**
     * Do the compute.
     */
    private void compute() {
        numActiveVertices = vertices.size();
        for (Vertex<V, E, M> vertex : vertices.values()) {
            computeFunction.accept(vertex);
            for (String valueName : aggregators.keySet()) {
               aggregate(valueName, vertex);
            }
        }

        for (Queue<Message<M>> queue : sendQueues.values()) {
            sendMessages(queue);
        }

        if (numActiveVertices == 0) {
            context.markAsDone(id());
        }
    }

    /**
     * Report aggregating result and metrics to master.
     */
    @SuppressWarnings("unchecked")
    <A> A report(String valueName) {
        return (A) aggregatedValues.get(valueName);
    }

    /**
     * Run.
     */
    @Override
    public Void call() {
        long startTime = System.currentTimeMillis();
        switch (context.state()) {
            case Initialized:
                load();
                break;
            case Loaded:
                clean();
                break;
            case Cleaned:
                compute();
                break;
            case Computed:
                clean();
                break;
            default:
                throw new IllegalStateException();
        }
        this.timeCost = System.currentTimeMillis() - startTime;
        return null;
    }
}