package framework;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import framework.api.EdgeValue;
import framework.api.VertexValue;

public class Worker<V extends VertexValue, E extends EdgeValue, M> implements Runnable {
    private final long id;

    /**
     * The master that this worker belongs to.
     */
    private final Master<V, E, M> context;

    /**
     * Vertices on this worker.
     * 
     * The key of the map is the id of the corresponding vertex.
     */
    private final Map<Long, Vertex<V, E, M>> vertices;

    private String graphPath = null;

    private boolean graphLoaded = false;

    private String verticesPath = null;

    private boolean verticesLoaded = false;

    private Function<String, E> edgeParser = null;

    private Function<String, V> vertexParser = null;

    private Consumer<Vertex<V, E, M>> computeFunction = null;

    Worker(long id, Master<V, E, M> context) {
        this.id = id;
        this.context = context;
        this.vertices = new HashMap<>();
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

    public Worker<V, E, M> setEdgeParser(Function<String, E> edgeParser) {
        this.edgeParser = edgeParser;
        return this;
    }

    public Worker<V, E, M> setVertexParser(Function<String, V> vertexParser) {
        this.vertexParser = vertexParser;
        return this;
    }

    public Worker<V, E, M> setComputeFunction(Consumer<Vertex<V, E, M>> computeFunction) {
        this.computeFunction = computeFunction;
        return this;
    }

    Worker<V, E, M> setGraphPath(String path) {
        this.graphPath = path;
        return this;
    }

    Worker<V, E, M> setVerticesPath(String path) {
        this.verticesPath = path;
        return this;
    }

    /**
     * Vertices will invoke this to send message to other vertices.
     * 
     * @param message message to send.
     */
    public void sendMessage(Message<M> message) {
        long vertexId = message.getReceiver();
        if (vertices.containsKey(vertexId)) {
            receiveMessage(message);
        } else {
            Worker<V, E, M> receiver = context.getWorkerFromVertexId(vertexId);
            receiver.receiveMessage(message);
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
            receiver.receiveMessage(message);
        }
    }

    public Iterator<Vertex<V, E, M>> getVertices() {
        return this.vertices.values().iterator();
    }

    public void loadEdges() {
        try {
            E edge;
            Vertex<V, E, M> source;
            long sourceId, targetId;
            BufferedReader reader = new BufferedReader(new FileReader(graphPath));
            String line = reader.readLine();
            while (line != null) {
                edge = edgeParser.apply(line);
                sourceId = edge.source();
                targetId = edge.target();
                if (!vertices.containsKey(sourceId)) {
                    source = new Vertex<>(sourceId, this);
                    this.vertices.put(sourceId, source);
                } else {
                    source = vertices.get(sourceId);
                }

                if (!source.hasOuterEdgeTo(targetId)) {
                    source.addOuterEdge(edge);
                } else {
                    System.out.println(
                        String.format("Warning: duplicate edge from %d to %d!", sourceId, targetId)
                    );
                }

                if (context.getWorkerIdFromVertexId(targetId) == this.id()) {
                    if (!vertices.containsKey(targetId)) {
                        this.vertices.put(targetId, new Vertex<>(targetId, this));
                    }
                }
                line = reader.readLine();
            }
            graphLoaded = true;
            reader.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void loadVertices() {
        try {
            V vertexValue;
            Vertex<V, E, M> vertex;
            BufferedReader reader = new BufferedReader(new FileReader(verticesPath));
            String line = reader.readLine();
            while (line != null) {
                vertexValue = vertexParser.apply(line);
                long vertexId = vertexValue.id();
                if (vertices.containsKey(vertexId)) {
                    vertex = vertices.get(vertexId);
                } else {
                    vertex = new Vertex<>(vertexId, this);
                }
                vertex.setValue(vertexValue);
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

    public void voteToHalt() {
        context.setDone(id());
    }

    @Override
    public void run() {
        if (graphPath != null && !graphLoaded) {
            loadEdges();
        }

        if (verticesPath != null && !verticesLoaded) {
            loadVertices();
        }

        long numActiveVertices = 0;
        for (Vertex<V, E, M> vertex : vertices.values()) {
            if (vertex.hasMessages() || context.getSuperstep() == 0) {
                computeFunction.accept(vertex);
                numActiveVertices++;
            }
        }
        if (numActiveVertices == 0) {
            voteToHalt();
        }
    }
}