package framework;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import framework.api.Edge;
import framework.api.Message;
import framework.api.Vertex;

public class Worker implements Runnable {
    private final long id;

    /**
     * The master that this worker belongs to.
     */
    private final Master context;

    /**
     * Vertices on this worker.
     * 
     * The key of the map is the id of the corresponding vertex.
     */
    private final Map<Long, Vertex> vertices;

    private Class<? extends Vertex> vertexClass;

    private Class<? extends Edge> edgeClass;

    private String graphPath = null;

    private boolean graphLoaded = false;

    private String verticesPath = null;

    private boolean verticesLoaded = false;

    Worker(long id, Master context) {
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

    Worker setVertexClass(Class<? extends Vertex> vertexClass) {
        this.vertexClass = vertexClass;
        return this;
    }

    Worker setEdgeClass(Class<? extends Edge> edgeClass) {
        this.edgeClass = edgeClass;
        return this;
    }

    Worker setGraphPath(String path) {
        this.graphPath = path;
        return this;
    }

    Worker setVerticesPath(String path) {
        this.verticesPath = path;
        return this;
    }

    /**
     * Vertices will invoke this to send message to other vertices.
     * 
     * @param message message to send.
     */
    public void sendMessage(Message message) {
        long vertexId = message.getReceiver();
        if (vertices.containsKey(vertexId)) {
            receiveMessage(message);
        } else {
            Worker receiver = context.getWorkerFromVertexId(vertexId);
            receiver.receiveMessage(message);
        }
    }

    /**
     * Master will invoke this method to deliver messages to workers.
     * 
     * @param message message sent to vertices on this worker.
     */
    public void receiveMessage(Message message) {
        long id = message.getReceiver();
        Vertex receiver = vertices.get(id);
        if (receiver != null) {
            receiver.receiveMessage(message);
        }
    }

    public Iterator<Vertex> getVertices() {
        return this.vertices.values().iterator();
    }

    public void loadGraph() {
        try {
            Edge edge;
            Vertex source, target;
            BufferedReader reader = new BufferedReader(new FileReader(graphPath));
            String line = reader.readLine();
            while (line != null) {
                String[] parts = line.split("\t");
                if (parts.length >= 2) {
                    long sourceId = Long.parseLong(parts[0]);
                    long targetId = Long.parseLong(parts[1]);

                    if (!vertices.containsKey(sourceId)) {
                        source = vertexClass.newInstance();
                        source.setContext(this);
                        source.setId(sourceId);
                        this.vertices.put(sourceId, source);
                    } else {
                        source = vertices.get(sourceId);
                    }

                    if (!source.hasOuterEdgeTo(targetId)) {
                        edge = edgeClass.newInstance();
                        edge.setSource(sourceId);
                        edge.setTarget(targetId);
                        edge.fromStrings(parts);
                        source.addOuterEdge(edge);
                    } else {
                        System.out.println(
                            String.format("Warning: duplicate edge from %d to %d!", sourceId, targetId)
                        );
                    }

                    if (context.getWorkerIdFromVertexId(targetId) == this.id()) {
                        if (!vertices.containsKey(targetId)) {
                            target = vertexClass.newInstance();
                            target.setContext(this);
                            target.setId(targetId);
                            this.vertices.put(targetId, target);
                        }
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

    public void loadVertexProperties() {
        try {
            Vertex vertex;
            BufferedReader reader = new BufferedReader(new FileReader(verticesPath));
            String line = reader.readLine();
            while (line != null) {
                String[] parts = line.split("\t");
                long vertexId = Long.parseLong(parts[0]);
                if (vertices.containsKey(vertexId)) {
                    vertex = vertices.get(vertexId);
                } else {
                    vertex = vertexClass.newInstance();
                    vertex.setContext(this);
                    vertex.setId(vertexId);
                }
                vertex.fromStrings(parts);
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
            loadGraph();
        }

        if (verticesPath != null && !verticesLoaded) {
            loadVertexProperties();
        }

        long numActiveVertices = 0;
        for (Vertex vertex : vertices.values()) {
            if (vertex.hasMessages() || context.getSuperstep() == 0) {
                vertex.compute();
                numActiveVertices++;
            }
        }
        if (numActiveVertices == 0) {
            voteToHalt();
        }
    }
}