package framework;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

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

    private Class<Vertex> vertexClass;

    private Constructor<Vertex> vertexConstructor;

    private Class<Edge> edgeClass;

    private Constructor<Edge> edgeConstructor;

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
    long getSuperstep() {
        return context.getSuperstep();
    }

    Worker setVertexClass(Class<Vertex> vertexClass) {
        this.vertexClass = vertexClass;
        try {
            this.vertexConstructor = vertexClass.getConstructor(long.class, Worker.class);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return this;
    }

    Worker setEdgeClass(Class<Edge> edgeClass) {
        this.edgeClass = edgeClass;
        try {
            this.edgeConstructor = edgeClass.getConstructor(long.class, long.class);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return this;
    }

    /**
     * Vertices will invoke this to send message to other vertices.
     * 
     * @param message message to send.
     */
    void sendMessage(Message message) {
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
    void receiveMessage(Message message) {
        long id = message.getReceiver();
        Vertex receiver = vertices.get(id);
        if (receiver != null) {
            receiver.receiveMessage(message);
        }
    }

    void loadGraph(String path) {
        try {
            Edge edge;
            Vertex source, target;
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line != null) {
                String[] parts = line.split(" ");
                if (parts.length >= 2) {
                    long sourceId = Long.parseLong(parts[0]);
                    long targetId = Long.parseLong(parts[1]);

                    if (!vertices.containsKey(sourceId)) {
                        source = vertexConstructor.newInstance(sourceId, this);
                        this.vertices.put(sourceId, source);
                    } else {
                        source = vertices.get(sourceId);
                    }

                    if (!source.hasOuterEdgeTo(targetId)) {
                        edge = edgeConstructor.newInstance(sourceId, targetId);
                        edge.fromStrings(parts);
                        source.addOuterEdge(edge);
                    } else {
                        System.out.println(String.format("Warning: duplicate edge from %d to %d!", sourceId, targetId));
                    }

                    if (context.getWorkerIdFromVertexId(targetId) == this.id()) {
                        if (!vertices.containsKey(targetId)) {
                            target = vertexConstructor.newInstance(targetId, this);
                            this.vertices.put(targetId, target);
                        }
                    }
                }
                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    void loadVertexProperties(String path) {
        try {
            Vertex vertex;
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line != null) {
                String[] parts = line.split(" ");
                long vertexId = Long.parseLong(parts[0]);
                if (vertices.containsKey(vertexId)) {
                    vertex = vertices.get(vertexId);
                } else {
                    vertex = vertexConstructor.newInstance(vertexId, this);
                }
                vertex.fromStrings(parts);
                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    public void run() {
        for (Vertex vertex : vertices.values()) {
            if (vertex.isActive()) {
                vertex.compute();
            }
        }
    }
}