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

    /**
     * Vertices will invoke this to send message to other vertices.
     * 
     * @param message message to send.
     */
    void sendMessage(Message message) {
        if (vertices.containsKey(message.getReceiver())) {
            receiveMessage(message);
        } else {
            context.sendMessage(message);
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
            Class<Vertex> vertexClass = context.getVertexClass();
            Constructor<Vertex> vertexConstructor = vertexClass.getConstructor(long.class, Worker.class);
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line != null) {
                String[] parts = line.split(" ");
                if (parts.length >= 2) {
                    long sourceId = Long.parseLong(parts[0]);
                    long targetId = Long.parseLong(parts[1]);

                    if (!vertices.containsKey(sourceId)) {
                        Vertex source = vertexConstructor.newInstance(sourceId, this);
                        this.vertices.put(sourceId, source);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    void loadVertexProperties(String path) {

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