package framework;

import java.util.HashMap;
import java.util.Map;

public class Worker {
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
    private final Map<Long, ? extends Vertex> vertices;

    Worker(Master context) {
        this.context = context;
        this.id = context.generateId();
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
}