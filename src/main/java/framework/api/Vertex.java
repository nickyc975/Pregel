package framework.api;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import framework.Worker;

public abstract class Vertex {
    /**
     * The worker that this vertex belongs to. 
     */
    protected Worker context;

    /**
     * Id of this vertex.
     */
    protected long id;

    /**
     * Outer edge of this vertex.
     */
    private final Map<Long, Edge> outerEdges;

    /**
     * When current superstep is odd, use oddReceiveQueue to store 
     * the messages received in current superstep.
     */
    private final Queue<Message> oddReceiveQueue;

    /**
     * When current superstep is even, use evenReceiveQueue to store 
     * the messages received in current superstep.
     */
    private final Queue<Message> evenReceiveQueue;

    public Vertex() {
        this.outerEdges = new HashMap<>();
        this.oddReceiveQueue = new LinkedList<>();
        this.evenReceiveQueue = new LinkedList<>();
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setContext(Worker context) {
        this.context = context;
    } 

    /**
     * Get id of this vertex.
     * 
     * Id must be globally identical.
     * 
     * @return id of this vertex.
     */
    public final long id() {
        return this.id;
    };

    /**
     * Add an edge with this vertex as the source to the edge list of this vertex.
     * 
     * If the source of the edge is not this vertex, it won't be added.
     * 
     * @param edge edge to add.
     */
    public final void addOuterEdge(Edge edge) {
        if (edge.getSource() == this.id()) {
            outerEdges.put(edge.getTarget(), edge);
        }
    }

    /**
     * Remove the edge with the given target.
     * 
     * @param target target of the removing edge.
     */
    public final void removeOuterEdge(long target) {
        outerEdges.remove(target);
    }

    /**
     * Check if the vertex has an outer edge to the given target.
     * 
     * @param target id of target vertex.
     * @return the result.
     */
    public final boolean hasOuterEdgeTo(long target) {
        return this.outerEdges.containsKey(target);
    }

    /**
     * Get the outer edge to the given target.
     * 
     * @param target id of target vertex.
     * @return the result.
     */
    public final Edge getOuterEdgeTo(long target) {
        return this.outerEdges.get(target);
    }

    /**
     * Get outer edges of this vertex.
     * 
     * @return outer edges of this vertex.
     */
    public final Map<Long, Edge> getOuterEdges() {
        Map<Long, Edge> result = new HashMap<>();
        result.putAll(this.outerEdges);
        return result;
    }

    /**
     * Send a message to the receiver.
     * 
     * @param receiver receiver id.
     * @param message message to send.
     */
    public final void sendMessageTo(long receiver, Message message) {
        message.setSender(this.id())
               .setReceiver(receiver)
               .setSuperstep(context.getSuperstep());
        context.sendMessage(message);
    }

    /**
     * Check if this vertex has messages to deal with.
     * 
     * @return true if has messages, false otherwise.
     */
    public final boolean hasMessages() {
        if (context.getSuperstep() % 2 == 0) {
            return !oddReceiveQueue.isEmpty();
        } else {
            return !evenReceiveQueue.isEmpty();
        }
    }

    /**
     * Read a message sent to this vertex for computing.
     * 
     * @return message received.
     */
    public final Message readMessage() {
        if (context.getSuperstep() % 2 == 0) {
            return oddReceiveQueue.remove();
        } else {
            return evenReceiveQueue.remove();
        }
    }

    /**
     * Send a message to all neighbors.
     * 
     * @param message message to be sent.
     */
    public final void sendMessage(Message message) {
        for (Long target : getOuterEdges().keySet()) {
            sendMessageTo(target, message);
        }
    }

    /**
     * Worker will invoke this method to deliver messages to vertices.
     * 
     * @param message message sent to this vertex.
     */
    public synchronized final void receiveMessage(Message message) {
        if (context.getSuperstep() % 2 == 0) {
            evenReceiveQueue.add(message);
        } else {
            oddReceiveQueue.add(message);
        }
    }

    /**
     * The main calculating logic of each vertex.
     */
    public abstract void compute();

    /**
     * Parse user defined properties of a vertex object from string.
     * 
     * @param strings Every string is a property. The first string is vertex id.
     */
    public abstract void fromStrings(String[] strings);
}