package framework;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import framework.api.EdgeValue;
import framework.api.VertexValue;

public final class Vertex<V extends VertexValue, E extends EdgeValue, M> {
    /**
     * Id of this vertex.
     */
    private final long id;

    /**
     * The worker that this vertex belongs to. 
     */
    private final Worker<V, E, M> context;

    private V value = null;

    /**
     * Outer edge of this vertex.
     */
    private Map<Long, E> outerEdges;

    /**
     * When current superstep is odd, use oddReceiveQueue to store 
     * the messages received in current superstep.
     */
    private Queue<M> oddReceiveQueue;

    /**
     * When current superstep is even, use evenReceiveQueue to store 
     * the messages received in current superstep.
     */
    private Queue<M> evenReceiveQueue;

    Vertex(long id, Worker<V, E, M> context) {
        this.id = id;
        this.context = context;
        this.outerEdges = new HashMap<>();
        this.oddReceiveQueue = new LinkedList<>();
        this.evenReceiveQueue = new LinkedList<>();
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
     * Get context of this vertex.
     * 
     * @return context of this vertex.
     */
    public final Worker<V, E, M> context() {
        return this.context;
    }

    public final V getValue() {
        return this.value;
    }

    public final void setValue(V value) {
        this.value = value;
    }

    /**
     * Add an edge with this vertex as the source to the edge list of this vertex.
     * 
     * If the source of the edge is not this vertex, it won't be added.
     * 
     * @param edge edge to add.
     */
    public final void addOuterEdge(E edge) {
        if (edge.source() == this.id()) {
            outerEdges.put(edge.target(), edge);
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
    public final E getOuterEdgeTo(long target) {
        return this.outerEdges.get(target);
    }

    /**
     * Get outer edges of this vertex.
     * 
     * @return outer edges of this vertex.
     */
    public final Map<Long, E> getOuterEdges() {
        Map<Long, E> result = new HashMap<>();
        result.putAll(this.outerEdges);
        return result;
    }

    /**
     * Send a message to the receiver.
     * 
     * @param receiver receiver id.
     * @param message message to send.
     */
    public final void sendMessageTo(long receiver, M value) {
        Message<M> message = new Message<>(value);
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
    public final M readMessage() {
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
    public final void sendMessage(M message) {
        for (Long target : getOuterEdges().keySet()) {
            sendMessageTo(target, message);
        }
    }

    /**
     * Worker will invoke this method to deliver messages to vertices.
     * 
     * @param message message sent to this vertex.
     */
    public synchronized final void receiveMessage(Message<M> message) {
        if (context.getSuperstep() % 2 == 0) {
            evenReceiveQueue.add(message.getValue());
        } else {
            oddReceiveQueue.add(message.getValue());
        }
    }
}