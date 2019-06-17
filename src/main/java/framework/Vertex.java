package framework;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import framework.utils.Tuple3;

public final class Vertex<V, E, M> {
    /**
     * Id of this vertex.
     */
    private final long id;

    /**
     * The worker that this vertex belongs to. 
     */
    private final Context<V, E, M> context;

    /**
     * Value of this vertex.
     */
    private V value = null;

    /**
     * Outer edges of this vertex.
     */
    private final Map<Long, Tuple3<Long, Long, E>> outerEdges;

    /**
     * When current superstep is odd, use oddReceiveQueue to store 
     * the messages received in current superstep.
     */
    private final Queue<M> oddReceiveQueue;

    /**
     * When current superstep is even, use evenReceiveQueue to store 
     * the messages received in current superstep.
     */
    private final Queue<M> evenReceiveQueue;

    Vertex(long id, Context<V, E, M> context) {
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
    public final Context<V, E, M> context() {
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
    public final void addOuterEdge(Tuple3<Long, Long, E> edge) {
        if (edge._1.equals(this.id())) {
            outerEdges.put(edge._2, edge);
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
    public final Tuple3<Long, Long, E> getOuterEdgeTo(long target) {
        return this.outerEdges.get(target);
    }

    /**
     * Get outer edges of this vertex.
     * 
     * @return outer edges of this vertex.
     */
    public final Map<Long, Tuple3<Long, Long, E>> getOuterEdges() {
        Map<Long, Tuple3<Long, Long, E>> result = new HashMap<>();
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
               .setSuperstep(context.superstep());
        context.sendMessage(message);
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
     * Check if this vertex has messages to deal with.
     * 
     * @return true if has messages, false otherwise.
     */
    public final boolean hasMessages() {
        if (context.superstep() % 2 == 0) {
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
        if (context.superstep() % 2 == 0) {
            return oddReceiveQueue.remove();
        } else {
            return evenReceiveQueue.remove();
        }
    }

    /**
     * Tell the worker that this vertex is done.
     */
    public void voteToHalt() {
        context.markAsDone(id());
    }

    /**
     * Worker will invoke this method to deliver messages to vertices.
     * 
     * @param message message sent to this vertex.
     */
    synchronized final void receiveMessage(M message) {
        if (context.superstep() % 2 == 0) {
            evenReceiveQueue.add(message);
        } else {
            oddReceiveQueue.add(message);
        }
    }

    /**
     * Check if this vertex has messages to deal with in next superstep.
     * 
     * @return true if has messages, false otherwise.
     */
    synchronized final boolean hasNextStepMessage() {
        if (context.superstep() % 2 == 0) {
            return !evenReceiveQueue.isEmpty();
        } else {
            return !oddReceiveQueue.isEmpty();
        }
    }

    /**
     * Read message that will be used in next super step.
     * 
     * Workers invoke this method to perform message combining.
     * 
     * @return A message in the next superstep receive queue.
     */
    synchronized final M readNextStepMessage() {
        if (context.superstep() % 2 == 0) {
            return evenReceiveQueue.remove();
        } else {
            return oddReceiveQueue.remove();
        }
    }
}