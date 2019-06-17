package framework;

/**
 * This is the interface that Vertex and Worker used to make global communication.
 * 
 * @param <V> user defined vertex value.
 * @param <E> user defined edge value.
 * @param <M> user defined message value.
 */
public interface Context<V, E, M> {
    /**
     * Get global state.
     * 
     * @return global state.
     */
    public State state();

    /**
     * Get global superstep.
     * 
     * @return superstep.
     */
    public long superstep();

    /**
     * Get global number of edges.
     * 
     * @return number of edges.
     */
    public long getNumEdges();

    /**
     * Get global number of vertices.
     * 
     * @return number of vertices.
     */
    public long getNumVertices();

    /**
     * Add a vertex with given id.
     * 
     * @param id vertex id.
     */
    public void addVertex(long id);

    /**
     * Mark a worker or vertex as done.
     * 
     * @param id worker or vertex id.
     */
    public void markAsDone(long id);

    /**
     * Send a message to a vertex.
     * 
     * @param message message to be sent.
     */
    public void sendMessage(Message<M> message);

    /**
     * Get globally aggregated value with given value name.
     * 
     * @param <A> value type.
     * @param valueName value name.
     * @return globally aggregated value with given value name.
     */
    public <A> A getAggregatedValue(String valueName);
}