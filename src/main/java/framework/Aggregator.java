package framework;

public interface Aggregator<V, A> {
    /**
     * Get the aggregating value from vertex.
     * 
     * @param vertex a vertex.
     * @return the value to be aggregated.
     */
    public A report(V vertex);

    /**
     * Aggregate value a and value b.
     * 
     * @param a value a.
     * @param b value b.
     * @return the aggregation of a and b.
     */
    public A aggregate(A a, A b);
}