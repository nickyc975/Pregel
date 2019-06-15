package framework;

public interface Aggregator<V, A> {
    public A report(V vertex);

    public A aggregate(A a, A b);
}