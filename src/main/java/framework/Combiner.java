package framework;

public interface Combiner<M> {
    public M combine(M a, M b);
}