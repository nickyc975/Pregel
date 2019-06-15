package framework;

public interface Combiner<M> {
    /**
     * Combine message value a and b.
     * 
     * @param a value a.
     * @param b value b.
     * @return the combination of a and b.
     */
    public M combine(M a, M b);
}