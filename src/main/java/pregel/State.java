package pregel;

/**
 * Global state.
 * 
 * INITIALIZED ---> LOADED ---> CLEANED ---> COMPUTED
 *                                 ^            |
 *                                 |            |
 *                                  ------------
 */
public enum State {
    INITIALIZED, // the master is just created.
    LOADED, // workers loaded data.
    CLEANED, // workers did clean up before compute.
    COMPUTED // workers finished one superstep.
}