package framework;

/**
 * Global state.
 * 
 * Initialized ---> Loaded ---> Cleaned ---> Computed
 *                                 ^            |
 *                                 |            |
 *                                  ------------
 */
public enum State {
    Initialized, // the master is just created.
    Loaded, // workers loaded data.
    Cleaned, // workers did clean up before compute.
    Computed // workers finished one superstep.
}