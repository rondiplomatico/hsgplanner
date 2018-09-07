/* _____________________________________________________________________________
 *
 * Project: PBK AP Hadoop
 * File:    SolverParameter.java
 * _____________________________________________________________________________
 *
 * Copyright: (C) Daimler AG 2016, all rights reserved
 * _____________________________________________________________________________
 */
package net.sf.javailp;

import java.io.OutputStream;

/**
 * Solver parameters that can be passed to the actual solver implementation prior to optimization start.
 *
 * @author schmidts
 * @since 10.11.2016
 */
public enum SolverParameter {

    TIMEOUT,

    VERBOSE,

    ADVANCED_START_SWITCH,

    MEMORY_EMPHASIS,

    POSTSOLVE,

    ALGORITHM_TYPE,

    /**
     * Type {@link OutputStream}
     */
    OUTSTREAM,

    WORK_DIRECTORY,

    NUMBER_OF_THREADS,

    RAND_SEED,

    NODE_STORAGE_FILE_SWITCH,

    BRANCH_AND_CUT_MEMORY_LIMIT,

    WORKING_MEMORY;

}
