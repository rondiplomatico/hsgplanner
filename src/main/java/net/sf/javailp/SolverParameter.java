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

    /**
     * If set to 1 or 2, this parameter specifies that CPLEX should use advanced starting information when it initiates
     * optimization.
     */
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

    /**
     * Used when working memory (CPX_PARAM_WORKMEM, WorkMem) has been exceeded by the size of the tree. If the node file
     * parameter is set to zero when the tree memory limit is reached, optimization is terminated. Otherwise, a group of
     * nodes is removed from the in-memory set as needed. By default, CPLEX transfers nodes to node files when the
     * in-memory set is larger than 128 MBytes, and it keeps the resulting node files in compressed form in memory. At
     * settings 2 and 3, the node files are transferred to disk, in uncompressed and compressed form respectively, into
     * a directory named by the working directory parameter (CPX_PARAM_WORKDIR, WorkDir), and CPLEX actively manages
     * which nodes remain in memory for processing.
     *
     * @see SynthesisParam#SOLVER_MASTER_MAX_MEMORY
     * @see SynthesisParam#SOLVER_SUB_MAX_MEMORY
     */
    NODE_STORAGE_FILE_SWITCH,

    BRANCH_AND_CUT_MEMORY_LIMIT,

    WORKING_MEMORY;

}
