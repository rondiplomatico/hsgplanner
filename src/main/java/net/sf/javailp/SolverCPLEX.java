/**
 * Java ILP is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 *
 * Java ILP is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with Java ILP. If not, see http://www.gnu.org/licenses/.
 */
package net.sf.javailp;

import ilog.concert.IloException;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumVar;
import ilog.concert.IloNumVarType;
import ilog.cplex.IloCplex;
import ilog.cplex.IloCplex.DoubleParam;
import ilog.cplex.IloCplex.Param;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * The {@code SolverCPLEX} is the {@code Solver} CPLEX.
 *
 * @author lukasiewycz
 *
 */
public class SolverCPLEX extends AbstractSolver {

    /**
     * The {@code Hook} for the {@code SolverCPLEX}.
     *
     * @author lukasiewycz
     *
     */
    public interface Hook {

        /**
         * This method is called once before the optimization and allows to
         * change some internal settings.
         *
         * @param cplex
         *            the cplex solver
         * @param varToNum
         *            the map of variables to cplex specific variables
         */
        public void call(IloCplex cplex, Map<Object, IloNumVar> varToNum);
    }

    protected final Set<Hook> hooks = new HashSet<Hook>();

    /**
     * Adds a hook.
     *
     * @param hook
     *            the hook to be added
     */
    public void addHook(final Hook hook) {
        hooks.add(hook);
    }

    /**
     * Removes a hook
     *
     * @param hook
     *            the hook to be removed
     */
    public void removeHook(final Hook hook) {
        hooks.remove(hook);
    }

    /*
     * (non-Javadoc)
     *
     * @see net.sf.javailp.Solver#solve(net.sf.javailp.Problem)
     */
    @Override
    public Result solve(final Problem problem) {
        Map<IloNumVar, Object> numToVar = new HashMap<IloNumVar, Object>();
        Map<Object, IloNumVar> varToNum = new HashMap<Object, IloNumVar>();

        try {
            IloCplex cplex = new IloCplex();
            cplex.setOut(System.err);

            solver.setParameter(SolverParameter.RAND_SEED, configuration.getInt(SynthesisParam.RAND_SEED));
            solver.setParameter(SolverParameter.NUMBER_OF_THREADS,
                                configuration.getInt(SynthesisParam.NUMBER_OF_THREADS));
            solver.setParameter(SolverParameter.WORK_DIRECTORY, workDir);
            solver.setParameter(SolverParameter.WORKING_MEMORY, configuration.getDouble(SynthesisParam.SOLVER_SUB_MAX_MEMORY));
            solver.setParameter(SolverParameter.NODE_STORAGE_FILE_SWITCH,
                                configuration.getInt(SynthesisParam.NODE_STORAGE_FILE_SWITCH));
            solver.setParameter(SolverParameter.MEMORY_EMPHASIS,
                                configuration.getBoolean(SynthesisParam.MEMORY_EMPHASIS));
            solver.setParameter(SolverParameter.ADVANCED_START_SWITCH,
                                configuration.getInt(SynthesisParam.ADVANCED_START_SWITCH));
            solver.setParameter(SolverParameter.VERBOSE, 3);

            initWithParameters(cplex);

            for (Object variable : problem.getVariables()) {
                VarType varType = problem.getVarType(variable);
                Number lowerBound = problem.getVarLowerBound(variable);
                Number upperBound = problem.getVarUpperBound(variable);

                double lb = lowerBound != null ? lowerBound.doubleValue() : Double.NEGATIVE_INFINITY;
                double ub = upperBound != null ? upperBound.doubleValue() : Double.POSITIVE_INFINITY;

                final IloNumVarType type;
                switch (varType) {
                case BOOL:
                    type = IloNumVarType.Bool;
                    break;
                case INT:
                    type = IloNumVarType.Int;
                    break;
                default: // REAL
                    type = IloNumVarType.Float;
                    break;
                }

                IloNumVar num = cplex.numVar(lb, ub, type, variable.toString());

                numToVar.put(num, variable);
                varToNum.put(variable, num);
            }

            for (Constraint constraint : problem.getConstraints()) {
                IloLinearNumExpr lin = cplex.linearNumExpr();
                Linear linear = constraint.getLhs();
                convert(linear, lin, varToNum);

                double rhs = constraint.getRhs().doubleValue();

                switch (constraint.getOperator()) {
                case LE:
                    cplex.addLe(lin, rhs, constraint.getName());
                    break;
                case GE:
                    cplex.addGe(lin, rhs, constraint.getName());
                    break;
                default: // EQ
                    cplex.addEq(lin, rhs, constraint.getName());
                }
            }

            for (SOS sos : problem.getSoss()) {
                int size = sos.size();
                IloNumVar[] vars = new IloNumVar[size];
                double[] coeff = new double[size];
                for (int i = 0; i < size; i++) {
                    vars[i] = varToNum.get(sos.getLhs().get(i).variable);
                    coeff[i] = i;//sos.getLhs().get(i).coefficient.doubleValue();
                }
                cplex.addSOS1(vars, coeff, sos.getName());
            }

            if (problem.getObjective() != null) {
                IloLinearNumExpr lin = cplex.linearNumExpr();
                Linear objective = problem.getObjective();
                convert(objective, lin, varToNum);

                if (problem.getOptType() == OptType.MIN) {
                    cplex.addMinimize(lin);
                } else {
                    cplex.addMaximize(lin);
                }
            }

            for (Hook hook : hooks) {
                hook.call(cplex, varToNum);
            }

            if (!cplex.solve()) {
                //System.err.println(cplex);
                // cplex.getStatus()
                cplex.end();
                return null;
            }
            //System.err.println(cplex);

            final Result result;
            if (problem.getObjective() != null) {
                Linear objective = problem.getObjective();
                result = new ResultImpl(objective);
            } else {
                result = new ResultImpl();
            }

            for (Entry<Object, IloNumVar> entry : varToNum.entrySet()) {
                Object variable = entry.getKey();
                IloNumVar num = entry.getValue();
                VarType varType = problem.getVarType(variable);

                double value = cplex.getValue(num);
                if (varType.isInt()) {
                    int v = (int) Math.round(value);
                    result.putPrimalValue(variable, v);
                } else {
                    result.putPrimalValue(variable, value);
                }
            }

            cplex.end();

            return result;

        } catch (IloException e) {
            e.printStackTrace();
        }

        return null;
    }

    protected void initWithParameters(final IloCplex cplex) throws IloException {
        Object timeout = parameters.get(Solver.TIMEOUT);
        Object verbose = parameters.get(Solver.VERBOSE);

        if (timeout != null && timeout instanceof Number) {
            Number number = (Number) timeout;
            double value = number.doubleValue();
            cplex.setParam(DoubleParam.TiLim, value);
        }
        if (verbose != null && verbose instanceof Number) {
            Number number = (Number) verbose;
            int value = number.intValue();

            if (value == 0) {
                cplex.setOut(null);
            }
        }

    }

    protected void convert(final Linear linear, final IloLinearNumExpr lin, final Map<Object, IloNumVar> varToNum) throws IloException {
        for (Term term : linear) {
            Number coeff = term.getCoefficient();
            Object variable = term.getVariable();

            IloNumVar num = varToNum.get(variable);
            lin.addTerm(coeff.doubleValue(), num);
        }
    }

    @Override
    public void setParameter(final SolverParameter param, final Object value) throws SolverException {
        try {
            switch (param) {
            case TIMEOUT:
                cplex.setParam(DoubleParam.TiLim, (Integer) value);
                break;
            case VERBOSE:
                int level = Math.abs((Integer) value);
                cplex.setParam(Param.Tune.Display, Math.min(3, level));
                cplex.setParam(Param.Simplex.Display, Math.min(2, level));
                cplex.setParam(Param.MIP.Display, Math.min(5, level));
                cplex.setParam(Param.Conflict.Display, Math.min(2, level));
                break;
            case ALGORITHM_TYPE:
                Algorithm algo = (Algorithm) value;
                if (Algorithm.PRIMAL == algo) {
                    cplex.setParam(IloCplex.IntParam.RootAlg, IloCplex.Algorithm.Primal);
                }
                break;
            case OUTSTREAM:
                cplex.setOut((OutputStream) value);
                break;
            case WORK_DIRECTORY:
                cplex.setParam(IloCplex.StringParam.WorkDir, (String) value);
                break;
            case NUMBER_OF_THREADS:
                cplex.setParam(IloCplex.IntParam.Threads, (Integer) value);
                break;
            case RAND_SEED:
                cplex.setParam(IloCplex.IntParam.RandomSeed, (Integer) value);
                break;
            case NODE_STORAGE_FILE_SWITCH:
                cplex.setParam(IloCplex.IntParam.NodeFileInd, (Integer) value);
                break;
            case MEMORY_EMPHASIS:
                cplex.setParam(IloCplex.BooleanParam.MemoryEmphasis, (Boolean) value);
                break;
            case ADVANCED_START_SWITCH:
                cplex.setParam(IloCplex.IntParam.AdvInd, (Integer) value);
                break;
            case BRANCH_AND_CUT_MEMORY_LIMIT:
                cplex.setParam(IloCplex.DoubleParam.TreLim, (Integer) value);
                break;
            case WORKING_MEMORY:
                cplex.setParam(IloCplex.Param.WorkMem, (Double) value);
                break;
            default:
            }
        } catch (IloException e) {
            throw new SolverException("Failed setting parameter " + param + " to " + value, e);
        } catch (ClassCastException e) {
            throw new SolverException("Wrong parameter type " + value.getClass().getSimpleName() + " for parameter " + param, e);
        }
    }

}
