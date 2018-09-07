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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;

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
            
            File f = new File("cplex_problem.txt");
        	try {
				FileUtils.writeStringToFile(f, cplex.toString());
			} catch (IOException e) {
				e.printStackTrace();
			}

            if (!cplex.solve()) {
//            	System.out.println(cplex.toString());
            	System.out.println("CPLEX Status:" + cplex.getStatus());
            	System.out.println("CPLEX Status:" + cplex.getCplexStatus() + " / Sub:" + cplex.getCplexSubStatus());
            	
                cplex.end();
                return null;
            }

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

}
