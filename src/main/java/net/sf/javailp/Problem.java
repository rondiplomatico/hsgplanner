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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Getter;

/**
 * The class {@code Problem} represents a linear problem consisting of multiple
 * constraints and up to one objective function.
 *
 * @author lukasiewycz
 *
 */
public class Problem {

    protected Linear objective = null;
    protected OptType optType = OptType.MIN;
    protected final List<Constraint> constraints = new ArrayList<Constraint>();
    @Getter protected final List<SOS> soss = new ArrayList<>();
    @Getter protected final List<GUB> gubs = new ArrayList<>();

    protected final Set<Object> variables = new HashSet<Object>();
    protected final Map<Object, VarType> varType = new HashMap<Object, VarType>();
    protected final Map<Object, Number> varLowerBound = new HashMap<Object, Number>();
    protected final Map<Object, Number> varUpperBound = new HashMap<Object, Number>();

    /**
     * Constructs a {@code Problem}.
     */
    public Problem() {
        super();
    }

    /**
     * Returns the objective function.
     *
     * @return the objective function
     */
    public Linear getObjective() {
        return objective;
    }

    /**
     * Sets the objective function.
     *
     * @param objective
     *            the objective function
     */
    public void setObjective(final Linear objective) {
        for (Term term : objective) {
            variables.add(term.getVariable());
        }
        Linear linear = new Linear(objective);
        this.objective = linear;
    }

    /**
     * Sets the objective function.
     *
     * @param objective
     *            the objective function
     * @param optType
     *            the optimization type
     */
    public void setObjective(final Linear objective, final OptType optType) {
        setObjective(objective);
        setOptimizationType(optType);
    }

    /**
     * Sets the objective function.
     *
     * @param objective
     *            the objective function
     * @param optType
     *            the optimization type (min,max)
     */
    public void setObjective(final Linear objective, final String optType) {
        setObjective(objective);

        if (optType.equalsIgnoreCase("min")) {
            setOptimizationType(OptType.MIN);
        } else if (optType.equalsIgnoreCase("max")) {
            setOptimizationType(OptType.MAX);
        } else {
            System.err.println("Unknown optType: " + optType + " (current optimization type is " + this.optType + ")");
        }

    }

    /**
     * Sets the optimization type.
     *
     * @param optType
     *            the optimization type to be set
     */
    public void setOptimizationType(final OptType optType) {
        this.optType = optType;
    }

    /**
     * Returns the optimization type.
     *
     * @return the optimization type
     */
    public OptType getOptType() {
        return optType;
    }

    /**
     * Returns the constraints.
     *
     * @return the constraints.
     */
    public List<Constraint> getConstraints() {
        return constraints;
    }

    /**
     * Returns the number of objectives.
     *
     * @return the number of objectives
     */
    public int getConstraintsCount() {
        return constraints.size();
    }

    /**
     * Returns the variables.
     *
     * @return the variables
     */
    public Collection<Object> getVariables() {
        return variables;
    }

    /**
     * Returns the number of variables.
     *
     * @return the number of variables
     */
    public int getVariablesCount() {
        return variables.size();
    }

    /**
     * Adds a constraint.
     *
     * @param constraint
     *            the constraint to be added
     */
    public void add(final Constraint constraint) {
        for (Term term : constraint.getLhs()) {
            variables.add(term.getVariable());
        }
        constraints.add(constraint);
    }

    public void add(final SOS sos) {
        for (Term term : sos.getLhs()) {
            variables.add(term.getVariable());
        }
        constraints.add(new Constraint(sos.getName(), sos.getLhs(), Operator.EQ, 1));
        soss.add(sos);
    }

    public void add(final GUB gub) {
        for (Term term : gub.getLhs()) {
            variables.add(term.getVariable());
        }
        gubs.add(gub);
    }

    /**
     * Adds a constraint.
     *
     * @param lhs
     *            the left-hand-side linear expression
     * @param operator
     *            the operator
     * @param rhs
     *            the right-hand-side number
     */
    public void add(final Linear lhs, final Operator operator, final Number rhs) {
        Linear linear = new Linear(lhs);
        Constraint constraint = new Constraint(linear, operator, rhs);
        add(constraint);
    }

    /**
     * Adds a constraint.
     *
     * @param lhs
     *            the left-hand-side linear expression
     * @param operator
     *            the operator (<=,=,>=)
     * @param rhs
     *            the right-hand-side number
     */
    public void add(final Linear lhs, final String operator, final Number rhs) {
        final Operator o;
        if (operator.equals("<=")) {
            o = Operator.LE;
        } else if (operator.equals("=")) {
            o = Operator.EQ;
        } else if (operator.equals(">=")) {
            o = Operator.GE;
        } else {
            throw new IllegalArgumentException("Unknown Boolean operator: " + operator);
        }
        add(lhs, o, rhs);
    }

    /**
     * Adds a constraint.
     *
     * @param name
     *            the name of the constraint
     * @param lhs
     *            the left-hand-side linear expression
     * @param operator
     *            the operator
     * @param rhs
     *            the right-hand-side number
     */
    public void add(final String name, final Linear lhs, final Operator operator, final Number rhs) {
        Linear linear = new Linear(lhs);
        Constraint constraint = new Constraint(name, linear, operator, rhs);
        add(constraint);
    }

    /**
     * Adds a constraint.
     *
     * @param name
     *            the name of the constraint
     * @param lhs
     *            the left-hand-side linear expression
     * @param operator
     *            the operator (<=,=,>=)
     * @param rhs
     *            the right-hand-side number
     */
    public void add(final String name, final Linear lhs, final String operator, final Number rhs) {
        final Operator o;
        if (operator.equals("<=")) {
            o = Operator.LE;
        } else if (operator.equals("=")) {
            o = Operator.EQ;
        } else if (operator.equals(">=")) {
            o = Operator.GE;
        } else {
            throw new IllegalArgumentException("Unknown Boolean operator: " + operator);
        }
        add(name, lhs, o, rhs);
    }

    /**
     * Returns the variable type.
     *
     * @param variable
     *            the variable
     * @return the type
     */
    public VarType getVarType(final Object variable) {
        VarType type = varType.get(variable);
        if (type != null) {
            return type;
        } else {
            return VarType.REAL;
        }
    }

    /**
     * Sets the variable type of one variable.
     *
     * @param variable
     *            the variable
     * @param type
     *            the type
     */
    public void setVarType(final Object variable, final VarType type) {
        varType.put(variable, type);
    }

    /**
     * Sets the variable type of one variable. The allowed types are Integer,
     * Boolean, and Double.
     *
     * @param variable
     *            the variable
     * @param type
     *            the type
     */
    public void setVarType(final Object variable, final Class<?> type) {
        try {
            final VarType t;
            if (type.equals(Integer.class)) {
                t = VarType.INT;
            } else if (type.equals(Boolean.class)) {
                t = VarType.BOOL;
            } else if (type.equals(Double.class)) {
                t = VarType.REAL;
            } else {
                throw new IllegalArgumentException();
            }
            varType.put(variable, t);
        } catch (IllegalArgumentException e) {
            System.err.println(type + " is an unknown type");
        }
    }

    /**
     * Returns the lower bound of a variable.
     *
     * @param variable
     *            the lower bound
     * @return the variable or {@code null} if no lower bound exists
     */
    public Number getVarLowerBound(final Object variable) {
        return varLowerBound.get(variable);
    }

    /**
     * Returns the upper bound of a variable.
     *
     * @param variable
     *            the upper bound
     * @return the variable or {@code null} if no upper bound exists
     */
    public Number getVarUpperBound(final Object variable) {
        return varUpperBound.get(variable);
    }

    /**
     * Sets the lower bound of a variable.
     *
     * @param variable
     *            the variable
     * @param value
     *            the lower bound value
     */
    public void setVarLowerBound(final Object variable, final Number value) {
        varLowerBound.put(variable, value);
    }

    /**
     * Sets the upper bound of a variable.
     *
     * @param variable
     *            the variable
     * @param value
     *            the upper bound value
     */
    public void setVarUpperBound(final Object variable, final Number value) {
        varUpperBound.put(variable, value);
    }

    /**
     * Sets the lower and upper bounds of a variable.
     *
     * @param lower
     *            the lower bound
     * @param variable
     *            the variable
     * @param upper
     *            the upper bound
     */
    public void setVarBounds(final Number lower, final Object variable, final Number upper) {
        setVarLowerBound(variable, lower);
        setVarUpperBound(variable, upper);
    }

    /**
     * Sets the lower and upper bounds of a variable and the variable type.
     *
     * @param lower
     *            the lower bound
     * @param variable
     *            the variable
     * @param upper
     *            the upper bound
     * @param type
     *            the variable type
     */
    public void setVarBoundsAndType(final Number lower, final Object variable, final Number upper, final Class<?> type) {
        setVarLowerBound(variable, lower);
        setVarUpperBound(variable, upper);
        setVarType(variable, type);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true, "UTF-8")) {
            print(ps);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Encoding " + StandardCharsets.UTF_8 + " not supported.", e);
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    public void print() {
        print(System.out);
    }

    private void print(final PrintStream s) {
        if (objective != null) {
            s.println(optType);
            s.println(" " + objective);
        } else {
            s.println("Find one solution");
        }
        s.println("Subject To");
        for (Constraint constraint : getConstraints()) {
            s.println(" " + constraint);
        }
        for (SOS sos : getSoss()) {
            s.println(" " + sos);
        }
        s.println("Bounds");
        for (Object variable : getVariables()) {
            Number lb = getVarLowerBound(variable);
            Number ub = getVarUpperBound(variable);

            if (lb != null || ub != null) {
                s.print(" ");
                if (lb != null) {
                    s.print(lb + " <= ");
                }
                s.print(variable);
                if (ub != null) {
                    s.print(" <= " + ub);
                }
                s.println("");
            }
        }

        s.println("Variables");
        for (Object variable : getVariables()) {
            s.println(" " + variable + " " + getVarType(variable));
        }
    }

}
