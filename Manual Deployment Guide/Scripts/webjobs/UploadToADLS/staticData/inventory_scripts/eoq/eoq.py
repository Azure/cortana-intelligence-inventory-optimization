from pyomo.environ import *
def defineOptimization():

    model = AbstractModel()

    # Set of products
    model.Q = Set()

    # Budget 
    model.b = Param(within=NonNegativeReals)

    # Total demand 
    model.demand = Param(model.Q, within=NonNegativeReals)

    # Ordering cost
    model.K = Param(model.Q, within=NonNegativeReals)

    # Purchase cost
    model.C = Param(model.Q, within=NonNegativeReals)

    # Number of items of each product ordered
    model.x = Var(model.Q, within=PositiveIntegers)

    # Minimize ordering cost
    def cost_rule(model):
        return sum(model.demand[i] * model.K[i] / model.x[i] for i in model.Q)
    model.cost = Objective(rule=cost_rule)

    # Budget constraint
    def budget_rule(model):
        return sum(model.C[i] * model.x[i] / 2.0 for i in model.Q) <= model.b
    model.budget_constraint = Constraint(rule=budget_rule)

    return model


#solver = SolverFactory("xpress", solver_io="mps")
#result = solver.solve(instance)

#print("Running time (sec)")
#print(result.Solver.Time)

#instance.pprint()
