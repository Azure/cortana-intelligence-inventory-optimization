from pyomo.environ import *
from math import ceil
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

    def I_init(model):
       return ((i,j) for i in model.Q for j in range(1,int(ceil(2*model.b/model.C[i]))))
    model.I = Set(initialize=I_init, dimen=2)

    # Number of items of each product ordered
    model.x = Var(model.I, within=Boolean)

    # Minimize ordering cost
    def cost_rule(model):
        return sum(model.demand[i] * model.K[i] / j * model.x[i,j] for (i,j) in model.I)
    model.cost = Objective(rule=cost_rule)

    # Budget constraint
    def budget_rule(model):
        return sum(model.C[i] * j * model.x[i,j] / 2.0 for (i,j) in model.I) <= model.b
    model.budget_constraint = Constraint(rule=budget_rule)

    # Sum constraint
    def sum_rule(model, i):
        return sum(model.x[i,j] for j in range(1,int(ceil(2*model.b/model.C[i])))) == 1
    model.sum_constraint = Constraint(model.Q, rule=sum_rule)

    return model

   