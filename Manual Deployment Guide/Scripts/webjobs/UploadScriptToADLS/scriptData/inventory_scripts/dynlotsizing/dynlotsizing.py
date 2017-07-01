from pyomo.environ import *
from numpy import arange
def defineOptimization():
    model = AbstractModel()

    # initial inventory
    model.init_inventory = Param(within=NonNegativeReals)

    # number of time intervals
    model.T = Param(within=NonNegativeReals)

    # Set of time intervals
    model.t = RangeSet(1, model.T)

    # Ordering cost
    model.K = Param(within=NonNegativeReals)

    # Ordering holding cost
    model.h = Param(within=NonNegativeReals)

    # Demand 
    model.demand = Param(model.t, within=NonNegativeReals)

    # Order size
    model.z = Var(model.t, within=NonNegativeReals)

    # Indicator if the order was made
    model.delta = Var(model.t, within=Boolean)

    # Minimize ordering cost
    def cost_rule(model):
        return sum(model.K * model.delta[t] + 
                   model.h * (model.init_inventory + sum(model.z[i] - model.demand[i] for i in sequence(t))) for t in model.t)    
    model.cost = Objective(rule=cost_rule)

    # init_inventory + sum z[i] >= sum demand[i] constraint
    def order_rule(model, t):
        return model.init_inventory + sum(model.z[i] for i in sequence(t)) >= sum(model.demand[i] for i in sequence(t))
    model.order_constraint = Constraint(model.t, rule=order_rule)

    # z[t] <= delta[t] * sum(demand[i])
    def delta_rule(model,t):
        return model.z[t] <= model.delta[t] * sum(model.demand[i] for i in sequence(t,model.T))
    model.delta_constraint = Constraint(model.t, rule=delta_rule)

    return model

#solver = SolverFactory("xpress", solver_io="lp", is_mip = True)
#result = solver.solve(instance)

#print("Running time (sec)")
#print(result.Solver.Time)

# print solution
#print("\nOrder indicator")
#for i in range(instance.T.value):
#  print("%f" % instance.delta[i+1].value)

#print("\nOrder size")
#for i in range(instance.T.value):
#  print("%f" % instance.z[i+1].value)





