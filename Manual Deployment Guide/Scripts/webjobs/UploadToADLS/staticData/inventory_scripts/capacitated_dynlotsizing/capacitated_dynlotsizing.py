from pyomo.environ import *
def defineOptimization():

    model = AbstractModel()

    # Set of products
    model.P = Set()

    # Initial inventory
    model.init_inventory = Param(model.P, within=NonNegativeReals)

    # Ordering cost
    model.K = Param(model.P, within=NonNegativeReals)

    # Ordering holding cost
    model.h = Param(model.P, within=NonNegativeReals)

    # Volume of each item
    model.v = Param(model.P, within=NonNegativeReals)

    # number of time intervals
    model.T = Param(within=NonNegativeReals)

    # supplier capacity
    model.C = Param(within=NonNegativeReals)

    # Set of time intervals
    model.t = RangeSet(1, model.T)

    # Demand 
    model.demand = Param(model.t, model.P, within=NonNegativeReals)

    # Order size
    model.z = Var(model.t, model.P, within=NonNegativeReals)

    # Indicator if the order was made
    model.delta = Var(model.t, model.P, within=Boolean)

    # Minimize ordering cost
    def cost_rule(model):
        return sum(sum(model.K[j] * model.delta[t,j] + 
                       model.h[j] * (model.init_inventory[j] + sum(model.z[i,j] - model.demand[i,j] for i in sequence(t))) 
               for j in model.P) for t in model.t)    
    model.cost = Objective(rule=cost_rule)

    # init_inventory[j] + sum z[i,j] >= sum demand[i,j] constraint
    def order_rule(model, t, j):
        return model.init_inventory[j] + sum(model.z[i,j] for i in sequence(t)) >= sum(model.demand[i,j] for i in sequence(t))
    model.order_constraint = Constraint(model.t, model.P, rule=order_rule)

    # z[t,j] <= delta[t,j] * sum(demand[i,j])
    def delta_rule(model, t, j):
        return model.z[t,j] <= model.delta[t,j] * sum(model.demand[i,j] for i in sequence(t,model.T))
    model.delta_constraint = Constraint(model.t, model.P, rule=delta_rule)

    # sum v[j] * z[t,j] <= C
    def capacity_rule(model,t):
        return sum(model.v[j] * model.z[t,j] for j in model.P) <= model.C
    model.capacity_constraint = Constraint(model.t, rule = capacity_rule)

    return model


#f:\MIPCL\mip\bin\mps_mipcl.exe capacitated_dynlotsizing.mps


#solver = SolverFactory("xpress", solver_io="lp", is_mip = True)
#solver = SolverFactory("gurobi", solver_io="lp", is_mip = True)
#result = solver.solve(instance)

#print("Running time (sec)")
#print(result.Solver.Time)

# print solution
#print("\n Time Product Order indicator Order size")
#for i in range(instance.T.value):
#  for j in instance.P.value:
#   if instance.delta[i+1,j].value > 0.1: 
#      print("%d %s %f %f" % (i, j, instance.delta[i+1,j].value, instance.z[i+1,j].value))

#instance.pprint()

#result.write()



