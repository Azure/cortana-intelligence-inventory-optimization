from pyomo.environ import *
from scipy.stats import poisson

def defineOptimization():
    
    model = AbstractModel()

    # Set of orders
    model.S = Set()

    # Budget 
    model.V = Param(within=NonNegativeReals)

    # Purchase cost
    model.c = Param(model.S, within=NonNegativeReals)

    # Holding/refurbishing cost
    model.h = Param(model.S, within=NonNegativeReals)

    # Expected demand
    model.demand = Param(model.S, within=NonNegativeReals)

    # Cost of the lost sale due to the shortage of supply (economic cost)
    model.b = Param(model.S, within=NonNegativeReals)

    # Number of items of each product ordered
    model.s = Var(model.S, within=NonNegativeIntegers, initialize=0)

    # Minimize ordering cost
    def cost_rule(model):

        return sum(model.h[i] * (model.s[i] - model.demand[i]) + 
                   (model.h[i] + model.b[i]) * sum((x - model.s[i]) * poisson.pmf(x,model.demand[i]) * (1/(1+exp(-10*(log(x+1)-log(model.s[i]+1))))) 
                                                   for x in sequence(0, int(model.V / model.c[i]))) 
                                               # we have to sum from 0 and not from s[i] since the indices of sum are determined 
                                               # at compilation time
                                               # 1/(1+exp(...)) term is an approximation of the step function 
                                               # f(x,s[i]) = 1 if x>s[i], f(x,s[i])=0 if x<=s[i]
                   for i in model.S)

    model.cost = Objective(rule=cost_rule)

    # Budget constraint
    def budget_rule(model):
        return summation(model.c, model.s) <= model.V

    model.budget_constraint = Constraint(rule=budget_rule)

    return model


