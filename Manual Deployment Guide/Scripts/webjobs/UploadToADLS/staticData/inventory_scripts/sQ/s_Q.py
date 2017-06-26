from pyomo.environ import *
def defineOptimization():
    model = AbstractModel()

    # storage cost per day
    model.h = Param(within=NonNegativeReals)

    # average demand per day
    model.a = Param(within=NonNegativeIntegers)

    # average demand in a lead time 
    model.mu = Param(within=NonNegativeIntegers)

    # purchase cost
    model.K = Param(within=NonNegativeReals)

    # backorder cost
    model.pi = Param(within=NonNegativeReals)

    # maximal demand in the lead time
    model.max_demand = Param(within=NonNegativeIntegers)

    # sequence of big demands
    model.big_demand = RangeSet(model.mu+1, model.max_demand)

    # probabilities of demand of large quantities in a lead time
    model.prob_demand = Param(model.big_demand, within=NonNegativeReals)

    # Safety stock
    model.s = Var(within=NonNegativeIntegers)

    # Order quantity
    model.Q = Var(within=NonNegativeIntegers)

    # Objective function: tradeoff between ordering costs, holding costs and backordering costs
    def cost_rule(model):
        return (model.h * (model.Q/2.0 + model.s - model.mu) + model.K * model.a / model.Q + 
                model.a * model.pi / model.Q *
                sum((x - model.s) * model.prob_demand[x] * (1/(1+exp(-1000*(log(x+1)-log(model.s+1))))) 
                    for x in sequence(model.mu+1,model.max_demand)))

    model.cost = Objective(rule=cost_rule)

    return model
