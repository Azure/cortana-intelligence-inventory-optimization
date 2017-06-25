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

    # cost of disposal of expired products
    model.W = Param(within=NonNegativeReals)

    # maximal demand in the lead time
    model.max_lead_demand = Param(within=NonNegativeIntegers)

    # sequence of big demands
    model.big_demand = RangeSet(model.mu, model.max_lead_demand)

    # probabilities of demand of large quantities in a lead time
    model.prob_lead_demand = Param(model.big_demand, within=NonNegativeReals)

    # maximum value of s+Q during lead time + lifetime time
    model.max_s_Q = Param(within=NonNegativeIntegers)

    # minimum value of s+Q during lead time + lifetime time
    model.min_s_Q = Param(within=NonNegativeIntegers)

    # s+Q index
    model.s_Q = RangeSet(model.min_s_Q, model.max_s_Q)

    # demand during lead time + lifetime time
    model.prob_lead_lifetime = Param(model.s_Q, within=NonNegativeReals)

    # Safety stock (unknown)
    model.s = Var(within=NonNegativeIntegers)

    # Order quantity (unknown)
    model.Q = Var(within=NonNegativeIntegers)

    # Objective function: tradeoff between ordering costs, holding costs, backordering costs and disposal costs
    def cost_rule(model):

        # compute expected number of expired items
        ER = (sum((model.s + model.Q - x) * model.prob_lead_lifetime[x] * (1/(1+exp(-(log(model.s + model.Q + 1) - log(x+1))))) 
                 for x in sequence(value(model.min_s_Q), model.max_s_Q)) - 
             sum((model.s - x) * model.prob_lead_lifetime[x] * (1/(1+exp(-(log(model.s + 1) - log(x + 1))))) 
                 for x in sequence(value(model.min_s_Q), model.max_s_Q)))

        # compute expected cycle length
        ET = (model.Q - ER) / model.a

        # compute expected number of backordered items
        ES = (sum((x - model.s) * model.prob_lead_demand[x] * (1/(1+exp(-500*(log(x + 1)-log(model.s + 1))))) 
                    for x in sequence(model.mu+1,model.max_lead_demand)))

        # compute cost function
        cost_function =  ((model.K + model.pi * ES + model.W * ER) / ET + 
                          model.h * (model.Q/2.0 + model.s - model.mu))

        return cost_function

    model.cost = Objective(rule=cost_rule)

    # Solution upper bound 
    def upper_bound(model):
        return model.s + model.Q <= model.max_s_Q
    model.upper_bound = Constraint(rule=upper_bound)

    return model



