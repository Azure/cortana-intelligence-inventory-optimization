from pyomo.environ import *
def defineOptimization():
    model = AbstractModel()

    # hold cost 
    model.h = Param(within=NonNegativeReals)

    # average demand in a unit of time, e.g 100 items per month
    model.a = Param(within=NonNegativeReals)
    
    #Lead time
    model.L = Param(within=NonNegativeReals)

    # purchase cost
    model.K = Param(within=NonNegativeReals)

    # backorder cost
    model.pi = Param(within=NonNegativeReals)

    # maximal demand rate
    model.max_demand_rate = Param(within=NonNegativeReals)

    # sequence of big demand rates
    model.big_demand_rate = RangeSet(model.a+1, model.max_demand_rate)

    # probabilities of demand of large rate
    model.prob_demand_rate = Param(model.big_demand_rate, within=NonNegativeReals)

    # Review Interval
    model.R = Var(within=PositiveReals)

    # Order level
    model.S = Var(within=NonNegativeReals)

    # End of cycle inventory can NOT be negative, sales can NOT be greater than order
    def OrderSalesRule(model):
        return(model.S - model.a * model.L - model.a *model.R >= 0)
    model.OrderGreaterThanSales = Constraint(rule=OrderSalesRule)

    # Objective function: tradeoff between ordering costs, holding costs and backordering costs
    def cost_rule(model):
        return (model.h * (model.S - model.a * model.L - model.a * model.R/2.0) + model.K / model.R + 
                model.pi / model.R *
                sum(model.prob_demand_rate[x] * (1/(1+exp(-10*(x-model.S/(model.R + model.L))))) 
                    for x in sequence(model.a+1,model.max_demand_rate)))					
    
    model.cost = Objective(rule=cost_rule)

    return model
