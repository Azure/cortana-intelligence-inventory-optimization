import subprocess
from pyomo.opt import SolverResults,SolutionStatus, SolverStatus, TerminationCondition
def solve(instance, solver_path, definition_file):

    sol_file = definition_file.split('.')[0] + '.sol'
    # write the model to a file
    _, smap_id = instance.write(definition_file)

    # Solve the problem
    rc = subprocess.call(solver_path + ' ' + definition_file,shell=True)
    assert rc == 0

    # parse the solution into a result object
    result = SolverResults()
    result._smap_id = smap_id
    
    fin = open(sol_file)

    #parse the first line which contains the objective function value or solution status
    line = fin.readline()
    #line = line.replace('\n','')
    line_split = line.split(' ')
    if len(line_split) > 1:
        if line_split[0] == '=obj=':
            msg = "Optimal objective function value is " + line_split[1]
            objno_message = "OPTIMAL SOLUTION FOUND!"
            result.solver.termination_condition = TerminationCondition.optimal
            result.solver.status = SolverStatus.ok
            soln_status = SolutionStatus.optimal
        else:
            msg = "Solution status is " + line
            objno_message = "Solution status unkown, check output log for details."
            result.solver.termination_condition = TerminationCondition.unknown
            result.solver.status = SolverStatus.unknown
            soln_status = SolutionStatus.unknown
    else:
        msg = "Solution status is " + line
        objno_message = "Solution status unkown, check output log for details."
        result.solver.termination_condition = TerminationCondition.unknown
        result.solver.status = SolverStatus.unknown
        soln_status = SolutionStatus.unknown

    result.solver.message = msg

    #parse the rest of the file with variable values
    x_names = []
    x = []
    line = fin.readline()
    if line.strip() == "":
            line = fin.readline()
    while line:
        #if line[0] == '\n' or (line[0] == '\r' and line[1] == '\n'):
        #    break
        if line[0] == 'x':
            line = line.replace('\n','')
            line_split = line.split(' ')
            x_names.append(line_split[0])
            x.append(float(line_split[1]))      
        line = fin.readline()


    if result.solver.termination_condition in [TerminationCondition.unknown,
                                            TerminationCondition.maxIterations,
                                            TerminationCondition.minFunctionValue,
                                            TerminationCondition.minStepLength,
                                            TerminationCondition.globallyOptimal,
                                            TerminationCondition.locallyOptimal,
                                            TerminationCondition.optimal,
                                            TerminationCondition.maxEvaluations,
                                            TerminationCondition.other,
                                            TerminationCondition.infeasible]:

            soln = result.solution.add()
            result.solution.status = soln_status
            soln.status_description = objno_message
            soln.message = msg.strip()
            soln.message = result.solver.message.replace("\n","; ")
            soln_variable = soln.variable
            i = 1
            for var_name, var_value in zip(x_names,x):
                #soln_variable["x"+str(i)] = {"Value" : var_value}
                soln_variable[var_name] = {"Value" : var_value}
                i = i + 1
    
    return result


'''
    instance.solutions.load_from(result)
    
    with open('mipcl_parser_test_big.csv', 'w') as f:
        for var in instance.component_data_objects(Var):
            print(var)
            var.value
            f.write('%s,%s\n' % (var, var.value))

    problem_writer = pyomo.opt.WriterFactory('mps')
    solver_capability = lambda x: True
    io_options={}
    (filename, smap) = problem_writer(instance,'problem_writer_test.mps',solver_capability,io_options)

    list(smap.bySymbol)

    smap_id = id(smap)

    instance.solutions.add_symbol_map(smap)

class ModelSolution(object):

    def __init__(self):
        self._metadata = {}
        self._metadata['status'] = None
        self._metadata['message'] = None
        self._metadata['gap'] = None
        self._entry = {}
        #
        # entry[name]: id -> (object weakref, entry)
        #
        for name in ['objective', 'variable', 'constraint', 'problem']:
            self._entry[name] = {}

    def __getattr__(self, name):
        if name[0] == '_':
            return self.__dict__[name]
        return self.__dict__['_metadata'][name]

    def __setattr__(self, name, val):
        if name[0] == '_':
            self.__dict__[name] = val
            return
        self.__dict__['_metadata'][name] = val


results = result
solution = results.solution[1]

soln = ModelSolution()
soln._metadata['status'] = solution.status
soln._metadata['message'] = solution.message

smap = instance.solutions.symbol_map[smap_id]
ignore_missing_symbols = True

for symb, val in iteritems(getattr(solution, 'variable')):
    print(symb)
    if symb in smap.bySymbol:
        obj = smap.bySymbol[symb]
    elif symb in smap.aliases:
        obj = smap.aliases[symb]
    elif ignore_missing_symbols:
        continue
    else:                                   #pragma:nocover
        #
        # This should never happen ...
        #
        raise RuntimeError(
            "ERROR: Symbol %s is missing from "
            "model %s when loading with a symbol map!"
            % (symb, instance.name))
    tmp[id(obj())] = (obj, val)
    '''


