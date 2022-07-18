import xml.etree.ElementTree as ET
import json 
import sys

if len(sys.argv) < 2:
    print("usage: python dxl-parser.py FILEPATH_TO_MDP_FILE")
    exit(1)
#files = ['q1.mdp']
#prefix = "gporca/data/dxl/tpch/"
#file2 = prefix + files[int(sys.argv[1])]
file2 = sys.argv[1]
tree = ET.parse(file2)
root = tree.getroot()

#filters out the prefix 
def filter_pre(str):
    i = 0
    for c in str:
        if c == "}":
            break
        i += 1
    return str[i+1:]

#removes orca specific data that is not needed
def filter_att(dict):
    dict.pop('TypeMdid', False)
    dict.pop('OperatorMdid',False)
    dict.pop('Mdid',False)
    return dict

#parsing a comparison
def comparison(node):
    step = {"op" : node.attrib["ComparisonOperator"], "col_a" : node[0].attrib["ColId"], "col_b" : node[1].attrib["ColId"]}
    return step

#parsing a table scan
def table_scan(node, id):
    #cols = []
    #c = 0
    #for n in node[3][0]:
    #    cols.append({str(c) : n.attrib["ColId"]})
    #    c += 1
    #step = {"id" : str(id), "type" : "TableScan", "name" : node[3].attrib['TableName'], "cols" : cols}
    step = {"id" : str(id), "type" : "TableScan", "name" : node[3].attrib['TableName']}
    return step

#parsing a condition
def condition(node):
    cond_list = []
    for n in node:
        cond_list.append(comparison(n))
    return cond_list

#parsing a projection
def projection(node, id, pre):
    elems = []
    counter = 0
    for n in node:
        name = filter_pre(n[0].tag)
        if  name == 'Ident':
            col_id = n[0].attrib['ColId']
        #deprecated start: aggregation is handled in different method now
        elif name == 'AggFunc':
            if len(n[0]) > 0:
                name = filter_pre(n[0][0].tag)
                if name == 'Ident':
                    col_id = n[0][0].attrib['ColId']
            else:
                col_id = n.attrib['ColId']
        #deprecated end
        elems.append(col_id)
        #orca uses global unique columns id instead of ids that tell the postion in relation to the input table, this part maps the global ids to relative ids
        for i in range(len(pre[0])):
            if pre[0][i] == n.attrib["ColId"]:
                pre[0][i] = str(counter)
        for i in range(len(pre[1])):
            if pre[1][i]["colID"] == n.attrib["ColId"]:
                pre[1][i]["colID"] = str(counter)
        for i in range(len(pre[2])):
            if pre[2][i]["colID"] == n.attrib["ColId"]:
                pre[2][i]["colID"] = str(counter)
        counter += 1
    #extra is a temporary storage for aggregation in case that we got nested expressions inside the aggreagtion, this will be moved to the next projection
    if len(extra) > 0:
        extra_tmp = extra.pop(str(id))
        for n in extra_tmp:
            for key in n.keys():
                elems.append(n[key])
            for i in range(len(pre[1])):
                for key in n.keys():
                    if pre[1][i]["colID"] == key:
                        pre[1][i]["colID"] = str(counter)
            #q = [n]
            #while len(q) != 0:
            #    tmp = q.pop()
            #    try:
            #        if tmp["col_id"] == 
            #    except:
            #        try:
            #            tmp["value"]
            #        except:
            #            q.append(tmp["operand_a"])
            #            q.append(tmp["operand_b"])
            #counter += 1

        elems += extra_tmp
    return elems

tmp_id = 0
extra = {}

#parsing an aggregation
def aggregate(node, pre, start):
    #temporary ids are for nested aggregation, temporary also unique as the normal column ids, 
    # and also will be overwritten with relativ position ids in the next operation
    global tmp_id
    elems = []
    #statements stores nested expressions that will be added to next projection
    projections = []
    counter = start
    for n in node:
        id_c = n.attrib['ColId']
        name = filter_pre(n[0].tag)
        if name == 'AggFunc':
            #id mapping previous op
            for pre2 in pre:
                for i in range(len(pre2)):
                    if pre2[i] == id_c:
                        pre2[i] = str(counter)
            #curent supported aggregation:
            if n[0].attrib["AggMdid"] == "0.2114.1.0":
                type = "sum"
            elif n[0].attrib["AggMdid"] == "0.2103.1.0":
                type = "average"
            elif n[0].attrib["AggMdid"] == "0.2803.1.0":
                type = "count"
            
            if len(n[0]) > 0:
                if filter_pre(n[0][0].tag) == 'Ident':
                    #easy aggregate over single col
                    input = n[0][0].attrib["ColId"]
                else:
                    #nested aggregation over multiple cols / consatnt values
                    q = [n[0][0]]
                    proj = {}
                    ops = [proj]
                    while len(q) != 0:
                        tmp = q.pop()
                        op = ops.pop()
                        name = filter_pre(tmp.tag)
                        if name == 'Ident':
                            op["colID"] = tmp.attrib['ColId']
                        elif name == 'OpExpr':
                            op["operator"] = tmp.attrib["OperatorName"]
                            a = {}
                            op["left"] = a
                            b = {}
                            op["right"] = b
                            ops.append(a)
                            ops.append(b)
                            q.append(tmp[0])
                            q.append(tmp[1])
                        elif name == "ConstValue":
                            op["value"] = tmp.attrib["DoubleValue"]
                    input = "tmp" + str(tmp_id)
                    projections.append({"tmp" + str(tmp_id) : proj})
                    tmp_id += 1
            else:
                #aggregation over all
                input = "*"
            elems.append({ "type" : type, "colID" : input})
            counter += 1
        else:
            pass
    return elems, projections

#parsing a filter / selection
def filter(node, id, id2):
    list = []
    for c in node:
        list.append({"valueType":"Date","comp_op" : c.attrib["ComparisonOperator"], "left" : {"colID" : c[0].attrib["ColId"]} , "right" : {"value" : "1998-09-02"}, "double_value" : c[1].attrib["DoubleValue"]})
    return {"id" : str(id), "type" : "Filter", "id_a" : str(id2), "cond" : list}

def grouping_columns(node, pre):
    list = []
    counter = 0
    for n in node:
        for i in range(len(pre)):
            if pre[i] == n.attrib["ColId"]:
                pre[i] = str(counter)
        list.append(n.attrib["ColId"])
        counter += 1
    return list

#parsing a sort
def sorting_cols(node):
    list = []
    for n in node:
        list.append({"colID" : n.attrib["ColId"], "sort_operator" : n.attrib["SortOperatorName"]})
    return list

#not really used, needed for redistribute, broadcast, which are not used currently
def hash_exprs(node):
    list = []
    for n in node:
        list.append({"colID" : n[0].attrib["ColId"]})
    return list

#returns the logical plan from the minidump file
def prep(r):
    for n in r:
        if filter_pre(n.tag) == "Thread":
            for n2 in n:
                if filter_pre(n2.tag) == "Plan":
                    #print(n2[0].tag)
                    return n2[0]

def parse(r):
    #q is queue wjich starts with the root of plan
    q = [r]
    #list of nodes you want to ingore while parsing the tree
    #alternatively write a case adn just pass
    black_list = ["Properties"]
    #plan will be returned at the end
    plan = []
    #q_ids is a queue for the operation id_s, q_ids usually increases in size when q increases
    #alternatively make 1 queue of tuples
    q_ids = [0]
    #counter for operation ids
    ids = 0
    #temprary storage for the previous col ids, needed because the global unique ids from previous op needs to be mapped to the position in the current "output table"
    pre = {0 : []}
    #broad width search trough the tree
    while(len(q) != 0):
        node = q.pop(0)
        name = filter_pre(node.tag)
        if not name in black_list:
            #currently working cases: TableScan, LogicalGet, HashJoin, GatherMotion, Aggregate, Sort
            if name == "HashJoin":
                id = q_ids.pop(0)
                #in orca each operation has a projection at the end
                plan.insert(0,projection(node[1], id, ids + 1))
                step = {"id" : str(ids + 1),"type" : "Join", "id_a" : str(ids + 2), "id_b" : str(ids + 3), "cond" : condition(node[4])}
                #adding the join to output variable
                plan.insert(0, step)
                q.append(node[5])
                q.append(node[6])
                q_ids.append(ids + 2)
                q_ids.append(ids + 3)
                ids += 3
            elif name == 'LogicalGet':
                q.append(node[0])
            elif name == "TableScan":
                id = q_ids.pop(0)
                cols = projection(node[1], id, pre[id])
                plan.insert(0, {"id" : str(id), "type" : "Projection", "id_a" : str(ids + 1) ,'colIDs': cols})
                plan.insert(0, filter(node[2], ids + 1, ids + 2))
                plan.insert(0, table_scan(node, ids + 2))
                ids += 2
            elif name == "GatherMotion":
                id = q_ids.pop(0)
                pre[ids + 1] = []
                sort_cols = sorting_cols(node[3])
                step = {"id" : str(id),"type" : "Sort", "sorting_colIDs" : sort_cols ,"id_a" : str(ids + 1)}
                pre[ids + 1] = [[], [], sort_cols]
                plan.insert(0, step)
                q.append(node[4])
                q_ids.append(ids + 1)
                ids += 1
            elif name == "BroadcastMotion":
                pass
            elif name == "RedistributeMotion":
                pass
            elif name == "Aggregate":
                id = q_ids.pop(0)
                #using pre.pop(id, []) instead of pre[id] makes sure that the pre doesn't grow with each operation
                cols = projection(node[2],id, pre[id])
                plan.insert(0, {"id" : str(id), "type" : "Projection", "id_a" : str(ids + 1) ,'colIDs': cols})
                #currently hard coded: needed for reversing the splitting of the aggregation operation
                if filter_pre(node[4].tag) ==  "Sort":
                    tmp = filter_pre(node[4][6].tag)
                    if tmp == "RedistributeMotion":
                        tmp = filter_pre(node[4][6][5].tag)
                        if tmp == "Result":
                            tmp = filter_pre(node[4][6][5][4].tag)
                            if tmp == "Aggregate":
                                node2 = node[4][6][5][4]

                gr_cols = grouping_columns(node[1], cols)
                agg_cols, projs = aggregate(node2[2], [cols], len(gr_cols))
                step = {"id" : str(ids + 1),"type" : "Aggregate", "agg_strat" : node.attrib["AggregationStrategy"], "grouping_colIDs" : gr_cols, "agg_colIDs" : agg_cols ,"id_a" : str(ids + 2)}
                plan.insert(0, step)
                pre[id + 2] = [gr_cols, agg_cols,[],]
                extra[str(ids + 2)] = projs
                q.append(node2[4])
                q_ids.append(ids + 2)
                ids += 2
            elif name == "Sort":
                id = q_ids.pop(0)
                plan.insert(0, projection(node[1], id, ids + 1))
                step = {"id" : str(ids + 1),"type" : "Sort", "sorting_colIDs" : sorting_cols(node[3]) ,"id_a" : str(ids + 2)}
                plan.insert(0, step)
                q.append(node[6])
                q_ids.append(ids + 2)
                ids += 2
            elif name == "Result":
                pass
            else:
                #default case for unknown operations
                id = q_ids.pop(0)
                step = {"id" : str(id),"type" : name}
                plan.insert(0, step)
    return plan

result = parse(prep(root))
print(json.dumps(result, indent=4))
#removes filepath and file ending from input file name
i = 0
last = 0
end = 0
for c in file2:
    if c == '/':
        last = i
    if c == '.':
        end = i
    i += 1
#stores the result in JSON
# with open("dxl_parser_result/"+ file2[last+1:end] + '.json', 'w', encoding='utf-8') as f:
#     json.dump(result, f, ensure_ascii=False, indent=4)
# print("results written to: ./dxl_parser_result/" + file2[last+1:end] + '.json')
