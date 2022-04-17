import sys
import csv
import pprint
from moz_sql_parser import parse
from itertools import product, chain


def readMetadata(metadata_file):

    metadata = open(metadata_file, 'r')
    Lines = metadata.readlines()

    db = dict()
    for line_no in range(len(Lines)):

        if Lines[line_no].strip() == "<begin_table>":
            line_no += 1
            table_name = Lines[line_no].strip()
            db[table_name] = dict()
            line_no += 1
            db[table_name]["Columns"] = []
            while Lines[line_no].strip() != "<end_table>":
                db[table_name]["Columns"].append(
                    Lines[line_no].strip().lower())
                line_no += 1

    return db


def getData(db):
    try:
        tables = list(db.keys())
        for table in tables:
            file = '../files/' + table + '.csv'
            with open(file, 'r') as csv_file:
                reader = csv.reader(csv_file)
                db[table]["data"] = []
                for row in reader:
                    db[table]["data"].append([int(x) for x in row])
        return db

    except Exception:
        print("Error: Unable to get data")
        exit()


def printOutput(col_names, data):
    print(col_names)
    for row in data:
        print(','.join(map(str, row)))


def processStatement(statement, db):

    stripped = statement.strip()
    if stripped[-1] != ";":
        print("Error: Invalid Syntax")
        exit()

    try:
        parsed = parse(statement)
    except Exception:
        print("Error: Invalid Query")
        exit()
    # print(parsed)
    # print(db)

    # Getting the list of tables
    tables = [parsed['from']] if isinstance(
        parsed['from'], str) else parsed['from']
    # print(tables)

    # Getting all the data from the selected tables
    try:
        temp = [db[table]["Columns"] for table in tables]
        cols_list = list(chain(*temp))
        data = [db[table]["data"] for table in tables]
    except Exception:
        print("Error: Invalid Query")
        exit()

    # Joining data using cartesian product
    temp = product(*data)
    Joined_data = []

    for row in temp:
        Joined_data.append(list(chain(*list(row))))

    # pprint.pprint(Joined_data)
    # pprint.pprint(cols_list)
    selected_cols_list = []
    agg_funcs = []
    distinct = False
    try:
        # Select * Case
        if isinstance(parsed['select'], str):
            if parsed['select'] == "*":
                # list all columns
                selected_cols_list = cols_list
            else:
                raise Exception

        # Multiple Columns case
        elif isinstance(parsed['select'], list):
            for col_dict in parsed['select']:
                if isinstance(col_dict['value'], dict):
                    func = list(col_dict['value'].keys())[0]
                    assert func in ["sum", "avg", "max", "min",
                                    "count"], "Error: Invalid Syntax"
                    agg_funcs.append((func, col_dict['value'][func]))
                    selected_cols_list.append(col_dict['value'][func])
                else:
                    selected_cols_list.append(col_dict['value'])

        # Single Column / Distinct
        elif isinstance(parsed['select'], dict):
            if isinstance(parsed['select']['value'], str):
                selected_cols_list.append(parsed['select']['value'])

            elif isinstance(parsed['select']['value'], dict):

                if "distinct" in parsed['select']['value'].keys():
                    distinct = True
                    if isinstance(parsed['select']['value']['distinct'], list):
                        for col_dict in parsed['select']['value']['distinct']:
                            if isinstance(col_dict['value'], dict):
                                func = list(col_dict['value'].keys())[0]
                                assert func in ["sum", "avg", "max", "min",
                                                "count"], "Error: Invalid Syntax"
                                agg_funcs.append(
                                    (func, col_dict['value'][func]))
                                selected_cols_list.append(
                                    col_dict['value'][func])
                            else:
                                selected_cols_list.append(col_dict['value'])

                    if isinstance(parsed['select']['value']['distinct'], dict):
                        if parsed['select']['value']['distinct']['value'] == "*":
                            selected_cols_list = cols_list
                        else:
                            selected_cols_list.append(
                                parsed['select']['value']['distinct']['value'])
                else:
                    func = list(parsed['select']['value'].keys())[0]
                    assert func in ["sum", "avg", "max", "min",
                                    "count"], "Error: Invalid Syntax"
                    agg_funcs.append((func, parsed['select']['value'][func]))
                    selected_cols_list.append(parsed['select']['value'][func])

    except Exception:
        print("Error: Invalid Query")
        exit()

    filtered_data = []

    def getVal(x, row): return row[cols_list.index(
        x)] if isinstance(x, str) else x

    # Handle 'Where'
    if "where" in parsed.keys():
        op = "&" if "and" in parsed["where"].keys(
        ) else "|" if "or" in parsed["where"].keys() else ""
        operators = {
            "gt": ">", "lt": "<", "gte": ">=", "eq": "==", "lte": "<="
        }
        for row in Joined_data:
            try:
                if op == "":
                    rel_op = list(parsed["where"].keys())[0]
                    comp_args = parsed["where"][rel_op]
                    eval_out = str(
                        getVal(comp_args[0], row)) + operators[rel_op] + str(getVal(comp_args[1], row))
                    if eval(eval_out):
                        filtered_data.append(row)
                else:
                    cond_op = list(parsed["where"].keys())[0]
                    eval_out = ""
                    for condition in parsed["where"][cond_op]:
                        rel_op = list(condition.keys())[0]
                        comp_args = condition[rel_op]
                        temp = "(" + str(getVal(comp_args[0], row)) + operators[rel_op] + str(
                            getVal(comp_args[1], row)) + ") "
                        eval_out += temp
                        eval_out += op
                    if eval(eval_out[:-1]):
                        filtered_data.append(row)

            except:
                print("Error: Invalid Query")
                exit()
        # pprint.pprint(filtered_data)
    else:
        filtered_data = Joined_data

    def agg_func_handle(func, col):
        if func == "sum":
            return sum(col)
        elif func == "max":
            maxm = max(col)
            return maxm, col.index(maxm)
        elif func == "min":
            minm = min(col)
            return minm, col.index(minm)
        elif func == "count":
            return len(col)
        elif func == "avg":
            return sum(col)/len(col)
        else:
            print("Error: Invalid Query")
            exit()

    def getTableName(col):
        for table in db.keys():
            if col in db[table]['Columns']:
                return table

    def orderby_handler(data):
        if "orderby" in parsed.keys():
            col_orderby = parsed["orderby"]["value"]
            sort = parsed["orderby"]["sort"]
            if sort == "asc":
                filtered_data = sorted(
                    data, key=lambda x: x[cols_list.index(col_orderby)])
            elif sort == "desc":
                filtered_data = sorted(
                    data, key=lambda x: x[cols_list.index(col_orderby)], reverse=True)
            else:
                print("Error: Invalid Syntax")
                exit()
        else:
            filtered_data = data
        return filtered_data

    def select_handler(filtered_data):
        col_inds = [cols_list.index(i) for i in selected_cols_list]
        selection_data = [[row[i] for i in col_inds]for row in filtered_data]
        return selection_data

    def distinct_handler(selection_data):
        distinct_data = []
        if distinct == True:
            for row in selection_data:
                if row not in distinct_data:
                    distinct_data.append(row)
        else:
            distinct_data = selection_data

        return distinct_data

    col_names = ""
    try:
        # Handle group by
        if "groupby" in parsed.keys():
            colname = parsed["groupby"]["value"]
            groupby_dict = {getVal(colname, row): [] for row in filtered_data}
            for row in filtered_data:
                groupby_dict[getVal(colname, row)].append(row)

            grouped_data = []
            for k, data in groupby_dict.items():
                agg_applied = data[0]
                for agg in agg_funcs:
                    ind_col = cols_list.index(agg[1])
                    col_data = [row[ind_col] for row in data]
                    agg_func_out = agg_func_handle(agg[0], col_data)
                    if agg[0] == "min" or agg[0] == "max":
                        temp = data[agg_func_out[1]]
                        temp[cols_list.index(agg[1])] = agg_func_out[0]
                        agg_applied = temp
                    else:
                        agg_applied[cols_list.index(agg[1])] = agg_func_out
                grouped_data.append(agg_applied)

            # Handle Order by
            filtered_data = orderby_handler(grouped_data)

            # Handle Select cols
            selection_data = select_handler(filtered_data)

            # Handle Distinct
            filtered_data = distinct_handler(selection_data)

            # pprint.pprint(filtered_data)

            # pprint.pprint(groupby_dict)
            # pprint.pprint(grouped_data)

        # No Group by
        else:
            # Handle Aggregate funcs
            agg_applied = [filtered_data[0]] if len(
                agg_funcs) > 0 else filtered_data
            for agg in agg_funcs:
                ind_col = cols_list.index(agg[1])
                col_data = [row[ind_col] for row in filtered_data]
                agg_func_out = agg_func_handle(agg[0], col_data)
                if agg[0] == "min" or agg[0] == "max":
                    temp = filtered_data[agg_func_out[1]]
                    temp[cols_list.index(agg[1])] = agg_func_out[0]
                    agg_applied[0] = temp
                else:
                    agg_applied[0][cols_list.index(agg[1])] = agg_func_out

            # Handle Order by
            filtered_data = orderby_handler(agg_applied)

            # Handle Select cols
            selection_data = select_handler(filtered_data)

            # Handle Distinct
            filtered_data = distinct_handler(selection_data)

            # pprint.pprint(filtered_data)
        for col in selected_cols_list:
            flag = False
            for agg in agg_funcs:
                if col == agg[1]:
                    col_names += agg[0]
                    col_names += "("
                    flag = True
                    break
            col_names += getTableName(col)
            col_names += "."
            col_names += col
            if flag:
                col_names += ")"
            col_names += ","
        printOutput(col_names[:-1], filtered_data)
    except:
        print("Error: Invalid Query")
        exit()


def main():

    metadata_file = '../files/metadata.txt'
    db = readMetadata(metadata_file)
    db = getData(db)
    # pprint.pprint(db)
    if len(sys.argv) <= 1:
        print("Error: Please Enter the sql statement to execute.")
        exit(0)
    statement = sys.argv[1].lower()
    processStatement(statement, db)


main()
