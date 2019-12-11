import json

def plot_graph_for_json(jsonfilename):
    with open(jsonfilename) as f:
        data = json.load(f)
    dtct = {}
    dtctnew = {}
    for col in data["columns"]:
        if col["column_name"] not in dtctnew.keys():
                dtctnew[col["column_name"]] = col["number_distinct_values"]
        for tp in col["dataTypes"]:
            dt = tp["type"]
            ct = tp["count"]
            dis = tp["number_distinct_values"]
            if dt not in dtct.keys():
                dtct[dt] = ct
            else:
                dtct[dt] += ct
    
    filename = jsonfilename.split('.')[0]
    plt.bar(range(len(dtct)), list(dtct.values()), align='center')
    st=0
    for x,y in dtct.items():
        x=st
        label = y
        plt.annotate(label,(x,y),textcoords="offset points",xytext=(0,2),ha='center',weight='bold')
        st+=1
    plt.xticks(range(len(dtct)), list(dtct.keys()))
    plt.title("Different datatypes in 3aka-ggej.tsv.gz.json", weight="bold")
#     plt.title("Different datatypes in {}".format(jsonfilename), weight='bold')
    plt.show()

def plot_non_null_values(jsonfilename):
    with open(jsonfilename) as f:
        data = json.load(f)
    dtct = {}
    for col in data["columns"]:
        if col["column_name"] not in dtct.keys():
                dtct[col["column_name"]] = col["number_non_empty_cells"]

    filename = jsonfilename.split('.')[0]
    plt.bar(range(len(dtct)), list(dtct.values()), align='center')
    st=0
    for x,y in dtct.items():
        x=st
        label = y
        plt.annotate(label,(x,y),textcoords="offset points",xytext=(0,2),ha='center',weight='bold')
        st+=1
    plt.xticks(range(len(dtct)), list(dtct.keys()))
    plt.title("Non null values in 3aka-ggej.tsv.gz.json", weight="bold")
#     plt.title("Different datatypes in {}".format(jsonfilename), weight='bold')
    plt.show()


def plot_distinct_values(jsonfilename):
    with open(jsonfilename) as f:
        data = json.load(f)
    dtct = {}
    for col in data["columns"]:
        if col["column_name"] not in dtct.keys():
                dtct[col["column_name"]] = col["number_distinct_values"]

    filename = jsonfilename.split('.')[0]
    plt.bar(range(len(dtct)), list(dtct.values()), align='center')
    st=0
    for x,y in dtct.items():
        x=st
        label = y
        plt.annotate(label,(x,y),textcoords="offset points",xytext=(0,2),ha='center',weight='bold')
        st+=1
    plt.xticks(range(len(dtct)), list(dtct.keys()))
    plt.title("Distinct values in 3aka-ggej.tsv.gz.json", weight="bold")
#     plt.title("Different datatypes in {}".format(jsonfilename), weight='bold')
    plt.show()


