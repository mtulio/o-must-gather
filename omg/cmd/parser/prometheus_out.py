# -*- coding: utf-8 -*-
from tabulate import tabulate
from omg.common.helper import (
    fmt_sizeof,
    fmt_countof,
    fmt_date_from_ts,
    load_json_file
)


def prom_status_tsdb(buffer=None):
    """
    Show Prometheus Status TSDB.
    """
    files = [
        "prometheus/prometheus-k8s-0/status/tsdb.json",
        "prometheus/prometheus-k8s-1/status/tsdb.json"
    ]
    err = False
    table_fmt = "pretty"

    repl0, err = load_json_file(files[0])
    if err:
        print(f"Error loading file {files[0]}")
        return

    repl1, err = load_json_file(files[1])
    if err:
        print(f"Error loading file {files[1]}")
        return

    print(">> headStats <<")
    output_res = []
    output_header = [
        "metric",
        "prometheus-k8s-0",
        "prometheus-k8s-1"
    ]

    output_res.append([
        "numSeries",
        fmt_countof(repl0["data"]["headStats"]["numSeries"]),
        fmt_countof(repl1["data"]["headStats"]["numSeries"])
    ])
    output_res.append([
        "chunkCount",
        fmt_countof(repl0["data"]["headStats"]["chunkCount"]),
        fmt_countof(repl1["data"]["headStats"]["chunkCount"])
    ])
    output_res.append([
        "minTime",
        fmt_date_from_ts(repl0["data"]["headStats"]["minTime"]),
        fmt_date_from_ts(repl1["data"]["headStats"]["minTime"])
    ])
    output_res.append([
        "maxTime",
        fmt_date_from_ts(repl0["data"]["headStats"]["maxTime"]),
        fmt_date_from_ts(repl1["data"]["headStats"]["maxTime"])
    ])

    print(tabulate(output_res, headers=output_header, tablefmt=table_fmt))

    print("\n>> seriesCountByMetricName <<")
    metric_name = "seriesCountByMetricName"
    output_res = []
    output_header = [
        "Top#N",
        "MetricName(prometheus-k8s-0)",
        "Count(prometheus-k8s-0)",
        "MetricName(prometheus-k8s-1)",
        "Count(prometheus-k8s-1)"
    ]

    for idx in range(0, len(repl0["data"][metric_name])):
        output_res.append([
            f"#{str(idx+1)}",
            repl0["data"][metric_name][idx]["name"],
            fmt_countof(repl0["data"][metric_name][idx]["value"]),
            repl1["data"][metric_name][idx]["name"],
            fmt_countof(repl1["data"][metric_name][idx]["value"])
        ])

    print(tabulate(output_res, headers=output_header, tablefmt=table_fmt, colalign=("right")))

    print("\n>> labelValueCountByLabelName <<")
    metric_name = "labelValueCountByLabelName"
    output_res = []
    output_header = [
        "Top#N",
        "MetricName(prometheus-k8s-0)",
        "Count(prometheus-k8s-0)",
        "MetricName(prometheus-k8s-1)",
        "Count(prometheus-k8s-1)"
    ]

    for idx in range(0, len(repl0["data"][metric_name])):
        output_res.append([
            f"#{str(idx+1)}",
            repl0["data"][metric_name][idx]["name"],
            fmt_countof(repl0["data"][metric_name][idx]["value"]),
            repl1["data"][metric_name][idx]["name"],
            fmt_countof(repl1["data"][metric_name][idx]["value"])
        ])
    
    print(tabulate(output_res, headers=output_header, tablefmt=table_fmt, colalign=("right")))

    print("\n>> memoryInBytesByLabelName <<")
    metric_name = "memoryInBytesByLabelName"
    output_res = []
    output_header = [
        "Top#N",
        "MetricName(prometheus-k8s-0)",
        "Size(prometheus-k8s-0)",
        "MetricName(prometheus-k8s-1)",
        "Size(prometheus-k8s-1)"
    ]

    for idx in range(0, len(repl0["data"][metric_name])):
        output_res.append([
            f"#{str(idx+1)}",
            repl0["data"][metric_name][idx]["name"],
            fmt_sizeof(repl0["data"][metric_name][idx]["value"]),
            repl1["data"][metric_name][idx]["name"],
            fmt_sizeof(repl1["data"][metric_name][idx]["value"])
        ])
    
    print(tabulate(output_res, headers=output_header, tablefmt=table_fmt, colalign=("right")))

    print("\n>> seriesCountByLabelValuePair <<")
    metric_name = "seriesCountByLabelValuePair"
    output_res = []
    output_header = [
        "Top#N",
        "MetricName(prometheus-k8s-0)",
        "Count(prometheus-k8s-0)",
        "MetricName(prometheus-k8s-1)",
        "Count(prometheus-k8s-1)"
    ]

    for idx in range(0, len(repl0["data"][metric_name])):
        output_res.append([
            f"#{str(idx+1)}",
            repl0["data"][metric_name][idx]["name"],
            fmt_countof(repl0["data"][metric_name][idx]["value"]),
            repl1["data"][metric_name][idx]["name"],
            fmt_countof(repl1["data"][metric_name][idx]["value"])
        ])
    
    print(tabulate(output_res, headers=output_header, tablefmt=table_fmt, colalign=("right")))


def prom_status_runtimeinfo(buffer=None):
    """
    Show Prometheus Status: Runtime Information (/api/v1/status/runtimeinfo).
    """
    files = [
        "prometheus/prometheus-k8s-0/status/runtimeinfo.json",
        "prometheus/prometheus-k8s-1/status/runtimeinfo.json"
    ]
    err = False
    table_fmt = "pretty"

    repl0, err = load_json_file(files[0])
    if err:
        print(f"Error loading file {files[0]}")
        return

    repl1, err = load_json_file(files[1])
    if err:
        print(f"Error loading file {files[1]}")
        return

    print(">> runtimeinfo <<")
    output_res = []
    output_header = [
        "metric",
        "prometheus-k8s-0",
        "prometheus-k8s-1"
    ]

    metrics = [
        "startTime",
        "CWD",
        "reloadConfigSuccess",
        "lastConfigTime",
        "corruptionCount",
        "goroutineCount",
        "GOMAXPROCS",
        "GODEBUG",
        "storageRetention"
    ]
    for metric in metrics:
        output_res.append([
            metric,
            repl0["data"][metric],
            repl1["data"][metric]
        ])

    print(tabulate(output_res, headers=output_header, tablefmt=table_fmt))


def prom_status_buildinfo(buffer=None):
    """
    Show Prometheus Status: Build Information (/api/v1/status/buildinfo).
    """
    files = [
        "prometheus/prometheus-k8s-0/status/buildinfo.json",
        "prometheus/prometheus-k8s-1/status/buildinfo.json"
    ]
    err = False
    table_fmt = "pretty"

    repl0, err = load_json_file(files[0])
    if err:
        print(f"Error loading file {files[0]}")
        return

    repl1, err = load_json_file(files[1])
    if err:
        print(f"Error loading file {files[1]}")
        return

    print(">> buildinfo <<")
    output_res = []
    output_header = [
        "metric",
        "prometheus-k8s-0",
        "prometheus-k8s-1"
    ]

    metrics = [
        "version",
        "revision",
        "branch",
        "buildUser",
        "buildDate",
        "goVersion"
    ]

    for metric in metrics:
        try:
            output_res.append([
                metric,
                repl0["data"][metric],
                repl1["data"][metric]
            ])
        except KeyError as e:
            print(e)

    print(tabulate(output_res, headers=output_header, tablefmt=table_fmt))


def prom_status_runtime_buildinfo(buffer=None):
    prom_status_buildinfo(buffer)
    prom_status_runtimeinfo(buffer)
    return


def prom_show_all(buffer=None):
    """
    Show all Prometheus commands.
    """
    from . import (
        parser_map, file_reader 
    )

    etcd_cmds = []
    for cmd in parser_map.keys():
        if not cmd.startswith('etcd'):
            continue
        if cmd.startswith('etcd-all'):
            continue
        etcd_cmds.append(parser_map[cmd])

    for cmd in etcd_cmds:
        buffer, err = file_reader(cmd['file_in'])
        parser_map[cmd['command']]["fn_out"](buffer)
    
    return
