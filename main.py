#!/bin/env python3
from datetime import datetime

import pyArango.connection
import argparse
import urllib3
import matplotlib.pyplot as plt

urllib3.disable_warnings()
plt.style.use('dark_background')


def parse_arguments():
    def user_pass(s: str):
        pair = s.split(':', 1)
        assert 0 < len(pair) <= 2
        if len(pair) == 1:
            password = input(f"Password for user `{pair[0]}`: ")
            return pair[0], password
        return tuple(pair)

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
    parser.add_argument("--server.endpoint", default="http://localhost:8529/")
    parser.add_argument("--server.user", type=user_pass, default="root")
    parser.add_argument("--server.no-verify-tls", action='store_true', default=False)
    return parser.parse_args()


def load_performance_tests(url, user, verify, verbose):
    conn = pyArango.connection.Connection(url, user[0], user[1], verify=verify, verbose=verbose)
    db = conn["perf"]

    query = """
for doc in rlog
    filter doc.name != NULL
    collect name = doc.name

    let latest_results = (
        for doc in rlog
            filter doc.name == name
            sort doc.date
            limit 50
            return {date: doc.date, values: doc.result})
            
    return {name, latest_results}"""

    result = db.AQLQuery(query, rawResults=True)
    return list(result)


def plot(name, results, file=None):
    if file is None:
        file = f"{name}.png"

    print(name, results)
    dates = [datetime.utcfromtimestamp(int(r['date'])).strftime('%Y-%m-%d') for r in results]
    rps = [r['values']['rps'] for r in results]

    fig, ax = plt.subplots()
    plt.title(name)
    twin1 = ax.twinx()

    a = list(range(0, len(results)))

    plots = []
    for d in ['min', 'max', 'p99', 'p99.9', 'avg']:
        p, = ax.plot(a, [1000 * r['values'][d] for r in results], "-", label=d)
        plots.append(p)

    p2, = twin1.plot(a, rps, "--", label="rps")
    plots.append(p2)

    ax.set_xlabel("Date")
    ax.xaxis.set_ticks(a)  # set the ticks to be a
    ax.xaxis.set_ticklabels(dates)  # change the ticks' names to x

    ax.set_ylabel("Time (ms)")
    twin1.set_ylabel("Requests/sec")

    twin1.yaxis.label.set_color(p2.get_color())
    twin1.tick_params(axis='y', colors=p2.get_color())
    ax.legend(handles=plots)
    plt.savefig(f"{name}.png")


def main():
    args = parse_arguments()
    results = load_performance_tests(getattr(args, 'server.endpoint'), getattr(args, 'server.user'),
                                     not getattr(args, 'server.no_verify_tls'), args.verbose)
    for result in results:
        plot(result['name'], result['latest_results'])


if __name__ == "__main__":
    main()
