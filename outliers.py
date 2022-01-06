import pandas as pd
import statsmodels.api as sm
import numpy as np
from scipy.stats import f


def create_dataframe(data):
    """
    Converts performance tests results into pandas dataframe, for easier manipulation.
    :param data: list of performance tests, as returned by load_performance_tests
    :return: dataframe
    """
    return pd.DataFrame.from_records([
        {
            'name': bench['name'],
            'date': res['date'],
            'norm_date': norm_date,
            'min': res['values']['min'],
            'max': res['values']['max'],
            'p99': res['values']['p99'],
            'p99.9': res['values']['p99.9'],
            'avg': res['values']['avg'],
            'rps': res['values']['rps'],
            'total': res['values']['total'],
        }
        for bench in data for norm_date, res in enumerate(sorted(bench['latest_results'], key=lambda x: x['date']))
    ])


def is_outlier(value, threshold):
    """
    Compares the value to a given threshold.
    :param value: int, float or numpy array
    :param threshold: threshold after which a values is considered to be an outlier
    :return: boolean or numpy array
    """
    return np.abs(value) > threshold


class ZScore:
    def __init__(self):
        self._mean = 0  # mean
        self._std = 1   # standard deviation

    def fit(self, x):
        """
        Fit Z-score on the given dataset.
        :param x: numpy array
        """
        self._mean = np.mean(x)
        self._std = np.std(x)

    def transform(self, x):
        """
        Apply Z-score
        :param x: single value or numpy array
        :return: single value or numpy array
        """
        return (x - self._mean) / self._std

    def test(self, x, threshold=2):
        """
        Compares the value to a given threshold.
        :param x: int, float or numpy array
        :param threshold: threshold after which a values is considered to be an outlier
        :return: True if x is an outlier, false otherwise
        """
        return np.abs(x) > threshold



class ModifiedZScore:
    def __init__(self):
        self._k = 0.6745  # constant, assuming a normal distribution
        self._median = 0  # median
        self._mda = 1     # median of absolute deviations

    def fit(self, x):
        """
        Fit Modified Z-score on the given dataset.
        :param x: numpy array
        """
        self._median = np.median(x)
        self._mda = np.median(np.abs(x - self._median))

    def transform(self, x):
        """
        Apply Modified Z-score
        :param x: single value or numpy array
        :return: single value or numpy array
        """
        return self._k * (x - self._median) / self._mda

    def test(self, x, threshold=3.5):
        """
        Compares the value to a given threshold.
        :param x: int, float or numpy array
        :param threshold: threshold after which a values is considered to be an outlier
        :return: True if x is an outlier, false otherwise
        """
        return np.abs(x) > threshold


def chow_test(x, y, day, alpha=0.000005, k=2):
    """
    Performs Chow Test for a given day.
    :param x: normalized day numbers
    :param y: benchmark values
    :param day: day on which the test is to be performed
    :param alpha: parameter used for F test, increasing this will make the test more sensitive, risking false alarms
    :param k: degrees of freedom, should not be modified
    :return: True if a structural break was detected, false otherwise
    """
    dfn = k
    dfd = len(x) - 2 * k
    res = sm.OLS(y, x).fit()
    ssr_total = res.ssr
    res = sm.OLS(y[:day], x[:day]).fit()
    ssr_before = res.ssr
    res = sm.OLS(y[day + 1:], x[day + 1:]).fit()
    ssr_after = res.ssr
    numerator = (ssr_total - (ssr_before + ssr_after)) / dfn
    denominator = (ssr_before + ssr_after) / dfd
    chow = numerator / denominator
    return chow > f.ppf(q=1-alpha, dfn=dfn, dfd=dfd)


def check_last_run(df, benchmark, parameter):
    """
    Checks if there was anything unusual with the *last* performance test.
    :param df: dataframe created out of performance tests
    :param benchmark: name of the benchmark, e.g. 'insert-c10-r3-wc2-ws'
    :param parameter: name of the benchmark parameter, e.g. 'total'
    :return: True if the last day was an outlier, False otherwise
    """
    y = df[df['name'] == benchmark][parameter].values
    scorer = ModifiedZScore()
    scorer.fit(y[:-1])
    return scorer.test(y[-1])


def sequence_chow_test(df, benchmark, parameter, left=19, right=36):
    """
    Performs a Chow Test on each day in interval [left, right), which is roughly the middle of a 50-day sequence.
    :param df: dataframe created out of performance tests
    :param benchmark: name of the benchmark, e.g. 'insert-c10-r3-wc2-ws'
    :param parameter: name of the benchmark parameter, e.g. 'total'
    :return: list of timestamps which could be considered to be the start of a structural break
    """
    x = df[df['name'] == benchmark]['norm_date'].values
    y = df[df['name'] == benchmark][parameter].values
    result = []
    for pivot in range(left, right):
        if chow_test(x, y, pivot):
            result.append(df[(df['name'] == benchmark) & (df['norm_date'] == pivot)]['date'].values[0])
    return result
