import sys
import os
sys.path.append(os.path.realpath('..'))

from yacs.utils import TaskPool

tasks = [
    {'id': 1, 'k':['test']},
    {'id': 2, 'k':['test2']},
    {'id': 3, 'k':['one', 'two', 'three']},
    {'id': 4, 'k':['horn', 'ok', 'pls']}
]

# TODO: Update these tests, these are failing atm

def test_taskpool_take():
    taskpool = TaskPool(tasks)
    taken = taskpool.take(2).tasks
    taken_result = [{'id': 1, 'k':['test']}, {'id': 2, 'k':['test2']}]
    assert taken == taken_result

def test_taskpool_take_filter():
    taskpool = TaskPool(tasks)
    got = taskpool.take(2).filter(lambda x: x['id'] > 1).tasks
    expected = [{'id': 2, 'k':['test2']}]
    assert got == expected

def test_taskpool_filter_take():
    taskpool = TaskPool(tasks)
    got = taskpool.filter(lambda x: x['id'] > 1).take(2).tasks
    expected = [{'id': 2, 'k':['test2']}, {'id': 3, 'k':['one', 'two', 'three']}]
    assert got == expected
