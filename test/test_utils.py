import sys
import os
sys.path.append(os.path.realpath('..'))

from yacs.utils import TaskPool, Status

tasks = [
    {'id': 1, 'depends_on':['test']},
    {'id': 2, 'depends_on':['test2']},
    {'id': 3, 'depends_on':['one', 'two', 'three']},
    {'id': 4, 'depends_on':['horn', 'ok', 'pls']}
]

# TODO: Update these tests, these are failing atm

def test_taskpool_take():
    taskpool = TaskPool(tasks)
    taken = taskpool.take(2).tasks
    taken_result = [{'id': 1, 'depends_on':['test'], 'status': Status.SCHEDULED}, {'id': 2, 'depends_on':['test2'], 'status': Status.SCHEDULED}]
    assert taken == taken_result

def test_taskpool_take_filter():
    taskpool = TaskPool(tasks)
    got = taskpool.take(2).filter(lambda x: x['id'] > 1).tasks
    expected = [{'id': 2, 'depends_on':['test2'], 'status': Status.SCHEDULED}]
    assert got == expected

def test_taskpool_filter_take():
    taskpool = TaskPool(tasks)
    got = taskpool.filter(lambda x: x['id'] > 1).take(2).tasks
    expected = [{'id': 2, 'depends_on':['test2'], 'status': Status.SCHEDULED}, {'id': 3, 'depends_on':['one', 'two', 'three'], 'status': Status.SCHEDULED}]
    assert got == expected

def test_update():
    taskpool = TaskPool(tasks)
    def update_func(t, id_to_remove):
        try:
            t['depends_on'].remove(id_to_remove)
        except ValueError:
            pass
    taskpool.update(
        lambda t: t['id'] == 3,
        lambda t: update_func(t, 'two')
    )
    expected = [
        {'id': 1, 'status': Status.SCHEDULED, 'depends_on':['test']},
        {'id': 2, 'status': Status.SCHEDULED, 'depends_on':['test2']},
        {'id': 3, 'status': Status.SCHEDULED, 'depends_on':['one', 'three']},
        {'id': 4, 'depends_on':['horn', 'ok', 'pls']}
    ]
    assert taskpool.tasks == expected

def test_remove():
    taskpool = TaskPool(tasks)
    taskpool.remove(lambda t: t['id'] == 4)
    expected = [
        {'id': 1, 'status': Status.SCHEDULED, 'depends_on':['test']},
        {'id': 2, 'status': Status.SCHEDULED, 'depends_on':['test2']},
        {'id': 3, 'status': Status.SCHEDULED, 'depends_on':['one', 'three']},
    ]
    assert taskpool.tasks == expected
