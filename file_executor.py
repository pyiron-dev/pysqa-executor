import os
import re
import queue
from concurrent.futures import Future, Executor
from threading import Thread

import hashlib
import cloudpickle
from pympipool import cancel_items_in_queue


def get_hash(binary):
    # Remove specification of jupyter kernel from hash to be deterministic
    binary_no_ipykernel = re.sub(b"(?<=/ipykernel_)(.*)(?=/)", b"", binary)
    return str(hashlib.md5(binary_no_ipykernel).hexdigest())


def serialize_funct(fn, *args, **kwargs):
    binary = cloudpickle.dumps({"fn": fn, "args": args, "kwargs": kwargs})
    return {fn.__name__ + get_hash(binary=binary): binary}


def get_file_name(name, state):
    return name + "." + state + ".pl"


def write_to_file(funct_dict, state):
    file_name_lst = []
    for k, v in funct_dict.items():
        file_name = get_file_name(name=k, state=state)
        file_name_lst.append(file_name)
        with open(file_name, "wb") as f:
            f.write(v)
    return file_name_lst


def read_from_file(file_name):
    name = file_name.split(".")[0]
    with open(file_name, "rb") as f:
        return {name: f.read()}
    
    
def deserialize(funct_dict):
    return {k: cloudpickle.loads(v) for k, v in funct_dict.items()}


def apply_funct(apply_dict):
    return {k: v["fn"].__call__(*v["args"], **v["kwargs"]) for k, v in apply_dict.items()} 


def serialize_result(result_dict):
    return {k: cloudpickle.dumps(v) for k, v in result_dict.items()}


def execute_function_file(fn, *args, **kwargs):
    funct_dict = serialize_funct(fn, *args, **kwargs)
    key = list(funct_dict.keys())[0]
    if key not in global_future_dict.keys():
        global_future_dict[key] = Future()
        file_name = write_to_file(
            funct_dict=funct_dict, 
            state="in"
        )[0]
        global_queue.put({key: global_future_dict[key]})
    return global_future_dict[key]


def reload_previous_futures(future_dict):
    file_lst = os.listdir()
    for f in file_lst:
        if f.endswith(".in.pl"):
            key = f.split(".in.pl")[0]
            future_dict[key] = Future()
            file_name_out = key + ".out.pl"
            if file_name_out in file_lst:
                future_dict[key].set_result(list(deserialize(
                    funct_dict=read_from_file(file_name=file_name_out)
                ).values())[0])
            else: 
                global_queue.put({key: future_dict[key]})


def execute_tasks(future_queue):
    while True:
        task_dict = None
        try:
            task_dict = future_queue.get_nowait()
        except queue.Empty:
            pass
        if task_dict is not None:
            key = list(task_dict.keys())[0]
            future = task_dict[key]
            if not future.done() and future.set_running_or_notify_cancel():
                file_lst = os.listdir()
                file_name_out = get_file_name(name=key, state="out")
                if file_name_out not in file_lst:
                    file_name_in = get_file_name(name=key, state="in")
                    funct_dict = read_from_file(file_name=file_name_in)
                    apply_dict = deserialize(funct_dict=funct_dict)
                    result_dict = apply_funct(apply_dict=apply_dict)
                    write_to_file(funct_dict=serialize_result(result_dict=result_dict), state="out")
                future.set_result(list(deserialize(funct_dict=read_from_file(file_name=file_name_out)).values())[0])
                                  
                    
class FileExecutor(Executor):
    def __init__(self):
        self._task_queue = queue.Queue()
        self._memory_dict = {}
        reload_previous_futures(future_dict=self._memory_dict)
        self._process = Thread(target=execute_tasks, args=(self._task_queue,))
        self._process.start()
        
    def submit(self, fn, *args, **kwargs):
        funct_dict = serialize_funct(fn, *args, **kwargs)
        key = list(funct_dict.keys())[0]
        if key not in self._memory_dict.keys():
            self._memory_dict[key] = Future()
            file_name = write_to_file(
                funct_dict=funct_dict, 
                state="in"
            )[0]
            self._task_queue.put({key: self._memory_dict[key]})
        return self._memory_dict[key]
    
    def shutdown(self, wait=True, *, cancel_futures=False):
        if cancel_futures:
            cancel_items_in_queue(que=self._task_queue)
        self._future_queue.put({"shutdown": True, "wait": wait})
        self._process.join()
