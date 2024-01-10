"""
 Models used by utility methods
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import concurrent
import os
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Any, List, TypeVar, Generic, Optional, Dict

T = TypeVar("T")


@dataclass
class Executable(Generic[T]):
    """
    A single executable function with arguments, ready to run if invoked.
    """

    func: Callable[[...], T]
    alias: str
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)


class ConcurrentTaskRunner(Generic[T]):
    """
     Provides parallel execution capability for a list of given functions.

      my_funcs = [Executable(func=my_func1, args=[arg1, .., argN], alias='task1'),..]
      threads = len(os.sched_getaffinity(0))
      runner = ConcurrentTaskRunner(my_funcs, threads, True)

      # get results lazily
      tasks = runner.run()['taskN'].result()

      # get results as they come
      task_results = runner.run(lazy=False)

     NB: For CPU bound work, set use_processes to True, otherwise overall run time will not improve due to GIL.

    :param func_list: A list of (function, arguments) tuples to parallelise.
    :param num_threads: Maximum number of threads to use. On Linux platforms use len(os.sched_getaffinity(0))
          to get number of threads available to current process
    :param use_processes: Use processes instead of thread for parallelisation. Preferrable for work that depends on GIL release.
    """

    def __init__(
        self,
        func_list: List[Executable[T]],
        num_threads: Optional[int] = None,
        use_processes: bool = False,
    ):
        self._func_list = func_list
        self._num_threads = num_threads
        self._use_processes = use_processes

    def _run_tasks(self) -> Dict[str, concurrent.futures.Future]:
        """
         Executes a list of functions in parallel using threads or processes.

        :param lazy: Whether to collect results right away or leave this to the caller.
        :return: A dictionary of (callable_name, callable_future)
        """
        worker_count = self._num_threads or (
            len(os.sched_getaffinity(0)) if sys.platform not in ["win32", "darwin"] else os.cpu_count()
        )
        runner_pool = (
            ProcessPoolExecutor(max_workers=worker_count)
            if self._use_processes
            else ThreadPoolExecutor(max_workers=worker_count)
        )
        with runner_pool:
            return {
                executable.alias: runner_pool.submit(executable.func, *executable.args, **executable.kwargs)
                for executable in self._func_list
            }

    def lazy(self) -> Dict[str, concurrent.futures.Future]:
        """
         Executes the function list without explicitly collecting the results, allowing them to be retrieved by the client
         when needed.

        :return: A dictionary of (task_alias, task_future)
        """
        return self._run_tasks()

    def eager(self) -> Dict[str, T]:
        """
         Executes the function list and wait for all threads to complete execution before returning

        :return: A dictionary of (task_alias, task_result)
        """
        return {task_name: task.result() for task_name, task in self._run_tasks().items()}
