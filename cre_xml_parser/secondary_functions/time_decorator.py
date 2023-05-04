import time
from functools import wraps
from typing import Callable


def timeit(func: Callable, args_to_print=None):
    if args_to_print is None:
        args_to_print = []

    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        args_to_print_with_values = {k: v for k, v in kwargs.items() if k in args_to_print}
        print(f'Function {func.__name__} called with {args_to_print_with_values} took {total_time:.4f} seconds')
        return result
    return timeit_wrapper
