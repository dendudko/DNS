import time


# Декоратор для подсчета времени выполнения
def get_execution_time(function):
    def wrapped(*args, **kwargs):
        start_time = time.time()
        res = function(*args, **kwargs)
        print(f"Время выполнения {function.__name__}: "
              f"{round(time.time() - start_time, 3)}s")
        return res

    return wrapped
