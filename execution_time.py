import time


# Декоратор для подсчета времени выполнения
def get_execution_time(function):
    def wrapped(*args):
        start_time = time.time()
        res = function(*args)
        print(f"Время выполнения {function.__name__}: "
              f"{round(time.time() - start_time, 3)}s")
        return res

    return wrapped
