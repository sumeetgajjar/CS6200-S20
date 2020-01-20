import logging
import time


def timing(func):
    def timing_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        run_time = time.perf_counter() - start_time
        logging.info("{} took {}".format(func.__name__, convert_seconds_to_human_time(run_time)))
        return value

    return timing_wrapper


def convert_seconds_to_human_time(seconds):
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0 and minutes > 0:
        return "{} hours {} minutes {:.4f} seconds".format(hours, minutes, seconds)
    elif minutes > 0:
        return "{} minutes {:.4f} seconds".format(minutes, seconds)
    else:
        return "{:.4f} seconds".format(seconds)
