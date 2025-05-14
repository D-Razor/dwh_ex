from datetime import datetime
from sqlalchemy.orm import sessionmaker
from custom_operator.database_initialization import project_engine


def sql_decorator_factory(*args, **kwargs):
    operation = kwargs.get("op_type", "unknown")
    # print("===decorator_factory begin")

    def sql_operation_decorator(func):
        # print("===sql_operation_decorator begin")

        def wrapper(*args, **kwargs):
            print(f"""SQL decor: args {args}, kwargs {kwargs}""")
            s = sessionmaker(bind=project_engine)()
            print(f"""Begin to run SQL {operation}""")
            result = func(*args, session=s, **kwargs)
            print(f"""End to run SQL {operation}""")

        return wrapper

    return sql_operation_decorator


def input_arguments_decorator(func):
    # print("===input_arguments_decorator begin")

    def wrapper(*args, **kwargs):
        print(f"""Input decor: args {args}, kwargs {kwargs}""")
        result = func(*args, **kwargs)

    return wrapper


def runtime_decorator(func):
    # print("===runtime_decorator begin")

    def wrapper(*args, **dwargs):
        print(f"""Runtime Input decor: args {args}, kwargs {dwargs}""")
        start = datetime.now()
        result = func(*args, **dwargs)
        time = datetime.now() - start
        print(f"""Func runtime: {time.microseconds / 1000}""")

    return wrapper


@runtime_decorator
@sql_decorator_factory(opd_type="ololo")
@input_arguments_decorator
def abv_func(*args, text, **kwargs):
    print(f"""I'm alive session {kwargs.get("session")}""")
    return text


if __name__ == "__main__":
    test_dict: dict = {"text": 14}
    test_tup: tuple = (0, 1, 3)
    abv_func(*test_tup, **test_dict)
    # abv_func(0, 1, 3, text=14, ggg=999)

# any unnamed input param goes to *args of a decorator
# any named input param goes to **kwargs of a decorator
# example of func input params: abc_func(*args, text: str, num: int, **kwargs)
# kwargs["text"] is a dict
# args[0] is a tuple/set type
