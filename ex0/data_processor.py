from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.storage: list[Any] = []
        self.output_count: int = -1
        ...

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        self.output_count += 1
        data = self.storage[0]
        self.storage.pop(0)
        return (self.output_count, data)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        is_int: bool = type(data) is int and type(data) is not bool
        is_float: bool = type(data) is float and type(data) is not bool
        is_list: bool = type(data) is list
        if (is_list):
            is_list = all(isinstance(i, (int, float)) and not
                          isinstance(i, (bool)) for i in data)
        if (is_int or is_float or is_list):
            return (True)
        return (False)

    def ingest(self, data: int | float | list[int | float]) -> None:
        if (not self.validate(data)):
            raise ValueError("Improper numeric data")
        if (type(data) is list):
            for i in data:
                self.storage.append(str(i))
            return
        self.storage.append(str(data))


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        is_str: bool = type(data) is str
        is_list: bool = type(data) is list
        if (is_list):
            is_list = all(isinstance(i, str) for i in data)
        if (is_str or is_list):
            return (True)
        return (False)

    def ingest(self, data: str | list[str]) -> None:
        if (not self.validate(data)):
            raise ValueError("Improper text data")
        if (type(data) is list):
            for i in data:
                self.storage.append(i)
            return
        self.storage.append(data)


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def validate(self, data: Any) -> bool:
        is_dict: bool = type(data) is dict
        if (is_dict):
            is_dict = all(isinstance(k, str) and isinstance(v, str)
                          for k, v in data.items())
        is_list: bool = type(data) is list
        if (is_list):
            is_list = all(all(isinstance(k, str) and isinstance(v, str)
                          for k, v in i.items()) for i in data)
        if (is_dict or is_list):
            return (True)
        return (False)

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if (not self.validate(data)):
            raise ValueError("Improper log data")
        if (type(data) is list):
            for i in data:
                self.storage.append(i)
            return
        self.storage.append(data)


if (__name__ == "__main__"):
    num: NumericProcessor = NumericProcessor()
    text: TextProcessor = TextProcessor()
    log: LogProcessor = LogProcessor()
    v_data: dict[str, Any] = {
        'num0': 1,
        'num1': 2,
        'num2': 4.2,
        'num3': [42, 2.4, -3],
        'text0': "hello",
        'text1': "Louka",
        'text2': ["ca", "va"],
        'log0': {'log level': "NOTICE",
                 'log message': "Connection to server"},
        'log1': [{'log level': "NOTICE",
                  'log message': "Connection to server"},
                 {'log level': "ERROR",
                  'log message': "Unauthorized access!!"}]
    }
    inv_data: dict[str, Any] = {
        'num0': "hello",
        'num1': [42, False],
        'text0': 42,
        'text1': ["ca", True],
        'log0': {'log level': 42,
                 'log message': "Connection to server"},
        'log1': [{'log level': "NOTICE",
                  'log message': True},
                 {'log level': "ERROR",
                  'log message': "Unauthorized access!!"}]
    }

    print("=== Code Nexus - Data Processor ===\n")

    # NUMERIC
    print("TESTING NUMERIC PROCESSOR")
    for i in range(4):
        print(f"Trying to validate input '{v_data[f'num{i}']}': ", end="")
        print(num.validate(v_data[f"num{i}"]))
    for i in range(2):
        print(f"Trying to validate input '{inv_data[f'num{i}']}': ", end="")
        print(num.validate(inv_data[f"num{i}"]))
    print('-' * 15)

    # TEXT
    print("TESTING TEXT PROCESSOR")
    for i in range(3):
        print(f"Trying to validate input '{v_data[f'text{i}']}': ", end="")
        print(text.validate(v_data[f"text{i}"]))
    for i in range(2):
        print(f"Trying to validate input '{inv_data[f'text{i}']}': ", end="")
        print(text.validate(inv_data[f"text{i}"]))
    print('-' * 15)

    # LOG
    print("TESTING LOG PROCESSOR")
    for i in range(2):
        print(f"Trying to validate input '{v_data[f'log{i}']}': ", end="")
        print(log.validate(v_data[f"log{i}"]))
    for i in range(2):
        print(f"Trying to validate input '{inv_data[f'log{i}']}': ", end="")
        print(log.validate(inv_data[f"log{i}"]))

    print("\n\nTESTING INGEST WITH INVALID DATA")
    try:
        num.ingest(inv_data["num0"])
    except ValueError as e:
        print(f"ValueError with input '{inv_data['num0']}': {e}")
    try:
        text.ingest(inv_data["text0"])
    except ValueError as e:
        print(f"ValueError with input '{inv_data['text0']}': {e}")
    try:
        text.ingest(inv_data["log0"])
    except ValueError as e:
        print(f"ValueError with input '{inv_data['log0']}': {e}")

    print("\n\nTESTING OUTPUT INGEST DATA")
    print(f"Processing data: {v_data['num3']}")
    num.ingest(v_data['num3'])
    print("Extracting 2 value...")
    output: tuple[int, str] = (0, "")
    for i in range(2):
        output = num.output()
        print(f"Numeric value {output[0]}: {output[1]}")
    print(f"\nProcessing data: {v_data['text0']}")
    text.ingest(v_data['text0'])
    print("Extracting 1 value...")
    output = text.output()
    print(f"Text value {output[0]}: {output[1]}")
    print(f"\nProcessing data: {v_data['log1']}")
    log.ingest(v_data['log1'])
    print("Extracting 1 value...")
    output = log.output()
    print(f"Log value {output[0]}: {output[1]}")
