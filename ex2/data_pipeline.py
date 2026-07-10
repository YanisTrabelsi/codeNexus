from abc import ABC, abstractmethod
from typing import Any, Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.name = "Unknow"
        self.storage: list[Any] = []
        self.output_count: int = 0
        self.ingest_count: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        if (type(data) is list):
            self.ingest_count += len(data)
        else:
            self.ingest_count += 1

    def output(self) -> tuple[int, str]:
        nb: int = self.output_count
        self.output_count += 1
        data = self.storage[0]
        self.storage.pop(0)
        return (nb, data)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()
        self.name = "Numeric"

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
        super().ingest(data)
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
        self.name = "Text"

    def validate(self, data: Any) -> bool:
        is_str: bool = type(data) is str
        is_list: bool = type(data) is list
        if (is_list):
            is_list = all(isinstance(i, str) for i in data)
        if (is_str or is_list):
            return (True)
        return (False)

    def ingest(self, data: str | list[str]) -> None:
        super().ingest(data)
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
        self.name = "Log"

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
        super().ingest(data)
        if (not self.validate(data)):
            raise ValueError("Improper log data")
        str1: str = ""
        str2: str = ""
        concat: str = ""
        if (isinstance(data, list)):
            for i in data:
                str1 = i[list(i.keys())[0]]
                str2 = i[list(i.keys())[1]]
                concat = ": ".join([str1, str2])
                self.storage.append(concat)
            return
        str1 = data[list(data.keys())[0]]
        str2 = data[list(data.keys())[1]]
        concat = ": ".join([str1, str2])
        self.storage.append(concat)


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CsvPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        output_list: list[str] = []
        for e in data:
            output_list.append(e[1])
        print("CSV Output:")
        print(",".join(output_list))


class JsonPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        output: dict[str, str] = {f"item_{n}": f"{v}" for n, v in data}
        print("JSON Output:")
        print(str(output).translate({39: 34}))


class DataStream():
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        processors_len = len(self.processors)
        for data in stream:
            for i, proc in enumerate(self.processors):
                if (proc.validate(data)):
                    proc.ingest(data)
                    break
                elif (i + 1 == processors_len):
                    print("DataStream error - "
                          f"Can't process element in stream: {data}")

    def print_processors_stats(self) -> None:
        if (len(self.processors) == 0):
            print("No processor found, no data")
            return
        for proc in self.processors:
            processed: int = proc.ingest_count
            remaining: int = proc.ingest_count - proc.output_count
            print(f"{proc.name} Processor: total {processed} items processed, "
                  f"remaining {remaining} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            output: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    output.append(proc.output())
                except Exception:
                    proc.output_count -= 1
                    break
            plugin.process_output(output)


if (__name__ == "__main__"):
    print("=== Code Nexus - Data Pipeline ===\n")
    num: DataProcessor = NumericProcessor()
    text: DataProcessor = TextProcessor()
    log: DataProcessor = LogProcessor()
    processors: list[DataProcessor] = [num, text, log]
    csv: ExportPlugin = CsvPlugin()
    json: ExportPlugin = JsonPlugin()
    stream1: Any = ["Hello world", [3.14, -1, 2.71],
                    [{"log_level": "WARNING", "log_message":
                     "Telnet access! Use ssh instead"},
                    {"log_level": "INFO", "log_message":
                     "User wil is connected"}], 42, ["Hi", "five"]]
    stream2: Any = [21, ["I love AI", "LLMs are wonderful",
                         "Stay healthy"],
                    [{"log_level": "ERROR", "log_message":
                      "500 server crash"},
                     {"log_level": "NOTICE", "log_message":
                      "Certificate expires in 10 days"}],
                    [32, 42, 64, 84, 128, 168], "World hello"]
    print("\nInitialize Data Stream...\n")
    Ds = DataStream()
    print("== DataStream statistics ==")
    Ds.print_processors_stats()
    for proc in processors:
        Ds.register_processor(proc)
    Ds.process_stream(stream1)
    print("\nRegistering Processors\n")
    print(f"Send first batch of data on stream: {stream1}\n")
    print("== DataStream statistics ==")
    Ds.print_processors_stats()
    print("\nSend 3 processed data from each processor to a CSV plugin:")
    Ds.output_pipeline(3, csv)
    print("\n== DataStream statistics ==")
    Ds.print_processors_stats()
    print(f"Send another batch of data: {stream2}\n")
    Ds.process_stream(stream2)
    print("== DataStream statistics ==")
    Ds.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    Ds.output_pipeline(5, json)
    print("== DataStream statistics ==")
    Ds.print_processors_stats()
