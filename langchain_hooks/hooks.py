try:
    from langchain.callbacks.base import BaseCallbackHandler
except ModuleNotFoundError:
    from langchain_core.callbacks import BaseCallbackHandler

import time


class SQLToolCallback(BaseCallbackHandler):
    def __init__(self):
        self.start_time = None

    def on_tool_start(self, serialized, input_str=None, **kwargs):
        tool_input = input_str or kwargs.get("tool_input") or kwargs.get("input") or ""
        print("\n==============================")
        print("🔹 TOOL START")
        print("Tool Name:", serialized.get("name"))
        print("Input:", tool_input)
        self.start_time = time.time()

    def on_tool_end(self, output, **kwargs):
        duration = time.time() - self.start_time if self.start_time else None
        print("🔹 TOOL END")
        if duration is not None:
            print("Execution Time:", round(duration, 4), "seconds")
        print("Output:", output)
        print("==============================\n")

    def on_tool_error(self, error, **kwargs):
        print("\n🔴 TOOL ERROR")
        print(error)
        print("==============================\n")