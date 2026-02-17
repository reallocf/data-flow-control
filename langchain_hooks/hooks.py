from langchain_core.callbacks import BaseCallbackHandler
import time


class SQLToolCallback(BaseCallbackHandler):

    def on_tool_start(self, serialized, input_str, **kwargs):
        print("\n==============================")
        print("🔹 TOOL START")
        print("Tool Name:", serialized.get("name"))
        print("Input SQL:", input_str)

        sql_upper = input_str.upper()

        # 🚫 Block dangerous operations
        forbidden = ["DROP", "DELETE", "UPDATE", "INSERT"]
        if any(word in sql_upper for word in forbidden):
            raise ValueError("❌ Write operations are not allowed.")

        self.start_time = time.time()

    def on_tool_end(self, output, **kwargs):
        duration = time.time() - self.start_time

        print("🔹 TOOL END")
        print("Execution Time:", round(duration, 4), "seconds")
        print("Output:", output)
        print("==============================\n")

    def on_tool_error(self, error, **kwargs):
        print("\n🔴 TOOL ERROR")
        print(error)
        print("==============================\n")
