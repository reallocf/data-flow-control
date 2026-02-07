from langchain.callbacks.base import BaseCallbackHandler

class DebugCallbackHandler(BaseCallbackHandler):
    def on_tool_start(self, serialized, input_str, **kwargs):
        print("[CALLBACK] Tool:", serialized["name"])
