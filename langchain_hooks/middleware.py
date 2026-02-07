from langchain.tools import BaseTool
from .middlewares.base import ToolMiddleware

class MiddlewareTool(BaseTool):
    def __init__(self, tool: BaseTool, middlewares: list[ToolMiddleware]):
        self._tool = tool
        self.middlewares = middlewares

        super().__init__(
            name=tool.name,
            description=tool.description,
            args_schema=tool.args_schema,
        )

    def _run(self, *args, **kwargs):
        tool_input = kwargs

        for mw in self.middlewares:
            tool_input = mw.pre(self.name, tool_input)

        output = self._tool.run(**tool_input)

        for mw in reversed(self.middlewares):
            output = mw.post(self.name, tool_input, output)

        return output
