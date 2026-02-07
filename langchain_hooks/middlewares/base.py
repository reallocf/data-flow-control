from typing import Any, Dict

class ToolMiddleware:
    def pre(self, tool_name: str, tool_input: Dict[str, Any]) -> Dict[str, Any]:
        return tool_input

    def post(self, tool_name: str, tool_input: Dict[str, Any], tool_output: Any) -> Any:
        return tool_output
