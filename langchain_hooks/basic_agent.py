from langchain.tools import tool
from logging import LoggingMiddleware
from middleware import MiddlewareTool
from factory import create_agent

@tool
def add_numbers(a: int, b: int) -> int:
    return a + b

middlewares = [LoggingMiddleware()]
tools = [MiddlewareTool(add_numbers, middlewares)]

agent = create_agent(tools)
agent.run("Add 2 and 3")
