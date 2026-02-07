from langchain.agents import initialize_agent, AgentType
from langchain.chat_models import ChatOpenAI

def create_agent(tools, callbacks=None):
    llm = ChatOpenAI(temperature=0, callbacks=callbacks or [])

    return initialize_agent(
        tools=tools,
        llm=llm,
        agent=AgentType.OPENAI_FUNCTIONS,
        verbose=True,
        callbacks=callbacks or [],
    )
