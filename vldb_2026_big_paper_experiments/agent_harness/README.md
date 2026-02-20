# Agent Harness

This harness provides a LangChain agent with exactly one tool:

- `execute_sql(sql: str)` -> runs SQL via `SQLRewriter` in **1Phase** mode.

## Location

- `/Users/charliesummers/code/data-flow-control/vldb_2026_big_paper_experiments/agent_harness/`

## Providers

Supported through LangChain integrations:

- OpenAI (`langchain-openai`)
- AWS Bedrock (`langchain-aws`)

## Environment Variables

Required:

- `AGENT_PROVIDER` = `openai` or `bedrock`

OpenAI mode:

- `OPENAI_API_KEY`
- `OPENAI_MODEL` (optional, default `gpt-4.1-mini`)

Bedrock mode:

- `AWS_REGION`
- `BEDROCK_MODEL_ID` (optional, default `anthropic.claude-3-5-sonnet-20241022-v2:0`)
- `AWS_PROFILE` (optional)

Database/tool options:

- `AGENT_DB_PATH` (optional, default `:memory:`)
- `AGENT_MAX_RESULT_ROWS` (optional, default `100`)
- `AGENT_VERBOSE` (optional, default `true`)
- `AGENT_SYSTEM_PROMPT` (optional, default is the current SQL-agent system prompt)
- `AGENT_DFC_POLICY` (optional, one policy string registered at startup)
- `AGENT_DFC_POLICY_FILE` (optional, file with one policy string per non-empty line)

Model options:

- Temperature is set to `1.0` for both OpenAI and Bedrock chat models.

## Run

Single turn:

```bash
cd /Users/charliesummers/code/data-flow-control/vldb_2026_big_paper_experiments
.venv/bin/python -m agent_harness.main --question "show all tables"
```

Single turn with startup policy registration:

```bash
cd /Users/charliesummers/code/data-flow-control/vldb_2026_big_paper_experiments
.venv/bin/python -m agent_harness.main \
  --policy "SOURCE test_data CHECK max(test_data.value) > 0 ON FAIL REMOVE" \
  --question "select * from test_data"
```

With policy file:

```bash
cd /Users/charliesummers/code/data-flow-control/vldb_2026_big_paper_experiments
.venv/bin/python -m agent_harness.main --policy-file /path/to/policies.txt
```

Interactive:

```bash
cd /Users/charliesummers/code/data-flow-control/vldb_2026_big_paper_experiments
.venv/bin/python -m agent_harness.main
```

## Notes

- The harness exposes only `execute_sql`.
- SQL is always transformed with:
  - `SQLRewriter.transform_query(sql, use_two_phase=False)`
- Library use supports direct pre-built policy registration:
  - `SQLExecutionHarness.register_policy(policy: DFCPolicy)`
  - `SQLExecutionHarness.register_aggregate_policy(policy: AggregateDFCPolicy)`
- Library helper functions in `agent_harness.agent`:
  - `build_agent(config)`
  - `run_single_turn(agent, user_input, chat_history)`
  - `run_agent_loop(agent, user_input, chat_history, max_iterations=25)`
