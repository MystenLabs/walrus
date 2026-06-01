> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## 0.0.2

### Internal

- Update `@mysten-incubation/memwal` dependency to `^0.0.2`

## 0.0.1

### Initial release

- NemoClaw/OpenClaw memory plugin powered by Walrus Memory
- Automatic memory recall through `before_prompt_build` hook
- Automatic fact capture through `agent_end` hook
- Session summary on `before_reset` hook
- CLI commands: `openclaw memwal stats`, `openclaw memwal search`
- LLM tools: `memory_search`, `memory_store`