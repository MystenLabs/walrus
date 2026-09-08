> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

Use the runnable [Walrus Memory Python SDK Colab](https://colab.research.google.com/drive/1SaKjkSp0DXnM_nktWSiEC-l9qGtVr6ph) when you want a notebook-first walkthrough.

The notebook covers:

- installing `memwal`
- loading credentials through Colab Secrets or hidden prompts
- configuring the SDK without exposing private keys, defaulting to `staging`
- switching to `prod` when you have production credentials
- creating `MemWalSync` for notebook-friendly calls
- checking relayer `health`, compatibility, and delegate public key derivation
- storing memory with `remember`
- waiting for async remember jobs to persist on Walrus
- retrieving memory with `recall`
- using `remember_async`, `remember_and_wait`, bulk remember, `remember_bulk_async`, `remember_bulk_and_wait`, `ask`, `analyze`, `analyze_and_wait`, `embed`, manual search/register with scoring weights, and `restore`
- optionally wrapping OpenAI and LangChain clients with Walrus Memory middleware
- using `OPENAI_BASE_URL` for OpenAI-compatible providers such as OpenRouter
- basic troubleshooting for auth, namespaces, and async remember jobs

The repo copy lives in [`packages/python-sdk-memwal/notebooks/walrus_memory_python_sdk.ipynb`](https://github.com/MystenLabs/MemWal/blob/main/packages/python-sdk-memwal/notebooks/walrus_memory_python_sdk.ipynb).