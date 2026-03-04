# Composing Operator Notifications

When the user asks to compose a notification message (for example, for Discord), operator contacts
can be found in the deployment questionnaire spreadsheet (shared via Google Drive).

First, check memory files for a known local path to the questionnaire file. If the file is not
found locally, ask the user to download it from the shared Google Drive and provide the local path.

Explore the sheet names and column headers to find the node names and Discord contact columns.

## Discord Message Formatting

- Use Discord markdown formatting (`**bold**`, `` `code` ``)
- Wrap URLs in `<>` to suppress link preview embeds
- Group issues by category (unreachable, DNS issues, event lag, recovery, etc.)
- Tag Discord users with `@username` format
- Present the message in a code block so it's easy for the user to copy-paste
