> For the complete documentation index, see [llms.txt](https://docs.wal.app/llms.txt)

## Overview

As part of ongoing security improvements, we're giving you the option to review and remove
memories you no longer want stored on Walrus Memory. This guide shows you how to preview
your memories and delete any you'd like to clear.

> **Warning**
>
> Deleting a memory is permanent and you cannot undo it. Preview your memories before you
>   delete them.
## Before you begin

Connect the wallet that owns the memories you want to review. After you connect, the
dashboard opens at `/dashboard`.

## Navigate to the delete memories panel

On the dashboard, scroll down to the **Delete memories** panel. The panel shows how many
memories your wallet has stored and their current status:

- **Stored**: memories that you can delete.
- **In progress**: memories that you have selected for deletion and that Walrus Memory is
  in the process of deleting.
- **Deleted**: memories that Walrus Memory has removed.

Each memory appears as a row with these columns:

| Column | Description |
| --- | --- |
| Select | Checkbox that you use to choose the memory for deletion. |
| Blob | The blob ID of the stored memory. |
| Object | The onchain object ID. |
| Created | The date you stored the memory. |
| State | The memory state, for example `deletable` (recommended). |
| Preview | Opens a preview of the memory content. |

## Preview a memory before deleting

To check what a memory contains before you delete it, select the **Preview** control on its
row. The **Memory preview** window opens and shows the stored content. Close the preview
when you finish.

> **Note**
>
> Previewing helps you confirm that you are deleting the right memories, because you cannot
>   reverse a deletion.
## Select memories to delete

Use the checkboxes in the **Select** column to choose the memories you want to delete.

- To select every memory on the current page, choose **Select page**.
- The **Delete selected** button updates to show how many memories you chose, for example
  **Delete selected (4)**.

## Delete memories

You can delete your selected memories, or delete all memories at once.

- **Delete selected (n)**: permanently deletes the memories you checked.
- **Delete all (n)**: permanently deletes every memory on the list.

> **Warning**
>
> Both actions are permanent. You cannot recover deleted memories.
After you start a deletion, the panel shows progress as it works through the memories, for
example `Deleting batch 1...`. When it finishes, you see a confirmation like **Deleted 4
memories**. The deleted memories now show the `deleted` state.

## Verify the deletion

Check the count line at the top of the panel to confirm the deletion. The memories you
removed now show the `deleted` state, and the **Deleted** count includes them.