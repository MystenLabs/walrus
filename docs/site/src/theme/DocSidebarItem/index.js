// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0
//
// Wraps the default DocSidebarItem to support inline code in sidebar labels.
// Any text wrapped in backticks (e.g. `withMemWal`) is rendered as <code>.

import React from "react";
import DocSidebarItemOriginal from "@theme-original/DocSidebarItem";

function parseInlineCode(text) {
  if (typeof text !== "string" || !text.includes("`")) return text;

  const parts = text.split(/(`[^`]+`)/g);
  if (parts.length === 1) return text;

  return parts.map((part, i) => {
    if (part.startsWith("`") && part.endsWith("`")) {
      return (
        <code
          key={i}
          style={{
            fontSize: "0.85em",
            padding: "0.1em 0.3em",
            borderRadius: "3px",
            background: "var(--ifm-code-background, rgba(0,0,0,0.06))",
            fontFamily: "var(--ifm-font-family-monospace)",
          }}
        >
          {part.slice(1, -1)}
        </code>
      );
    }
    return part;
  });
}

export default function DocSidebarItem(props) {
  if (props.item?.label && typeof props.item.label === "string" && props.item.label.includes("`")) {
    const modifiedProps = {
      ...props,
      item: {
        ...props.item,
        label: parseInlineCode(props.item.label),
      },
    };
    return <DocSidebarItemOriginal {...modifiedProps} />;
  }

  return <DocSidebarItemOriginal {...props} />;
}
