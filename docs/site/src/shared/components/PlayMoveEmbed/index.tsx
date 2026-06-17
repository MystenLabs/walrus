// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * PlayMoveEmbed — replaces a static Move code block with an embedded PlayMove
 * IDE (https://www.playmove.dev) so readers can edit, build, and test Move code
 * directly in the docs.
 *
 * The component renders a toolbar (title, copy button, and the shared
 * OpenInAgentButton dropdown) above an iframe pointed at PlayMove, with the
 * code passed through the URL fragment. A hidden <pre><code> mirror keeps the
 * code discoverable by tools that walk the rendered DOM (for example the
 * OpenInAgentButton, which reads the nearest code block).
 */
import React from "react";
import BrowserOnly from "@docusaurus/BrowserOnly";
import copy from "copy-text-to-clipboard";
import OpenInAgentButton from "@site/src/shared/components/OpenInAgentButton";
import "./styles.css";

const PLAYMOVE_ORIGIN = "https://www.playmove.dev";

type Props = {
  /** The Move source to load into the PlayMove editor. */
  code: string;
  /** Optional filename or title shown on the left of the toolbar. */
  title?: string;
  /** CSS height for the iframe. Defaults to "600px". */
  height?: string;
};

/** Read the active Docusaurus theme so PlayMove can match light or dark mode. */
function currentTheme(): "light" | "dark" {
  if (typeof document === "undefined") return "dark";
  const attr = document.documentElement.getAttribute("data-theme");
  return attr === "light" ? "light" : "dark";
}

function CopyButton({ code }: { code: string }) {
  const [copied, setCopied] = React.useState(false);
  const timeout = React.useRef<ReturnType<typeof setTimeout> | null>(null);

  const onCopy = React.useCallback(() => {
    copy(code);
    setCopied(true);
    if (timeout.current) clearTimeout(timeout.current);
    timeout.current = setTimeout(() => setCopied(false), 2000);
  }, [code]);

  React.useEffect(
    () => () => {
      if (timeout.current) clearTimeout(timeout.current);
    },
    [],
  );

  return (
    <button
      type="button"
      className="playmove-toolbar-btn"
      aria-label="Copy code to clipboard"
      title="Copy"
      onClick={onCopy}
    >
      {copied ? "Copied" : "Copy"}
    </button>
  );
}

function PlayMoveInner({ code, title, height = "600px" }: Props) {
  const theme = currentTheme();
  const src = `${PLAYMOVE_ORIGIN}/?theme=${encodeURIComponent(
    theme,
  )}#${encodeURIComponent(code)}`;

  return (
    <div className="playmove-embed">
      <div className="playmove-toolbar">
        <span className="playmove-toolbar-title">{title}</span>
        <div className="playmove-toolbar-actions">
          <CopyButton code={code} />
          <OpenInAgentButton className="playmove-toolbar-btn" />
        </div>
      </div>
      <iframe
        className="playmove-iframe"
        src={src}
        title={title ? `PlayMove: ${title}` : "PlayMove editor"}
        style={{ height, width: "100%" }}
        allow="clipboard-write"
        sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
      />
      {/* Hidden mirror so DOM-walking tooling (for example OpenInAgentButton)
          can find the underlying Move source. */}
      <pre style={{ display: "none" }} aria-hidden="true">
        <code className="language-move">{code}</code>
      </pre>
    </div>
  );
}

export default function PlayMoveEmbed(props: Props) {
  return (
    <BrowserOnly
      fallback={
        <div className="playmove-embed">
          <div className="playmove-toolbar">
            <span className="playmove-toolbar-title">{props.title}</span>
          </div>
          <pre>
            <code className="language-move">{props.code}</code>
          </pre>
        </div>
      }
    >
      {() => <PlayMoveInner {...props} />}
    </BrowserOnly>
  );
}
