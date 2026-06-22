// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

/**
 * CodeWalkthrough — a split-view, scroll-spy explainer for a single source
 * file. Explanation steps scroll on the left while the full source stays
 * sticky on the right. As each step enters the viewport, the lines it
 * references are highlighted and the rest are dimmed.
 *
 * Usage (MDX):
 *
 *   <CodeWalkthrough source="path/to/file.move" org="MystenLabs" repo="walrus" language="move">
 *     <Step lines="1-5" title="Module declaration">Explanation…</Step>
 *     <Step lines="7-12" title="Struct definition">Explanation…</Step>
 *   </CodeWalkthrough>
 *
 * The `lines` prop uses 1-based line numbers from the cleaned code (after the
 * license header is stripped). It supports ranges ("5-9") and comma-separated
 * values ("1-3,5,8-10").
 */
import React from "react";
import { Highlight } from "prism-react-renderer";
import { usePrismTheme } from "@docusaurus/theme-common";
import { importContentMap } from "../../../.generated/ImportContentMap";
import "./styles.css";

/**
 * Declarative step. Renders nothing on its own — CodeWalkthrough reads its
 * props through React.Children.
 */
export function Step(_props: {
  lines: string;
  title?: string;
  children: React.ReactNode;
}): null {
  return null;
}

type StepData = {
  lines: string;
  title?: string;
  content: React.ReactNode;
};

type Props = {
  /** Repo-relative path (local import map) or path within the GitHub repo. */
  source: string;
  /** GitHub org. When org and repo are set, code is fetched from GitHub raw. */
  org?: string;
  /** GitHub repo. */
  repo?: string;
  /** GitHub branch. Defaults to "main". */
  branch?: string;
  /** Prism language. Defaults to "move". */
  language?: string;
  children: React.ReactNode;
};

/** Parse a `lines` spec like "1-3,5,8-10" into a set of 1-based line numbers. */
function parseLines(spec: string): Set<number> {
  const out = new Set<number>();
  if (!spec) return out;
  for (const part of spec.split(",")) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    const range = trimmed.match(/^(\d+)\s*-\s*(\d+)$/);
    if (range) {
      const start = Number(range[1]);
      const end = Number(range[2]);
      for (let i = Math.min(start, end); i <= Math.max(start, end); i++) {
        out.add(i);
      }
    } else if (/^\d+$/.test(trimmed)) {
      out.add(Number(trimmed));
    }
  }
  return out;
}

/** Strip the standard copyright header and leading blank lines. */
function cleanCode(raw: string): string {
  return raw
    .replace(
      /^\/\/\s*Copyright.*\n\/\/\s*SPDX-License-Identifier:.*\n?/im,
      "",
    )
    .replace(/^\s*\n+/, "");
}

function collectSteps(children: React.ReactNode): StepData[] {
  const steps: StepData[] = [];
  React.Children.forEach(children, (child) => {
    if (!React.isValidElement(child)) return;
    const props = child.props as {
      lines?: string;
      title?: string;
      children?: React.ReactNode;
    };
    if (typeof props.lines !== "string") return;
    steps.push({
      lines: props.lines,
      title: props.title,
      content: props.children,
    });
  });
  return steps;
}

export default function CodeWalkthrough({
  source,
  org,
  repo,
  branch = "main",
  language = "move",
  children,
}: Props) {
  const steps = React.useMemo(() => collectSteps(children), [children]);
  const isGitHub = Boolean(org && repo);

  const [code, setCode] = React.useState<string | null>(
    isGitHub ? null : cleanCode(importContentMap[source] ?? ""),
  );
  const [error, setError] = React.useState<string | null>(null);
  const [activeStep, setActiveStep] = React.useState(0);

  const stepRefs = React.useRef<Array<HTMLDivElement | null>>([]);
  const codePanelRef = React.useRef<HTMLDivElement | null>(null);

  // Fetch from GitHub raw when org and repo are provided.
  React.useEffect(() => {
    if (!isGitHub) return;
    let cancelled = false;
    const path = String(source || "").replace(/^\.?\//, "");
    const url = `https://raw.githubusercontent.com/${org}/${repo}/${branch}/${path}`;
    fetch(url)
      .then((res) => {
        if (!res.ok) throw new Error(`GitHub fetch failed: ${res.status}`);
        return res.text();
      })
      .then((text) => {
        if (!cancelled) setCode(cleanCode(text));
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "Failed to fetch source");
        }
      });
    return () => {
      cancelled = true;
    };
  }, [isGitHub, org, repo, branch, source]);

  // Scroll-spy: mark the step whose card is in the reading zone as active.
  React.useEffect(() => {
    if (steps.length === 0) return;
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            const idx = Number(
              (entry.target as HTMLElement).dataset.stepIndex,
            );
            if (!Number.isNaN(idx)) setActiveStep(idx);
          }
        }
      },
      { rootMargin: "-30% 0px -50% 0px", threshold: 0 },
    );
    const nodes = stepRefs.current.filter(Boolean) as HTMLDivElement[];
    nodes.forEach((node) => observer.observe(node));
    return () => observer.disconnect();
  }, [steps.length, code]);

  const highlighted = React.useMemo(
    () => parseLines(steps[activeStep]?.lines ?? ""),
    [steps, activeStep],
  );

  // Keep the highlighted region visible in the sticky code panel.
  React.useEffect(() => {
    const panel = codePanelRef.current;
    if (!panel || highlighted.size === 0) return;
    const first = Math.min(...highlighted);
    const target = panel.querySelector<HTMLElement>(`[data-line="${first}"]`);
    if (target) {
      target.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }
  }, [highlighted]);

  const prismTheme = usePrismTheme();

  if (error) {
    return <pre className="cw-error">{error}</pre>;
  }
  if (code == null) {
    return <div className="cw-loading">Loading…</div>;
  }

  return (
    <div className="cw-container">
      <div className="cw-steps">
        {steps.map((step, idx) => (
          <div
            key={idx}
            ref={(el) => {
              stepRefs.current[idx] = el;
            }}
            data-step-index={idx}
            className={`cw-step${idx === activeStep ? " cw-step--active" : ""}`}
            onClick={() => setActiveStep(idx)}
          >
            {step.title && <h4 className="cw-step-title">{step.title}</h4>}
            <div className="cw-step-content">{step.content}</div>
          </div>
        ))}
      </div>

      <div className="cw-code-panel">
        <div className="cw-code-sticky" ref={codePanelRef}>
          <Highlight theme={prismTheme} code={code} language={language as never}>
            {({ className, style, tokens, getLineProps, getTokenProps }) => (
              <pre className={`cw-pre ${className}`} style={style}>
                {tokens.map((line, i) => {
                  const lineNumber = i + 1;
                  const isHi = highlighted.has(lineNumber);
                  const lineProps = getLineProps({ line });
                  return (
                    <div
                      key={i}
                      {...lineProps}
                      data-line={lineNumber}
                      className={`cw-line ${lineProps.className ?? ""} ${
                        isHi ? "cw-line--highlighted" : "cw-line--dimmed"
                      }`}
                    >
                      <span className="cw-line-number">{lineNumber}</span>
                      <span className="cw-line-content">
                        {line.map((token, key) => (
                          <span key={key} {...getTokenProps({ token })} />
                        ))}
                      </span>
                    </div>
                  );
                })}
              </pre>
            )}
          </Highlight>
        </div>
      </div>
    </div>
  );
}
