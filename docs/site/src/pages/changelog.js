// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React, { useState } from "react";
import Layout from "@theme/Layout";
import Head from "@docusaurus/Head";
import styles from "./changelog.module.css";

import ENTRIES from "../data/changelog.json";

const CATEGORIES = [
  { value: "all", label: "ALL" },
  { value: "walrus", label: "WALRUS" },
  { value: "walrus-memory", label: "WALRUS MEMORY" },
  { value: "walrus-sites", label: "WALRUS SITES" },
];

const CATEGORY_LABELS = {
  walrus: "WALRUS",
  "walrus-memory": "WALRUS MEMORY",
  "walrus-sites": "WALRUS SITES",
};

function formatTimelineDate(isoDate) {
  if (!isoDate) return "";
  const d = new Date(isoDate);
  const months = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
  ];
  const year = String(d.getUTCFullYear()).slice(2);
  return `${months[d.getUTCMonth()]}  ${String(d.getUTCDate()).padStart(2, " ")},  '${year}`;
}

function SearchIcon({ className }) {
  return (
    <svg
      className={className}
      viewBox="0 0 20 20"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
    >
      <circle cx="8.5" cy="8.5" r="5.5" />
      <line x1="13" y1="13" x2="18" y2="18" />
    </svg>
  );
}

export default function Changelog() {
  const [active, setActive] = useState("all");
  const [query, setQuery] = useState("");

  const visible = [];
  for (let i = 0; i < ENTRIES.length; i++) {
    const entry = ENTRIES[i];
    if (active !== "all" && entry.category !== active) {
      continue;
    }
    if (query) {
      const q = query.toLowerCase();
      const text = (entry.title + " " + (entry.description || "")).toLowerCase();
      if (!text.includes(q)) {
        continue;
      }
    }
    visible.push(entry);
  }

  return (
    <Layout
      title="Changelog"
      description="New updates and improvements at Walrus"
    >
      <Head>
        <link
          href="https://fonts.googleapis.com/css2?family=Google+Sans+Flex:opsz@6..144&display=swap"
          rel="stylesheet"
        />
      </Head>
      <div className={styles.page}>
        <div className={styles.hero}>
          <h1 className={styles.heroTitle}>Changelog</h1>
          <p className={styles.heroSubtitle}>
            New updates and improvements at Walrus
          </p>
        </div>

        <div className={styles.filterBar}>
          <span className={styles.filterLabel}>Filter by product:</span>
          <div className={styles.filterButtons}>
            {CATEGORIES.map((cat) => (
              <button
                key={cat.value}
                type="button"
                className={
                  active === cat.value
                    ? `${styles.filterBtn} ${styles.filterBtnActive}`
                    : styles.filterBtn
                }
                onClick={() => {
                  setActive(cat.value);
                }}
              >
                {cat.label}
              </button>
            ))}
          </div>
          <div className={styles.searchBox}>
            <input
              type="text"
              placeholder="Search Changelog"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className={styles.searchInput}
            />
            <SearchIcon className={styles.searchIcon} />
          </div>
        </div>

        <div className={styles.timeline}>
          {visible.length === 0 && (
            <p className={styles.empty}>No matching entries found.</p>
          )}
          {visible.map((entry) => (
            <div key={entry.id} className={styles.entry}>
              <div className={styles.entryDate}>
                {formatTimelineDate(entry.date)}
              </div>
              <div className={styles.entryMarker}>
                <div className={styles.diamond} />
              </div>
              <div className={styles.entryContent}>
                <span className={styles.badge}>
                  {CATEGORY_LABELS[entry.category]}
                </span>
                {entry.badge && (
                  <span className={styles.networkBadge}>{entry.badge}</span>
                )}

                <h2 className={styles.entryTitle}>{entry.title}</h2>

                {entry.description && (
                  <p className={styles.entryDesc}>{entry.description}</p>
                )}

                {entry.changes.length > 0 && (
                  <>
                    <h3 className={styles.changesHeading}>Improvements</h3>
                    <ul className={styles.changesList}>
                      {entry.changes.map((change, i) => (
                        <li key={i}>
                          <a
                            href={change.url}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {change.text}
                          </a>
                        </li>
                      ))}
                    </ul>
                  </>
                )}

                <a
                  className={styles.githubLink}
                  href={entry.githubUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  View on GitHub &rarr;
                </a>
              </div>
            </div>
          ))}
        </div>
      </div>
    </Layout>
  );
}
