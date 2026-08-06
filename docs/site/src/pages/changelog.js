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
    <svg className={className} viewBox="0 0 14 14" fill="none">
      <path
        d={
          "M10.6135 9.90643C10.4183 9.71118 10.1017 9.71119 9.90643 9.90646C9.71118 10.1017 9.71119 10.4183 9.90646 " +
          "10.6136L10.26 10.26L10.6135 9.90643ZM11.9286 6.21429H11.4286C11.4286 9.09243 9.09243 11.4286 6.21429 " +
          "11.4286V11.9286V12.4286C9.64471 12.4286 12.4286 9.64471 12.4286 6.21429H11.9286ZM6.21429 " +
          "11.9286V11.4286C3.33614 11.4286 1 9.09243 1 6.21429H0.5H0C0 9.64471 2.78386 12.4286 6.21429 " +
          "12.4286V11.9286ZM0.5 6.21429H1C1 3.33614 3.33614 1 6.21429 1V0.5V0C2.78386 0 0 2.78386 0 6.21429H0.5ZM6.21429 " +
          "0.5V1C9.09243 1 11.4286 3.33614 11.4286 6.21429H11.9286H12.4286C12.4286 2.78386 9.64471 0 6.21429 0V0.5ZM10.26 " +
          "10.26L9.90646 10.6136L13.1465 13.8533L13.5 13.4998L13.8535 13.1462L10.6135 9.90643L10.26 10.26Z"
        }
        fill="currentColor"
      />
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
        <link
          href="https://fonts.googleapis.com/css2?family=Google+Sans+Code:wght@400&display=swap"
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
                <div className={styles.diamond}>
                  <svg viewBox="0 0 29 29" fill="none" aria-hidden="true">
                    <path
                      d="M18.6022 10.3981V18.6018H10.3985V10.3981H18.6022ZM19.6276 9.37268H9.37305V19.6273H19.6276V9.37268Z"
                      fill="currentColor"
                    />
                    <path
                      d="M14.5 1.4459L27.5541 14.5L14.5 27.5541L1.4459 14.5L14.5 1.4459ZM14.5 0L0 14.5L14.5 29L29 14.5L14.5 0Z"
                      fill="currentColor"
                    />
                  </svg>
                </div>
              </div>
              <div className={styles.entryContent}>
                <div className={styles.entryDateMobile}>
                  {formatTimelineDate(entry.date)}
                </div>
                <div className={styles.badgeRow}>
                  <span className={styles.badge}>
                    {CATEGORY_LABELS[entry.category]}
                  </span>
                  {entry.badge && (
                    <span className={styles.networkBadge}>{entry.badge}</span>
                  )}
                </div>

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
