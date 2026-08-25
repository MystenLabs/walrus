// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

// Injects per-page JSON-LD structured data into <head> for SEO.
//
// Rendered inside the DocItem/Layout wrapper, which passes doc metadata and TOC
// as props (avoiding direct useDoc() calls from a separate module).
//
// Schemas emitted:
//   - TechArticle on the docs root (/docs/getting-started)
//   - HowTo + LearningResource on getting-started pages that contain steps
//
// BreadcrumbList is already handled by the default Docusaurus DocBreadcrumbs
// theme component, and the sitewide WebSite schema lives in headTags in
// docusaurus.config.js.

import React from "react";
import Head from "@docusaurus/Head";
import { useLocation } from "@docusaurus/router";

const SITE_URL = "https://docs.wal.app";
const ORG_ID = "https://walrus.xyz/#organization";

interface TOCItem {
  value: string;
  id: string;
  level: number;
}

interface DocMetadata {
  title: string;
  description?: string;
}

interface StructuredDataProps {
  metadata: DocMetadata;
  toc: TOCItem[];
}

function buildTechArticle(pathname: string): object {
  return {
    "@context": "https://schema.org",
    "@type": "TechArticle",
    headline: "Walrus Documentation",
    description:
      "Technical documentation for Walrus, a decentralized storage protocol built on Sui.",
    url: `${SITE_URL}${pathname}`,
    publisher: { "@id": ORG_ID },
    mainEntityOfPage: {
      "@type": "WebPage",
      "@id": `${SITE_URL}${pathname}`,
    },
    inLanguage: "en",
  };
}

function buildHowTo(
  pathname: string,
  title: string,
  description: string,
  toc: TOCItem[],
): object | null {
  const stepRegex = /^Step \d+:\s+(.+)$/;
  const steps = toc
    .filter((item) => stepRegex.test(item.value))
    .map((item, index) => {
      const match = item.value.match(stepRegex);
      return {
        "@type": "HowToStep",
        position: index + 1,
        name: item.value,
        text: match ? match[1] : item.value,
        url: `${SITE_URL}${pathname}#${item.id}`,
      };
    });

  if (steps.length === 0) {
    return null;
  }

  return {
    "@context": "https://schema.org",
    "@type": "HowTo",
    name: title,
    description,
    step: steps,
  };
}

function buildLearningResource(
  pathname: string,
  title: string,
  description: string,
): object {
  return {
    "@context": "https://schema.org",
    "@type": "LearningResource",
    name: title,
    description,
    url: `${SITE_URL}${pathname}`,
    educationalLevel: "Beginner",
    learningResourceType: "tutorial",
    provider: { "@id": ORG_ID },
    inLanguage: "en",
  };
}

export default function StructuredData({
  metadata,
  toc,
}: StructuredDataProps): React.JSX.Element | null {
  const { pathname } = useLocation();
  const { title, description } = metadata;

  const normalizedPath = pathname.replace(/\/$/, "");
  const schemas: object[] = [];

  // TechArticle on the docs root page
  if (normalizedPath === "/docs/getting-started") {
    schemas.push(buildTechArticle(normalizedPath));
  }

  // HowTo and LearningResource on getting-started pages that have steps
  if (normalizedPath.startsWith("/docs/getting-started")) {
    const howTo = buildHowTo(
      normalizedPath,
      title,
      description || title,
      toc,
    );
    if (howTo) {
      schemas.push(howTo);
    }
    schemas.push(
      buildLearningResource(normalizedPath, title, description || title),
    );
  }

  if (schemas.length === 0) {
    return null;
  }

  return (
    <Head>
      {schemas.map((schema, i) => (
        <script key={`sd-${i}`} type="application/ld+json">
          {JSON.stringify(schema)}
        </script>
      ))}
    </Head>
  );
}
