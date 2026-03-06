// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
import React from "react";
import Link from "@docusaurus/Link";
import isInternalUrl from "@docusaurus/isInternalUrl";
import Heading from "@theme/Heading";
import styles from "./styles.module.css";

function findFirstLink(item: any): string | undefined {
  if (item.href) return item.href;
  if (item.items) {
    for (const child of item.items) {
      const link = findFirstLink(child);
      if (link) return link;
    }
  }
  return undefined;
}

function CardContainer({
  href,
  title,
  children,
}: {
  href: string;
  title: string;
  children: React.ReactNode;
}) {
  const isExternal = !isInternalUrl(href);

  return (
    <Link href={href} className={styles.card} aria-label={title}>
      <span className={styles.accent} aria-hidden="true" />
      <div className={styles.content}>
        {children}
        <span className={styles.chevron} aria-hidden="true">
          {isExternal ? "↗" : "→"}
        </span>
      </div>
    </Link>
  );
}

function CardLayout({
  href,
  title,
  description,
  footer,
}: {
  href: string;
  title: string;
  description?: string;
  footer?: React.ReactNode;
}) {
  return (
    <CardContainer href={href} title={title}>
      <Heading as="h4" className={styles.title} title={title}>
        {title}
      </Heading>

      {description ? (
        <p className={styles.description} title={description}>
          {description}
        </p>
      ) : null}

      {footer ? <div className={styles.footer}>{footer}</div> : null}
    </CardContainer>
  );
}

function CategoryFooter({ item }: { item: any }) {
  const MAX_ITEMS = 6;
  const items = (item.items ?? []).slice(0, MAX_ITEMS);
  const remaining = (item.items?.length ?? 0) - items.length;

  return (
    <div className={styles.childWrap}>
      <div className={styles.childLabel}>Inside this section</div>
      <ul className={styles.childList}>
        {items.map((child: any, i: number) => (
          <li key={i} className={styles.childItem} title={child.label}>
            <span className={styles.dot} aria-hidden="true" />
            <span className={styles.childText}>{child.label}</span>
          </li>
        ))}
      </ul>

      {remaining > 0 ? (
        <div className={styles.more}>+ {remaining} more</div>
      ) : null}
    </div>
  );
}

function CardCategory({ item }: { item: any }) {
  const href = findFirstLink(item);
  if (!href) return null;

  return (
    <CardLayout
      href={href}
      title={item.label}
      description={item.description}
      footer={<CategoryFooter item={item} />}
    />
  );
}

function CardLink({ item }: { item: any }) {
  return (
    <CardLayout
      href={item.href}
      title={item.label}
      description={item.description}
    />
  );
}

export default function DocCard({ item }: { item: any }) {
  switch (item.type) {
    case "link":
      return <CardLink item={item} />;
    case "category":
      return <CardCategory item={item} />;
    default:
      throw new Error(`unknown item type ${JSON.stringify(item)}`);
  }
}