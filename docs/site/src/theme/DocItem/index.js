// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from "react";
import DocItem from "@theme-original/DocItem";
import { useLocation } from "@docusaurus/router";
import StructuredData from "@site/src/components/StructuredData";

export default function DocItemWrapper(props) {
  const doc = props?.content ?? {};
  const metadata = doc.metadata ?? {};
  const toc = doc.toc ?? [];

  const { pathname } = useLocation();

  return (
    <>
      {pathname.startsWith("/docs/operator-guide") && (
        <style>{`
          article:has(> a[href="/docs/operator-guide"]) {
            display: none;
          }
        `}</style>
      )}
      <StructuredData metadata={metadata} toc={toc} />
      <DocItem {...props} />
    </>
  );
}
