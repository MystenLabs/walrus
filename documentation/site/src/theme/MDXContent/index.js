// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from "react";
import { MDXProvider } from "@mdx-js/react";
import MDXComponents from "@theme/MDXComponents";
import Link from "@docusaurus/Link";
import Term from "../../components/Glossary/Term";
import ImportContent from "@site/src/components/ImportContent";
import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

export default function MDXContent({ children }) {
  const suiComponents = {
    ...MDXComponents,
    Link,
    Term,
    ImportContent,
    Tabs,
    TabItem,
  };
  return <MDXProvider components={suiComponents}>{children}</MDXProvider>;
}
