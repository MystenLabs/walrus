// Copyright (c) Walrus Foundation
// SPDX-License-Identifier: Apache-2.0

import React from "react";
import { MDXProvider } from "@mdx-js/react";
import MDXComponents from "@theme/MDXComponents";
import Link from "@docusaurus/Link";
import Term from "../../shared/components/Glossary/Term";
import ImportContent from "@site/src/shared/components/ImportContent";
import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import {Card, Cards} from "../../shared/components/Cards";

export default function MDXContent({ children }) {
    const suiComponents = {
        ...MDXComponents,
        Link,
        Term,
        ImportContent,
        Tabs,
        TabItem,
        Cards,
        Card
    };
    return <MDXProvider components={suiComponents}>{children}</MDXProvider>;
}
